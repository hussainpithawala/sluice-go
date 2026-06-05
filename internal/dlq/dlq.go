// Package dlq provides dead-letter queue processing with pluggable strategies.
package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Strategy enumerates the dead-letter processing modes.
type Strategy int

const (
	// Ignore logs each dead-letter record and discards it.
	Ignore Strategy = iota
	// Upsert re-runs the WriteContract with Upsert=true and BulkWrites.
	Upsert
	// ReInsert mutates the key and pushes the record back to the dirty queue.
	ReInsert
)

func (s Strategy) String() string {
	switch s {
	case Ignore:
		return "ignore"
	case Upsert:
		return "upsert"
	case ReInsert:
		return "reinsert"
	default:
		return "unknown"
	}
}

// WriteContract is the domain function that transforms a correlation key and
// payload into a WriteModel ready for BulkWrite assembly.
type WriteContract func(correlationKey string, payload []byte) (*sink.WriteModel, error)

// MetricsRecorder is the subset of telemetry methods the DLQ processor needs.
type MetricsRecorder interface {
	RecordDLQProcess(namespace, strategy string, processed, succeeded, failed int)
}

// Result carries the outcome of a ProcessDLQ invocation.
type Result struct {
	Processed int // total records handled across all bands
	Succeeded int // successfully committed/re-queued
	Failed    int // records that failed during DLQ processing
}

// Config holds DLQ processor options.
type Config struct {
	Strategy     Strategy
	MaxBatchSize int
	KeyMutator   func(string) string
	Logger       *slog.Logger
}

// Processor drains and handles dead-letter records.
type Processor struct {
	shield   *shield.Shield
	sink     sink.FlushSink
	contract WriteContract
	metrics  MetricsRecorder
	cfg      Config
}

// NewProcessor constructs a DLQ Processor.
func NewProcessor(
	sh *shield.Shield,
	sk sink.FlushSink,
	contract WriteContract,
	metrics MetricsRecorder,
	cfg Config,
) *Processor {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.KeyMutator == nil {
		cfg.KeyMutator = DefaultKeyMutator
	}
	return &Processor{
		shield:   sh,
		sink:     sk,
		contract: contract,
		metrics:  metrics,
		cfg:      cfg,
	}
}

// Process drains DLQ records across all bands using the configured strategy.
// Iterates each band and processes batches until the DLQ is empty or ctx is cancelled.
func (p *Processor) Process(ctx context.Context) (*Result, error) {
	total := &Result{}

	for band := 0; band < p.shield.BandCount(); band++ {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := p.processBand(ctx, band, total); err != nil {
			return total, err
		}
	}

	p.metrics.RecordDLQProcess(
		p.shield.Namespace(), p.cfg.Strategy.String(),
		total.Processed, total.Succeeded, total.Failed,
	)
	return total, nil
}

func (p *Processor) processBand(ctx context.Context, band int, total *Result) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		records, err := p.shield.DrainDLQ(ctx, band, p.cfg.MaxBatchSize)
		if err != nil {
			return fmt.Errorf("dlq: drain band %d: %w", band, err)
		}
		if len(records) == 0 {
			return nil
		}

		switch p.cfg.Strategy {
		case Ignore:
			err = p.handleIgnore(ctx, band, records, total)
		case Upsert:
			err = p.handleUpsert(ctx, band, records, total)
		case ReInsert:
			err = p.handleReInsert(ctx, band, records, total)
		}
		if err != nil {
			return err
		}
	}
}

func (p *Processor) handleIgnore(ctx context.Context, band int, records []shield.FlushRecord, total *Result) error {
	keys := make([]string, len(records))
	for i, rec := range records {
		keys[i] = rec.CorrelationKey
		p.cfg.Logger.Info("dlq: ignoring record",
			"correlation_key", rec.CorrelationKey,
			"payload_size", len(rec.Payload),
			"band", band,
		)
	}

	if err := p.shield.CommitDLQKeys(ctx, band, keys); err != nil {
		return fmt.Errorf("dlq: commit ignore band %d: %w", band, err)
	}
	total.Processed += len(records)
	total.Succeeded += len(records)
	return nil
}

func (p *Processor) handleUpsert(ctx context.Context, band int, records []shield.FlushRecord, total *Result) error {
	models := make([]sink.WriteModel, 0, len(records))
	corrKeys := make([]string, 0, len(records))

	for _, rec := range records {
		wm, err := p.contract(rec.CorrelationKey, rec.Payload)
		if err != nil {
			p.cfg.Logger.Warn("dlq: contract error during upsert, skipping",
				"correlation_key", rec.CorrelationKey,
				"error", err,
			)
			total.Processed++
			total.Failed++
			continue
		}
		wm.Upsert = true
		models = append(models, sink.WriteModel{
			CorrelationKey: rec.CorrelationKey,
			Filter:         wm.Filter,
			Update:         wm.Update,
			Upsert:         true,
		})
		corrKeys = append(corrKeys, rec.CorrelationKey)
	}

	if len(models) == 0 {
		return nil
	}

	result, err := p.sink.BulkWrite(ctx, models)
	if err != nil {
		// Total failure — leave keys in DLQ for retry on next invocation.
		return fmt.Errorf("dlq: bulk write band %d: %w", band, err)
	}

	// Partition results: identify failed keys.
	failedKeys := make(map[string]bool)
	if result != nil {
		for _, se := range result.Errors {
			failedKeys[se.CorrelationKey] = true
			p.cfg.Logger.Warn("dlq: upsert failed for key",
				"correlation_key", se.CorrelationKey,
				"code", se.Code,
				"error", se.Err,
			)
		}
	}

	successKeys := make([]string, 0, len(corrKeys))
	for _, k := range corrKeys {
		if !failedKeys[k] {
			successKeys = append(successKeys, k)
		}
	}

	if len(successKeys) > 0 {
		if commitErr := p.shield.CommitDLQKeys(ctx, band, successKeys); commitErr != nil {
			return fmt.Errorf("dlq: commit upsert band %d: %w", band, commitErr)
		}
	}

	total.Processed += len(corrKeys)
	total.Succeeded += len(successKeys)
	total.Failed += len(failedKeys)
	return nil
}

func (p *Processor) handleReInsert(ctx context.Context, band int, records []shield.FlushRecord, total *Result) error {
	committable := make([]string, 0, len(records))

	for _, rec := range records {
		newKey := p.cfg.KeyMutator(rec.CorrelationKey)
		if err := p.shield.Write(ctx, newKey, rec.Payload); err != nil {
			p.cfg.Logger.Warn("dlq: reinsert write failed",
				"original_key", rec.CorrelationKey,
				"new_key", newKey,
				"error", err,
			)
			total.Processed++
			total.Failed++
			continue
		}
		committable = append(committable, rec.CorrelationKey)
	}

	if len(committable) > 0 {
		if err := p.shield.CommitDLQKeys(ctx, band, committable); err != nil {
			return fmt.Errorf("dlq: commit reinsert band %d: %w", band, err)
		}
	}

	total.Processed += len(committable)
	total.Succeeded += len(committable)
	return nil
}

// DefaultKeyMutator appends a timestamp suffix to avoid key collision.
func DefaultKeyMutator(oldKey string) string {
	return fmt.Sprintf("%s_dlq_%d", oldKey, time.Now().UnixNano())
}
