// Package engine runs the flush loop that drains dirty Redis keys
// into the sink via BulkWrite. One goroutine per band, dual-trigger.
package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Engine runs one goroutine per band with a time trigger and a volume trigger.
type Engine struct {
	cfg           sluice.Config
	shield        *shield.Shield
	sink          sink.FlushSink
	contract      sluice.WriteContract
	metrics       sluice.MetricsRecorder
	callback      sluice.OnFlushCallback
	volumeSignals []chan struct{}
	wg            sync.WaitGroup
	stopCh        chan struct{}
	once          sync.Once
}

// New constructs an Engine. Call Start() to begin band goroutines.
func New(cfg sluice.Config, sh *shield.Shield, sk sink.FlushSink,
	contract sluice.WriteContract, metrics sluice.MetricsRecorder, cb sluice.OnFlushCallback) *Engine {
	signals := make([]chan struct{}, cfg.BandCount)
	for i := range signals {
		signals[i] = make(chan struct{}, 1)
	}
	return &Engine{cfg: cfg, shield: sh, sink: sk, contract: contract,
		metrics: metrics, callback: cb, volumeSignals: signals, stopCh: make(chan struct{})}
}

// Start launches one goroutine per band.
func (e *Engine) Start() {
	for band := 0; band < e.cfg.BandCount; band++ {
		e.wg.Add(1)
		go e.runBand(band)
	}
}

// SignalVolume sends a non-blocking volume trigger to a band.
func (e *Engine) SignalVolume(band int) {
	select {
	case e.volumeSignals[band] <- struct{}{}:
	default:
	}
}

// DrainAndStop signals all bands to flush and exit, then blocks until done.
func (e *Engine) DrainAndStop() {
	e.once.Do(func() { close(e.stopCh); e.wg.Wait() })
}

func (e *Engine) runBand(band int) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.FlushWindow)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = e.flushBand(ctx, band)
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.FlushWindow*4)
			_ = e.flushBand(ctx, band)
			cancel()
		case <-e.volumeSignals[band]:
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.FlushWindow*4)
			_ = e.flushBand(ctx, band)
			cancel()
		}
	}
}

func (e *Engine) flushBand(ctx context.Context, band int) error {
	start   := time.Now()
	bandStr := fmt.Sprintf("%d", band)

	if depth, err := e.shield.DirtyQueueDepth(ctx, band); err == nil {
		e.metrics.RecordDirtyQueueDepth(e.cfg.Namespace, bandStr, int(depth))
	}
	records, err := e.shield.DrainBand(ctx, band, e.cfg.MaxBatchSize)
	if err != nil {
		e.metrics.RecordFlush(e.cfg.Namespace, bandStr, 0, time.Since(start), err)
		return err
	}
	if len(records) == 0 {
		return nil
	}
	models   := make([]sink.WriteModel, 0, len(records))
	corrKeys := make([]string, 0, len(records))
	for _, rec := range records {
		wm, contractErr := e.contract(rec.CorrelationKey, rec.Payload)
		if contractErr != nil {
			e.metrics.RecordContractError(e.cfg.Namespace, rec.CorrelationKey, contractErr)
			if e.callback != nil {
				e.callback(
					[]string{rec.CorrelationKey},
					&sluice.BulkWriteResult{Errors: []sluice.SinkError{{CorrelationKey: rec.CorrelationKey, Err: contractErr}}},
					fmt.Errorf("%w: %v", sluice.ErrContractViolation, contractErr),
				)
			}
			continue
		}
		models   = append(models, sink.WriteModel{CorrelationKey: rec.CorrelationKey, Filter: wm.Filter, Update: wm.Update, Upsert: wm.Upsert})
		corrKeys = append(corrKeys, rec.CorrelationKey)
	}
	if len(models) == 0 {
		return nil
	}
	result, flushErr := e.sink.BulkWrite(ctx, models)
	duration := time.Since(start)
	e.metrics.RecordFlush(e.cfg.Namespace, bandStr, len(models), duration, flushErr)
	if e.callback != nil {
		e.callback(corrKeys, result, flushErr)
	}
	return flushErr
}
