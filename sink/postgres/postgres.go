// Package postgres provides a FlushSink implementation for PostgreSQL.
//
// It uses multi-row INSERT ... ON CONFLICT DO UPDATE to flatten write rates
// and eliminate per-row transaction overhead. Connection pooling is handled
// by pgxpool with strict bounds to prevent connection saturation.
//
// Schema Boundary: sluice will NEVER auto-create tables, emit DDL, or manage
// migrations. The operator must provision the target table and indexes using
// standard migration tooling (e.g., goose, golang-migrate) before sluice
// begins flushing. If the schema drifts, sluice fails fast with a permanent
// error and routes the batch to the DLQ.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OnConflictAction defines the behavior when a conflict occurs.
type OnConflictAction int

const (
	OnConflictDoUpdate OnConflictAction = iota
	OnConflictDoNothing
)

// Config holds connection and table parameters for PostgreSQL.
type Config struct {
	ConnString      string
	TableName       string
	ConflictColumns []string         // e.g., []string{"id"} or []string{"user_id", "event_id"}
	OnConflict      OnConflictAction // Default: OnConflictDoUpdate
	UpdateColumns   []string         // Columns to update on conflict (if DoUpdate). If empty, updates all non-conflict columns.
}

// Sink implements sink.FlushSink against PostgreSQL.
type Sink struct {
	pool            *pgxpool.Pool
	tableName       string
	conflictColumns []string
	onConflict      OnConflictAction
	updateColumns   []string
}

// New connects to PostgreSQL and returns a ready FlushSink.
func New(ctx context.Context, cfg Config) (*Sink, error) {
	if cfg.TableName == "" {
		return nil, errors.New("sluice/postgres: table name is required")
	}
	if len(cfg.ConflictColumns) == 0 {
		return nil, errors.New("sluice/postgres: conflict columns are required")
	}

	pool, err := pgxpool.New(ctx, cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("sluice/postgres: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("sluice/postgres: ping: %w", err)
	}

	return &Sink{
		pool:            pool,
		tableName:       cfg.TableName,
		conflictColumns: cfg.ConflictColumns,
		onConflict:      cfg.OnConflict,
		updateColumns:   cfg.UpdateColumns,
	}, nil
}

// BulkWrite executes all models as a single INSERT ... ON CONFLICT statement.
// It handles partial success, column mismatches, and duplicate conflict keys.
func (s *Sink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	if len(models) == 0 {
		return &sink.BulkWriteResult{}, nil
	}

	var preErrors []sink.SinkError
	validModels := make([]sink.WriteModel, 0, len(models))
	var expectedCols []string
	seenConflictKeys := make(map[string]bool)

	for _, m := range models {
		// RFP Fix: Catch type assertion failures per-item.
		item, ok := m.Update.(map[string]any)
		if !ok {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            errors.New("update field must be map[string]any"),
			})
			continue
		}

		cols := sortedColumnsFromMap(item)
		if expectedCols == nil {
			expectedCols = cols
		} else if !sameColumns(expectedCols, cols) {
			// RFP Fix: Column mismatch gets a permanent per-row error instead of NULL-overwriting fields.
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            errors.New("column mismatch in batch"),
			})
			continue
		}

		conflictKey := buildConflictKeyFromMap(item, s.conflictColumns)
		if seenConflictKeys[conflictKey] {
			// RFP Fix: Duplicate conflict keys become permanent per-row errors (avoids 21000).
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            errors.New("duplicate conflict key in batch"),
			})
			continue
		}
		seenConflictKeys[conflictKey] = true
		validModels = append(validModels, m)
	}

	if len(validModels) == 0 {
		return &sink.BulkWriteResult{Errors: preErrors}, nil
	}

	// Execute bulk insert
	err := s.executeBulk(ctx, validModels, expectedCols)
	if err == nil {
		return &sink.BulkWriteResult{Errors: preErrors}, nil
	}

	// Handle errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// RFP Fix: SQLSTATE classes 23xxx, 42P01, 42703 and 21000 are ClassPermanent.
		if isPermanentSQLState(pgErr.Code) {
			// RFP Fix: A permanent multi-row error is retried row by row to isolate the bad rows.
			return s.retryRowByRow(ctx, validModels, expectedCols, preErrors)
		}
		// Transient error for all valid models
		for _, m := range validModels {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassTransient,
				Err:            err,
			})
		}
		return &sink.BulkWriteResult{Errors: preErrors}, nil
	}

	// Unmapped error
	return nil, fmt.Errorf("sluice/postgres: unmapped bulkwrite error: %w", err)
}

// executeBulk builds and executes a single INSERT ... ON CONFLICT statement.
func (s *Sink) executeBulk(ctx context.Context, models []sink.WriteModel, cols []string) error {
	if len(models) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.tableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(models)*len(cols))
	for i, m := range models {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		item := m.Update.(map[string]any)
		for j, col := range cols {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("$%d", len(args)+1))
			args = append(args, item[col])
		}
		sb.WriteString(")")
	}

	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(strings.Join(s.conflictColumns, ", "))
	sb.WriteString(") ")

	if s.onConflict == OnConflictDoNothing {
		sb.WriteString("DO NOTHING")
	} else {
		sb.WriteString("DO UPDATE SET ")
		updateCols := s.updateColumns
		if len(updateCols) == 0 {
			// Default to all non-conflict columns
			updateCols = make([]string, 0, len(cols))
			for _, c := range cols {
				if !contains(s.conflictColumns, c) {
					updateCols = append(updateCols, c)
				}
			}
		}
		for i, c := range updateCols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(c)
			sb.WriteString(" = EXCLUDED.")
			sb.WriteString(c)
		}
	}

	_, err := s.pool.Exec(ctx, sb.String(), args...)
	return err
}

// retryRowByRow executes each model individually to isolate permanent failures.
func (s *Sink) retryRowByRow(ctx context.Context, models []sink.WriteModel, cols []string, existingErrors []sink.SinkError) (*sink.BulkWriteResult, error) {
	var errs []sink.SinkError
	errs = append(errs, existingErrors...)

	for _, m := range models {
		err := s.executeBulk(ctx, []sink.WriteModel{m}, cols)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				if isPermanentSQLState(pgErr.Code) {
					errs = append(errs, sink.SinkError{
						CorrelationKey: m.CorrelationKey,
						Class:          sink.ClassPermanent,
						Err:            err,
					})
				} else {
					errs = append(errs, sink.SinkError{
						CorrelationKey: m.CorrelationKey,
						Class:          sink.ClassTransient,
						Err:            err,
					})
				}
			} else {
				errs = append(errs, sink.SinkError{
					CorrelationKey: m.CorrelationKey,
					Class:          sink.ClassTransient,
					Err:            err,
				})
			}
		}
	}

	return &sink.BulkWriteResult{Errors: errs}, nil
}

// Write performs a single-document upsert. Used in degraded mode only.
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
	item, ok := model.Update.(map[string]any)
	if !ok {
		return errors.New("update field must be map[string]any")
	}
	cols := sortedColumnsFromMap(item)
	return s.executeBulk(ctx, []sink.WriteModel{model}, cols)
}

func (s *Sink) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Sink) Close(ctx context.Context) error {
	s.pool.Close()
	return nil
}

// --- Helper Functions ---

func sortedColumnsFromMap(item map[string]any) []string {
	cols := make([]string, 0, len(item))
	for k := range item {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func buildConflictKeyFromMap(item map[string]any, conflictCols []string) string {
	parts := make([]string, 0, len(conflictCols))
	for _, c := range conflictCols {
		parts = append(parts, fmt.Sprintf("%v", item[c]))
	}
	return strings.Join(parts, "|")
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// isPermanentSQLState checks if the PostgreSQL SQLSTATE code indicates a permanent failure.
// RFP Fix: Codes are kept as strings, not Atoi'd.
func isPermanentSQLState(code string) bool {
	// 23xxx: Integrity Constraint Violation (e.g., 23505 unique_violation)
	if strings.HasPrefix(code, "23") {
		return true
	}
	// 42P01: Undefined Table
	// 42703: Undefined Column
	if code == "42P01" || code == "42703" {
		return true
	}
	// 21000: Cardinality Violation
	if code == "21000" {
		return true
	}
	return false
}
