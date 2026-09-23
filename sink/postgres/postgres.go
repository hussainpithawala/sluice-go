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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds connection parameters for the PostgreSQL sink.
type Config struct {
	ConnString        string
	TableName         string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	StatementTimeout  time.Duration
}

// DefaultConfig returns safe defaults tuned for connection multiplexing.
func DefaultConfig(connString, tableName string) Config {
	return Config{
		ConnString:        connString,
		TableName:         tableName,
		MaxConns:          20,
		MinConns:          5,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		StatementTimeout:  10 * time.Second,
	}
}

// Sink implements sink.FlushSink against PostgreSQL.
type Sink struct {
	pool      *pgxpool.Pool
	tableName string
}

// New connects to PostgreSQL and returns a ready FlushSink.
func New(ctx context.Context, cfg Config) (*Sink, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolConfig.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	// Enforce statement timeout to prevent hung queries from exhausting the pool
	if cfg.StatementTimeout > 0 {
		poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", cfg.StatementTimeout.Milliseconds())
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: initial ping: %w", err)
	}

	return &Sink{
		pool:      pool,
		tableName: cfg.TableName,
	}, nil
}

// BulkWrite executes a multi-row INSERT ... ON CONFLICT DO UPDATE.
//
// WriteModel.Update must be map[string]any representing the full row.
// WriteModel.Filter must be a string representing the conflict target
// (e.g., "id" or "tenant_id, user_id"). Defaults to "id" if empty.
//
// This flattens write rates and eliminates per-row BEGIN/COMMIT overhead.
func (s *Sink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	if len(models) == 0 {
		return &sink.BulkWriteResult{}, nil
	}

	// Extract columns from the first model (assuming uniform schema per batch)
	firstRow, ok := models[0].Update.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("postgres sink requires model.Update to be map[string]any")
	}

	cols := sortedKeys(firstRow)
	conflictTarget := resolveConflictTarget(models[0].Filter)

	// Build: INSERT INTO table (col1, col2) VALUES ($1, $2), ($3, $4)
	//        ON CONFLICT (target) DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(s.tableName)
	query.WriteString(" (")
	query.WriteString(strings.Join(cols, ", "))
	query.WriteString(") VALUES ")

	args := make([]any, 0, len(models)*len(cols))
	placeholderIdx := 1

	for i, model := range models {
		row, ok := model.Update.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("postgres sink requires all model.Update to be map[string]any")
		}

		query.WriteString("(")
		for j, col := range cols {
			query.WriteString(fmt.Sprintf("$%d", placeholderIdx))
			args = append(args, row[col])
			placeholderIdx++
			if j < len(cols)-1 {
				query.WriteString(", ")
			}
		}
		query.WriteString(")")
		if i < len(models)-1 {
			query.WriteString(", ")
		}
	}

	query.WriteString(" ON CONFLICT (")
	query.WriteString(conflictTarget)
	query.WriteString(") DO UPDATE SET ")
	for i, col := range cols {
		query.WriteString(col)
		query.WriteString("=EXCLUDED.")
		query.WriteString(col)
		if i < len(cols)-1 {
			query.WriteString(", ")
		}
	}

	tag, err := s.pool.Exec(ctx, query.String(), args...)
	if err != nil {
		// Check for permanent constraint violations (e.g., unique index, check constraint)
		// These should be routed to DLQ, not retried.
		var pgErr *pgconn.PgError
		if isPermanentError(err, &pgErr) {
			errs := make([]sink.SinkError, len(models))
			for i, m := range models {
				code, err := strconv.Atoi(pgErr.Code)
				if err != nil {
					return nil, fmt.Errorf("unable to convert pg-error code %s", pgErr.Code)
				}
				errs[i] = sink.SinkError{
					CorrelationKey: m.CorrelationKey,
					Code:           code,
					Err:            fmt.Errorf("postgres permanent error %s: %s", pgErr.Code, pgErr.Message),
				}
			}
			return &sink.BulkWriteResult{Errors: errs}, nil
		}
		return nil, fmt.Errorf("postgres: bulk write: %w", err)
	}

	return &sink.BulkWriteResult{
		InsertedCount: tag.RowsAffected(),
		MatchedCount:  tag.RowsAffected(),
		ModifiedCount: tag.RowsAffected(),
		UpsertedCount: tag.RowsAffected(),
	}, nil
}

// Write performs a single-row upsert. Used in degraded mode only.
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
	row, ok := model.Update.(map[string]any)
	if !ok {
		return fmt.Errorf("postgres sink requires model.Update to be map[string]any")
	}

	cols := sortedKeys(row)
	conflictTarget := resolveConflictTarget(model.Filter)

	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(s.tableName)
	query.WriteString(" (")
	query.WriteString(strings.Join(cols, ", "))
	query.WriteString(") VALUES (")

	args := make([]any, 0, len(cols))
	for i, col := range cols {
		query.WriteString(fmt.Sprintf("$%d", i+1))
		args = append(args, row[col])
		if i < len(cols)-1 {
			query.WriteString(", ")
		}
	}
	query.WriteString(") ON CONFLICT (")
	query.WriteString(conflictTarget)
	query.WriteString(") DO UPDATE SET ")
	for i, col := range cols {
		query.WriteString(col)
		query.WriteString("=EXCLUDED.")
		query.WriteString(col)
		if i < len(cols)-1 {
			query.WriteString(", ")
		}
	}

	_, err := s.pool.Exec(ctx, query.String(), args...)
	return err
}

// Ping verifies connectivity to PostgreSQL.
func (s *Sink) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the connection pool.
func (s *Sink) Close(ctx context.Context) error {
	s.pool.Close()
	return nil
}

// Pool returns the underlying pgxpool.Pool, allowing it to be shared
// with source/postgres.Source to maintain a single connection pool.
func (s *Sink) Pool() *pgxpool.Pool {
	return s.pool
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// sortedKeys returns the keys of a map in deterministic sorted order.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resolveConflictTarget extracts the ON CONFLICT target from the Filter field.
// Defaults to "id" if the filter is empty or not a string.
func resolveConflictTarget(filter interface{}) string {
	if ct, ok := filter.(string); ok && ct != "" {
		return ct
	}
	return "id"
}

// isPermanentError checks if a PostgreSQL error is a permanent constraint
// violation that should be routed to the DLQ rather than retried.
// PostgreSQL error classes: 23xxx = Integrity Constraint Violation.
func isPermanentError(err error, pgErr **pgconn.PgError) bool {
	if e, ok := err.(*pgconn.PgError); ok {
		*pgErr = e
		// 23505 = unique_violation
		// 23503 = foreign_key_violation
		// 23514 = check_violation
		// 23502 = not_null_violation
		// 42P01 = undefined_table (schema drift)
		// 42703 = undefined_column (schema drift)

		if e.Code[:2] == "23" {
			return true
		}
		switch e.Code {
		case "42P01", "42703":
			return true
		}
	}
	return false
}

// Ensure pgx import is used (for RowToMap in source).
var _ pgx.Row
