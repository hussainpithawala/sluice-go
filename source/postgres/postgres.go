// Package postgres provides a Source implementation for PostgreSQL.
//
// It uses the QueryParams pattern to allow operators to define arbitrary
// SQL queries (including JOINs, aggregations, and subqueries) with a
// custom Projector function that shapes the result into the canonical
// []byte payload for the Redis journal.
//
// Schema Boundary: sluice will NEVER auto-create tables or manage migrations.
// If the query references a missing column or table, sluice fails fast and
// routes the read failure appropriately.
package postgres

import (
	"context"
	"fmt"

	"github.com/hussainpithawala/sluice-go/source"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New connects to PostgreSQL and returns a ready Source.
func New(ctx context.Context, connString string) (*Source, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("postgres source: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres source: initial ping: %w", err)
	}

	return &Source{pool: pool}, nil
}

// NewSourceWithPool creates a Source using an existing pgxpool.Pool.
// Use this to share a connection pool with sink/postgres.Sink.
func NewSourceWithPool(pool *pgxpool.Pool) *Source {
	return &Source{pool: pool}
}

// Read executes the SQL query defined in QueryParams and delegates
// the result shaping entirely to the operator's Projector function.
//
// This design keeps sluice agnostic to SQL schema while empowering
// the operator to define arbitrarily complex read patterns (JOINs,
// aggregations, window functions) without modifying the library.
func (s *Source) Read(ctx context.Context, model source.ReadModel) ([]byte, error) {
	params, ok := model.Filter.(QueryParams)
	if !ok {
		return nil, fmt.Errorf("postgres source requires Filter to be postgres.QueryParams")
	}

	if params.Query == "" {
		return nil, fmt.Errorf("postgres source requires a non-empty Query")
	}

	if params.Projector == nil {
		return nil, fmt.Errorf("postgres source requires a non-nil Projector function")
	}

	row := s.pool.QueryRow(ctx, params.Query, params.Args...)
	return params.Projector(row)
}

// ReadBulk executes the SQL query defined in PostgresBulkReadModel and delegates
// the result shaping entirely to the operator's Projector function.
func (s *Source) ReadBulk(ctx context.Context, model source.BulkReadModel) ([]source.BulkReadResult, error) {
	pgModel, ok := model.Query.(PostgresBulkReadModel)
	if !ok {
		return nil, fmt.Errorf("postgres source requires Query to be postgres.PostgresBulkReadModel")
	}

	if pgModel.Query == "" {
		return nil, fmt.Errorf("postgres source requires a non-empty SQL Query")
	}

	if pgModel.Projector == nil {
		return nil, fmt.Errorf("postgres source requires a non-nil Projector function")
	}

	// Execute the query using the strictly bounded pgxpool
	rows, err := s.pool.Query(ctx, pgModel.Query, pgModel.Args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: bulk read query execution failed: %w", err)
	}
	// Note: The Projector is responsible for calling rows.Close() if it returns early,
	// but we defer it here as a safety net to prevent connection leaks.
	defer rows.Close()

	return pgModel.Projector(rows)
}

// Ping verifies connectivity to PostgreSQL.
func (s *Source) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the connection pool.
func (s *Source) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}
