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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryParams defines the execution plan for a cold read in PostgreSQL.
// It is designed to be passed via source.ReadModel.Filter.
//
// Best Practice: Use Postgres-native JSON aggregation functions
// (e.g., json_build_object, json_agg) to return a single, fully-formed
// JSON payload per correlation key directly from the database.
type QueryParams struct {
	// Query is the SQL statement to execute.
	// Example: "SELECT json_build_object('id', u.id, 'name', u.name) FROM users u WHERE u.id = $1"
	Query string

	// Args are the parameters to bind to the query (e.g., []any{correlationKey}).
	Args []any

	// Projector transforms the resulting pgx.Row into the canonical []byte
	// payload that will be stored in the Redis L2 journal and L1 cache.
	// If the query returns no rows, the projector should return source.ErrRecordNotFound.
	Projector func(row pgx.Row) ([]byte, error)
}

// Source implements source.Source for PostgreSQL.
type Source struct {
	pool *pgxpool.Pool
}

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

// Ping verifies connectivity to PostgreSQL.
func (s *Source) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the connection pool.
func (s *Source) Close(ctx context.Context) error {
	s.pool.Close()
	return nil
}
