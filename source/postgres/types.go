package postgres

import (
	"github.com/hussainpithawala/sluice-go/source"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresBulkReadModel is the PostgreSQL-specific execution plan for bulk reads.
// It allows operators to write complex SQL (including JOINs and aggregations)
// and shape the resulting rows into canonical JSON payloads.
type PostgresBulkReadModel struct {
	// Query is the SQL statement to execute.
	// Best Practice: Use Postgres-native JSON aggregation (e.g., json_build_object)
	// to return fully-formed JSON payloads directly from the database.
	Query string

	// Args are the parameters to bind to the query (e.g., []any{userID}).
	Args []any

	// Projector iterates over the returned pgx.Rows and shapes them into BulkReadResults.
	Projector func(rows pgx.Rows) ([]source.BulkReadResult, error)
}

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
