package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/source"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnString = "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable"

func newTestPool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(testConnString)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

//nolint:unused
func setupPostgresSource(t *testing.T) (*Source, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srcPool, err := newTestPool(ctx)
	require.NoError(t, err)

	tableName := "test_sluice_source_" + t.Name()

	_, err = srcPool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true
		)
	`, tableName))
	require.NoError(t, err)

	s := NewSourceWithPool(srcPool)

	cleanup := func() {
		_, _ = srcPool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		srcPool.Close()
	}

	return s, cleanup
}

func TestPostgresSource_ReadBulk_Success(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pool, err := newTestPool(ctx)
	require.NoError(t, err)
	defer pool.Close()

	tableName := "test_source_bulk_" + t.Name()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			name TEXT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)
	defer func() { _, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) }()

	// Seed data
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, user_id, name) VALUES 
		('item_1', 'user_123', 'Alice'),
		('item_2', 'user_123', 'Bob')
	`, tableName))
	require.NoError(t, err)

	s := NewSourceWithPool(pool)

	model := source.BulkReadModel{
		Query: PostgresBulkReadModel{
			Query: fmt.Sprintf("SELECT id, json_build_object('name', name) as payload FROM %s WHERE user_id = $1", tableName),
			Args:  []any{"user_123"},
			Projector: func(rows pgx.Rows) ([]source.BulkReadResult, error) {
				var results []source.BulkReadResult
				for rows.Next() {
					var id string
					var payload []byte
					if err := rows.Scan(&id, &payload); err != nil {
						return nil, err
					}
					results = append(results, source.BulkReadResult{
						CorrelationKey: id,
						Payload:        payload,
					})
				}
				return results, rows.Err()
			},
		},
	}

	results, err := s.ReadBulk(ctx, model)
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Verify one of the results
	found := false
	for _, r := range results {
		if r.CorrelationKey == "item_1" {
			var payload map[string]any
			err := json.Unmarshal(r.Payload, &payload)
			require.NoError(t, err)
			assert.Equal(t, "Alice", payload["name"])
			found = true
		}
	}
	assert.True(t, found)
}

func TestPostgresSource_Read_Success(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pool, err := newTestPool(ctx)
	require.NoError(t, err)
	defer pool.Close()

	tableName := "test_source_read_" + t.Name()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true
		)
	`, tableName))
	require.NoError(t, err)
	defer func() { _, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) }()

	// Seed data
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, name, active) VALUES ($1, $2, $3)", tableName,
	), "user_123", "Alice", true)
	require.NoError(t, err)

	s := NewSourceWithPool(pool)

	// Define the QueryParams with a JSON-agg query and Projector
	params := QueryParams{
		Query: fmt.Sprintf(
			"SELECT json_build_object('id', id, 'name', name, 'active', active) FROM %s WHERE id = $1",
			tableName,
		),
		Args: []any{"user_123"},
		Projector: func(row pgx.Row) ([]byte, error) {
			var payload []byte
			if err := row.Scan(&payload); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, source.ErrRecordNotFound
				}
				return nil, err
			}
			return payload, nil
		},
	}

	model := source.ReadModel{Filter: params}
	data, err := s.Read(ctx, model)

	require.NoError(t, err)
	assert.NotNil(t, data)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result["name"])
	assert.Equal(t, true, result["active"])
}

func TestPostgresSource_Read_NotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pool, err := newTestPool(ctx)
	require.NoError(t, err)
	defer pool.Close()

	tableName := "test_source_notfound_" + t.Name()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)
	defer func() { _, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) }()

	s := NewSourceWithPool(pool)

	params := QueryParams{
		Query: fmt.Sprintf("SELECT json_build_object('id', id, 'name', name) FROM %s WHERE id = $1", tableName),
		Args:  []any{"non_existent"},
		Projector: func(row pgx.Row) ([]byte, error) {
			var payload []byte
			if err := row.Scan(&payload); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, source.ErrRecordNotFound
				}
				return nil, err
			}
			return payload, nil
		},
	}

	model := source.ReadModel{Filter: params}
	data, err := s.Read(ctx, model)

	assert.Nil(t, data)
	assert.ErrorIs(t, err, source.ErrRecordNotFound)
}

func TestPostgresSource_Ping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pool, err := newTestPool(ctx)
	require.NoError(t, err)
	defer pool.Close()

	s := NewSourceWithPool(pool)
	err = s.Ping(ctx)
	require.NoError(t, err)
}
