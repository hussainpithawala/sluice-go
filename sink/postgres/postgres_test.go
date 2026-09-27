package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnString = "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable"

func setupPostgresSink(t *testing.T) (*Sink, *pgxpool.Pool, string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tableName := "test_sluice_" + t.Name()

	cfg := Config{
		ConnString:      testConnString,
		TableName:       tableName,
		ConflictColumns: []string{"id"},
		OnConflict:      OnConflictDoUpdate,
	}

	s, err := New(ctx, cfg)
	require.NoError(t, err, "Failed to connect to PostgreSQL")

	// Create a separate pool for test setup/verification since sink.pool is unexported
	testPool, err := pgxpool.New(ctx, testConnString)
	require.NoError(t, err)

	// Create test table
	_, err = testPool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			channel TEXT NOT NULL,
			priority INTEGER NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, tableName))
	require.NoError(t, err)

	cleanup := func() {
		_, _ = testPool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		testPool.Close()
		_ = s.Close(context.Background())
	}
	return s, testPool, tableName, cleanup
}

func TestPostgresSink_BulkWrite_Success(t *testing.T) {
	t.Parallel()
	s, testPool, tableName, cleanup := setupPostgresSink(t)
	defer cleanup()

	models := []sink.WriteModel{
		{
			CorrelationKey: "key1",
			Update: map[string]any{
				"id":       "key1",
				"channel":  "push",
				"priority": 5,
			},
			Upsert: true,
		},
		{
			CorrelationKey: "key2",
			Update: map[string]any{
				"id":       "key2",
				"channel":  "email",
				"priority": 3,
			},
			Upsert: true,
		},
	}

	res, err := s.BulkWrite(context.Background(), models)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res.Errors)

	// Verify rows were inserted
	var count int
	err = testPool.QueryRow(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestPostgresSink_BulkWrite_Upsert(t *testing.T) {
	t.Parallel()
	s, testPool, tableName, cleanup := setupPostgresSink(t)
	defer cleanup()

	// First write
	model := sink.WriteModel{
		CorrelationKey: "key1",
		Update: map[string]any{
			"id":       "key1",
			"channel":  "push",
			"priority": 5,
		},
		Upsert: true,
	}

	_, err := s.BulkWrite(context.Background(), []sink.WriteModel{model})
	require.NoError(t, err)

	// Second write (upsert — same key, different values)
	model.Update = map[string]any{
		"id":       "key1",
		"channel":  "email",
		"priority": 1,
	}

	res, err := s.BulkWrite(context.Background(), []sink.WriteModel{model})
	require.NoError(t, err)
	assert.Empty(t, res.Errors)

	// Verify the updated value
	var channel string
	var priority int
	err = testPool.QueryRow(context.Background(),
		fmt.Sprintf("SELECT channel, priority FROM %s WHERE id = $1", tableName), "key1",
	).Scan(&channel, &priority)
	require.NoError(t, err)
	assert.Equal(t, "email", channel)
	assert.Equal(t, 1, priority)
}

func TestPostgresSink_Write_DegradedMode(t *testing.T) {
	t.Parallel()
	s, testPool, tableName, cleanup := setupPostgresSink(t)
	defer cleanup()

	model := sink.WriteModel{
		CorrelationKey: "key1",
		Update: map[string]any{
			"id":       "degraded_key",
			"channel":  "sms",
			"priority": 2,
		},
		Upsert: true,
	}

	err := s.Write(context.Background(), model)
	require.NoError(t, err)

	// Verify
	var channel string
	err = testPool.QueryRow(context.Background(),
		fmt.Sprintf("SELECT channel FROM %s WHERE id = $1", tableName), "degraded_key",
	).Scan(&channel)
	require.NoError(t, err)
	assert.Equal(t, "sms", channel)
}

func TestPostgresSink_Ping(t *testing.T) {
	t.Parallel()
	s, _, _, cleanup := setupPostgresSink(t)
	defer cleanup()

	err := s.Ping(context.Background())
	require.NoError(t, err)
}
