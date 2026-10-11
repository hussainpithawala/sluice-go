package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresSink_BulkWrite_Many_ExecutesRawSQL verifies that Many:true models
// execute the raw SQL UPDATE statement via pool.Exec.
// RFP #23 §3.6 (expanded scope): Many models carry raw SQL + args.
func TestPostgresSink_BulkWrite_Many_ExecutesRawSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Table has 'id SERIAL PRIMARY KEY'
	s := testPostgresSink(t, "test_many", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	// Create test table
	_, err := s.pool.Exec(ctx, `
		DROP TABLE IF EXISTS test_many;
		CREATE TABLE test_many (
			id SERIAL PRIMARY KEY,
			user_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending'
		);
		INSERT INTO test_many (user_id, status) VALUES
			('u1', 'pending'), ('u1', 'pending'), ('u1', 'pending'),
			('u2', 'pending');
	`)
	require.NoError(t, err)

	models := []sink.WriteModel{
		{
			CorrelationKey: "user-u1",
			Filter:         []any{"u1"},
			Update:         "UPDATE test_many SET status = 'completed' WHERE user_id = $1",
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Errors)

	// Verify all 3 rows for u1 were updated
	var count int
	err = s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM test_many WHERE user_id = 'u1' AND status = 'completed'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify u2 was NOT updated
	err = s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM test_many WHERE user_id = 'u2' AND status = 'pending'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestPostgresSink_BulkWrite_Many_InvalidUpdateType verifies that a Many model
// with a non-string Update field returns a permanent SinkError.
func TestPostgresSink_BulkWrite_Many_InvalidUpdateType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := testPostgresSink(t, "test_invalid_update", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	models := []sink.WriteModel{
		{
			CorrelationKey: "bad-update-type",
			Filter:         []any{"u1"},
			Update:         map[string]any{"col": "val"}, // Wrong: should be string for Many
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Errors, 1)

	assert.Equal(t, "bad-update-type", result.Errors[0].CorrelationKey)
	assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
	assert.Contains(t, result.Errors[0].Err.Error(), "raw SQL string")
}

// TestPostgresSink_BulkWrite_Many_InvalidFilterType verifies that a Many model
// with a non-[]any Filter field returns a permanent SinkError.
func TestPostgresSink_BulkWrite_Many_InvalidFilterType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Table has 'id INT' (no explicit PK, but we pass 'id' as conflict col for Sink init)
	s := testPostgresSink(t, "test_write_many", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	models := []sink.WriteModel{
		{
			CorrelationKey: "bad-filter-type",
			Filter:         "not-a-slice", // Wrong: should be []any for Many
			Update:         "UPDATE test_many SET status = 'x' WHERE user_id = $1",
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Errors, 1)

	assert.Equal(t, "bad-filter-type", result.Errors[0].CorrelationKey)
	assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
	assert.Contains(t, result.Errors[0].Err.Error(), "[]any slice")
}

// TestPostgresSink_BulkWrite_MixedManyAndUpsert verifies that a batch containing
// both Many:true (raw SQL) and Many:false (upsert) models processes both correctly.
func TestPostgresSink_BulkWrite_MixedManyAndUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Table has 'user_id TEXT PRIMARY KEY'
	s := testPostgresSink(t, "test_mixed", []string{"user_id"})
	defer func() { _ = s.Close(ctx) }()

	// Create test table
	_, err := s.pool.Exec(ctx, `
		DROP TABLE IF EXISTS test_mixed;
		CREATE TABLE test_mixed (
			user_id TEXT PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'pending',
			score INT NOT NULL DEFAULT 0
		);
		INSERT INTO test_mixed (user_id, status, score) VALUES
			('u1', 'pending', 10), ('u1_dup', 'pending', 20), ('u2', 'pending', 30);
	`)
	require.NoError(t, err)

	models := []sink.WriteModel{
		// Many model: raw SQL update
		{
			CorrelationKey: "many-key",
			Filter:         []any{"pending"},
			Update:         "UPDATE test_mixed SET status = 'active' WHERE status = $1",
			Many:           true,
		},
		// Upsert model: map-based single row
		{
			CorrelationKey: "upsert-key",
			Filter:         nil,
			Update: map[string]any{
				"user_id": "u3",
				"status":  "new",
				"score":   99,
			},
			Upsert: true,
			Many:   false,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Errors)

	// Verify Many model updated all 'pending' rows
	var count int
	err = s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM test_mixed WHERE status = 'active'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count) // all 3 were 'pending'

	// Verify upsert model inserted the new row
	err = s.pool.QueryRow(ctx,
		"SELECT score FROM test_mixed WHERE user_id = 'u3'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 99, count)
}

// TestPostgresSink_BulkWrite_Many_SQLStateError verifies that a SQL error in a Many
// model is correctly classified (permanent vs transient).
func TestPostgresSink_BulkWrite_Many_SQLStateError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := testPostgresSink(t, "test_sqlstate", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	// Reference a non-existent table → 42P01 (undefined_table) → permanent
	models := []sink.WriteModel{
		{
			CorrelationKey: "bad-table",
			Filter:         []any{},
			Update:         "UPDATE nonexistent_table SET x = 1",
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Errors, 1)

	assert.Equal(t, "bad-table", result.Errors[0].CorrelationKey)
	assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
}

// TestPostgresSink_Write_Many_ExecutesRawSQL verifies the degraded-mode Write method
// handles Many:true correctly.
func TestPostgresSink_Write_Many_ExecutesRawSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Table has 'id INT' (no explicit PK, but we pass 'id' as conflict col for Sink init)
	s := testPostgresSink(t, "test_write_many", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	_, err := s.pool.Exec(ctx, `
		DROP TABLE IF EXISTS test_write_many;
		CREATE TABLE test_write_many (id INT, val TEXT);
		INSERT INTO test_write_many VALUES (1, 'old'), (2, 'old'), (3, 'old');
	`)
	require.NoError(t, err)

	model := sink.WriteModel{
		CorrelationKey: "write-many",
		Filter:         []any{"new"},
		Update:         "UPDATE test_write_many SET val = $1",
		Many:           true,
	}

	err = s.Write(ctx, model)
	require.NoError(t, err)

	var count int
	err = s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM test_write_many WHERE val = 'new'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

// TestPostgresSink_Write_Many_InvalidUpdateType verifies degraded-mode Write
// rejects non-string Update for Many:true.
func TestPostgresSink_Write_Many_InvalidUpdateType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Table has 'id INT' (no explicit PK, but we pass 'id' as conflict col for Sink init)
	s := testPostgresSink(t, "test_write_many", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	model := sink.WriteModel{
		CorrelationKey: "bad-write",
		Filter:         nil,
		Update:         12345, // Wrong type
		Many:           true,
	}

	err := s.Write(ctx, model)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "raw SQL string")
}

// TestPostgresSink_BulkWrite_Many_NilFilter verifies that a Many model with nil
// Filter (no arguments) executes correctly.
func TestPostgresSink_BulkWrite_Many_NilFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := testPostgresSink(t, "test_nil_filter", []string{"id"})
	defer func() { _ = s.Close(ctx) }()

	_, err := s.pool.Exec(ctx, `
		DROP TABLE IF EXISTS test_nil_filter;
		CREATE TABLE test_nil_filter (id INT, done BOOLEAN DEFAULT FALSE);
		INSERT INTO test_nil_filter (id) VALUES (1), (2), (3);
	`)
	require.NoError(t, err)

	models := []sink.WriteModel{
		{
			CorrelationKey: "nil-filter",
			Filter:         nil, // No arguments
			Update:         "UPDATE test_nil_filter SET done = TRUE",
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	assert.Empty(t, result.Errors)

	var count int
	err = s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM test_nil_filter WHERE done = TRUE",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}
