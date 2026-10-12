// tests/integration/postgres/postgres_sink_update_many_test.go
package postgres_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	sinkpostgres "github.com/hussainpithawala/sluice-go/sink/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresBulkWrite_UpdateMany(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up the OFFICIAL PostgreSQL container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
	}
	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer func(pgC testcontainers.Container, ctx context.Context, opts ...testcontainers.TerminateOption) {
		err := pgC.Terminate(ctx, opts...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while terminating the PgContainer %e", err))
		}
	}(pgC, ctx)

	host, _ := pgC.Host(ctx)
	port, _ := pgC.MappedPort(ctx, "5432")
	connString := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())

	// 2. Create a SEPARATE pool for seeding + verification
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()

	// 3. Create TWO segregated tables:
	//    - test_many: NO unique constraint on user_id (Many:true needs multiple rows per user)
	//    - test_upsert: user_id IS the primary key (Many:false upsert needs ON CONFLICT)
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS test_many;
		CREATE TABLE test_many (
			id        SERIAL PRIMARY KEY,
			user_id   TEXT NOT NULL,
			campaign  TEXT NOT NULL DEFAULT '',
			fulfilled BOOLEAN NOT NULL DEFAULT FALSE,
			status    TEXT NOT NULL DEFAULT 'pending'
		);

		DROP TABLE IF EXISTS test_upsert;
		CREATE TABLE test_upsert (
			user_id   TEXT PRIMARY KEY,
			campaign  TEXT NOT NULL DEFAULT '',
			fulfilled BOOLEAN NOT NULL DEFAULT FALSE,
			status    TEXT NOT NULL DEFAULT 'pending'
		);
	`)
	require.NoError(t, err)

	// 4. Initialize TWO sinks — one per table
	// Sink for Many:true tests (raw SQL path, conflict columns irrelevant)
	sMany, err := sinkpostgres.New(ctx, sinkpostgres.Config{
		ConnString:      connString,
		TableName:       "test_many",
		ConflictColumns: []string{"id"},
		OnConflict:      sinkpostgres.OnConflictDoUpdate,
	})
	require.NoError(t, err)
	defer func(s *sinkpostgres.Sink, ctx context.Context) {
		err := s.Close(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("unable to close the many-sink due to error %e", err))
		}
	}(sMany, ctx)

	// Sink for Many:false upsert tests (ON CONFLICT path)
	sUpsert, err := sinkpostgres.New(ctx, sinkpostgres.Config{
		ConnString:      connString,
		TableName:       "test_upsert",
		ConflictColumns: []string{"user_id"},
		OnConflict:      sinkpostgres.OnConflictDoUpdate,
	})
	require.NoError(t, err)
	defer func(s *sinkpostgres.Sink, ctx context.Context) {
		err := s.Close(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("unable to close the upsert-sink due to error %e", err))
		}
	}(sUpsert, ctx)

	// ===================================================================
	// Many:true tests — use sMany (test_many table, no unique on user_id)
	// ===================================================================

	t.Run("Many: single model updates all matching rows via raw SQL", func(t *testing.T) {
		// Seed 5 rows for user-1 and 1 row for user-2
		for i := 1; i <= 5; i++ {
			_, err := pool.Exec(ctx,
				`INSERT INTO test_many (user_id, campaign, fulfilled, status) VALUES ($1, $2, FALSE, 'pending')`,
				"user-1", fmt.Sprintf("camp-%d", i),
			)
			require.NoError(t, err)
		}
		_, err := pool.Exec(ctx,
			`INSERT INTO test_many (user_id, campaign, fulfilled, status) VALUES ('user-2', 'camp-99', FALSE, 'pending')`,
		)
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "user-1",
				Filter:         []any{"user-1"},
				Update:         `UPDATE test_many SET fulfilled = TRUE, status = 'completed' WHERE user_id = $1`,
				Many:           true,
			},
		}

		res, err := sMany.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Empty(t, res.Errors)

		// Verify all 5 user-1 rows updated
		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE user_id = 'user-1' AND fulfilled = TRUE AND status = 'completed'`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 5, count)

		// Verify user-2 row untouched
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE user_id = 'user-2' AND fulfilled = FALSE`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Degraded Write with Many:true executes raw SQL", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM test_many`)
		require.NoError(t, err)

		for i := 1; i <= 4; i++ {
			_, err := pool.Exec(ctx,
				`INSERT INTO test_many (user_id, campaign) VALUES ('degraded-user', $1)`,
				fmt.Sprintf("c%d", i),
			)
			require.NoError(t, err)
		}

		model := sink.WriteModel{
			CorrelationKey: "degraded-user",
			Filter:         []any{"degraded-user"},
			Update:         `UPDATE test_many SET fulfilled = TRUE, status = 'done' WHERE user_id = $1`,
			Many:           true,
		}

		err = sMany.Write(ctx, model)
		require.NoError(t, err)

		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE user_id = 'degraded-user' AND fulfilled = TRUE`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 4, count, "degraded Write with Many:true must update all matching rows")
	})

	t.Run("SQL error in Many model is classified as permanent", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "bad-column",
				Filter:         []any{},
				Update:         `UPDATE test_many SET nonexistent_col = TRUE`,
				Many:           true,
			},
		}

		res, err := sMany.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res.Errors, 1)

		assert.Equal(t, "bad-column", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)
		assert.True(t, res.Errors[0].IsPermanent())
	})

	t.Run("Invalid Update type for Many returns permanent error", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "bad-type",
				Filter:         []any{},
				Update:         map[string]any{"col": "val"}, // Wrong: must be string for Many
				Many:           true,
			},
		}

		res, err := sMany.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)
		assert.Equal(t, "bad-type", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)
		assert.Contains(t, res.Errors[0].Err.Error(), "raw SQL string")
	})

	t.Run("Invalid Filter type for Many returns permanent error", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "bad-filter",
				Filter:         "not-a-slice", // Wrong: must be []any for Many
				Update:         `UPDATE test_many SET fulfilled = TRUE WHERE user_id = $1`,
				Many:           true,
			},
		}

		res, err := sMany.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)
		assert.Equal(t, "bad-filter", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)
		assert.Contains(t, res.Errors[0].Err.Error(), "[]any slice")
	})

	t.Run("Nil Filter executes SQL without arguments", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM test_many`)
		require.NoError(t, err)

		for i := 1; i <= 3; i++ {
			_, err := pool.Exec(ctx,
				`INSERT INTO test_many (user_id, campaign) VALUES ('nil-filter-user', $1)`,
				fmt.Sprintf("c%d", i),
			)
			require.NoError(t, err)
		}

		models := []sink.WriteModel{
			{
				CorrelationKey: "nil-filter",
				Filter:         nil, // No arguments
				Update:         `UPDATE test_many SET fulfilled = TRUE`,
				Many:           true,
			},
		}

		res, err := sMany.BulkWrite(ctx, models)
		require.NoError(t, err)
		assert.Empty(t, res.Errors)

		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE fulfilled = TRUE`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("Idempotent reapplication: applying Many write twice is safe", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM test_many`)
		require.NoError(t, err)

		_, err = pool.Exec(ctx,
			`INSERT INTO test_many (user_id, campaign) VALUES ('idem-user', 'c1'), ('idem-user', 'c2')`,
		)
		require.NoError(t, err)

		model := sink.WriteModel{
			CorrelationKey: "idem-user",
			Filter:         []any{"idem-user"},
			Update:         `UPDATE test_many SET fulfilled = TRUE WHERE user_id = $1`,
			Many:           true,
		}

		// First application
		r1, err := sMany.BulkWrite(ctx, []sink.WriteModel{model})
		require.NoError(t, err)
		assert.Empty(t, r1.Errors)

		// Second application: idempotent, no error
		r2, err := sMany.BulkWrite(ctx, []sink.WriteModel{model})
		require.NoError(t, err)
		assert.Empty(t, r2.Errors, "re-applying a Many write must not error")

		// Final state correct
		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE user_id = 'idem-user' AND fulfilled = TRUE`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	// ===================================================================
	// Many:false / Mixed tests — use sUpsert (test_upsert table, user_id PK)
	// ===================================================================

	t.Run("Mixed batch: Many:true raw SQL + Many:false upsert both execute", func(t *testing.T) {
		// Seed rows for the Many model (in test_many table)
		for i := 1; i <= 3; i++ {
			_, err := pool.Exec(ctx,
				`INSERT INTO test_many (user_id, campaign) VALUES ('batch-user', $1)`,
				fmt.Sprintf("c%d", i),
			)
			require.NoError(t, err)
		}

		// Many model targets test_many; upsert model targets test_upsert via sUpsert
		// Since a single BulkWrite goes to ONE sink/table, we test them separately:

		// Many path via sMany
		manyModels := []sink.WriteModel{
			{
				CorrelationKey: "batch-many",
				Filter:         []any{"batch-user"},
				Update:         `UPDATE test_many SET fulfilled = TRUE WHERE user_id = $1`,
				Many:           true,
			},
		}
		res, err := sMany.BulkWrite(ctx, manyModels)
		require.NoError(t, err)
		assert.Empty(t, res.Errors)

		// Upsert path via sUpsert
		upsertModels := []sink.WriteModel{
			{
				CorrelationKey: "batch-single",
				Update: map[string]any{
					"user_id":   "new-user",
					"campaign":  "solo",
					"fulfilled": true,
					"status":    "done",
				},
				Upsert: true,
				Many:   false,
			},
		}
		res, err = sUpsert.BulkWrite(ctx, upsertModels)
		require.NoError(t, err)
		assert.Empty(t, res.Errors)

		// Verify Many path
		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_many WHERE user_id = 'batch-user' AND fulfilled = TRUE`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Verify upsert path
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_upsert WHERE user_id = 'new-user'`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Many:false default behavior preserved (no regression)", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "regression",
				Update: map[string]any{
					"user_id":   "regression-user",
					"campaign":  "solo",
					"fulfilled": false,
					"status":    "new",
				},
				Upsert: true,
				Many:   false, // explicit default
			},
		}

		res, err := sUpsert.BulkWrite(ctx, models)
		require.NoError(t, err)
		assert.Empty(t, res.Errors)

		// Verify the row was inserted via ON CONFLICT
		var count int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM test_upsert WHERE user_id = 'regression-user'`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}
