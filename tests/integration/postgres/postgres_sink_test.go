package postgres_test

import (
	"context"
	"fmt"
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

func TestPostgresBulkWrite_SilentLossPrevention(t *testing.T) {
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
	defer pgC.Terminate(ctx)

	host, _ := pgC.Host(ctx)
	port, _ := pgC.MappedPort(ctx, "5432")
	connString := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())

	// 2. Create test table
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE test_table (
			user_id TEXT PRIMARY KEY,
			email TEXT UNIQUE,
			val INTEGER
		)
	`)
	require.NoError(t, err)

	tableName := "test_table"

	// 3. Initialize the sink
	s, err := sinkpostgres.New(ctx, sinkpostgres.Config{
		ConnString:      connString,
		TableName:       tableName,
		ConflictColumns: []string{"user_id"}, // Fixed: match the actual primary key
		OnConflict:      sinkpostgres.OnConflictDoUpdate,
	})
	require.NoError(t, err)
	defer s.Close(ctx)

	t.Run("Column mismatch: Row with different columns gets permanent error, rest succeed", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "user1",
				Update:         map[string]any{"user_id": "user1", "email": "a@b.com", "val": 1},
			},
			{
				CorrelationKey: "user2",
				Update:         map[string]any{"user_id": "user2", "email": "c@d.com"}, // Missing "val" column
			},
			{
				CorrelationKey: "user3",
				Update:         map[string]any{"user_id": "user3", "email": "e@f.com", "val": 3},
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err) // No top-level error
		require.Len(t, res.Errors, 1)
		assert.Equal(t, "user2", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)

		// Verify user1 and user3 were written
		var count int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_table WHERE user_id IN ('user1', 'user3')").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("Duplicate conflict key: Second row with same key gets permanent error", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "user4",
				Update:         map[string]any{"user_id": "dup_key", "email": "g@h.com", "val": 4},
			},
			{
				CorrelationKey: "user5",
				Update:         map[string]any{"user_id": "dup_key", "email": "i@j.com", "val": 5}, // Duplicate user_id
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)
		assert.Equal(t, "user5", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)

		// Verify only one row was written
		var count int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_table WHERE user_id = 'dup_key'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Unique violation (23505): Fails only that row, commits the rest", func(t *testing.T) {
		// Insert a row with a specific email
		_, err := pool.Exec(ctx, "INSERT INTO test_table (user_id, email, val) VALUES ('existing', 'unique@test.com', 99)")
		require.NoError(t, err)

		// Try to insert a batch where one row violates the unique email constraint
		models := []sink.WriteModel{
			{
				CorrelationKey: "user6",
				Update:         map[string]any{"user_id": "user6", "email": "new@test.com", "val": 6},
			},
			{
				CorrelationKey: "user7",
				Update:         map[string]any{"user_id": "user7", "email": "unique@test.com", "val": 7}, // Violates unique email
			},
			{
				CorrelationKey: "user8",
				Update:         map[string]any{"user_id": "user8", "email": "another@test.com", "val": 8},
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)
		assert.Equal(t, "user7", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)

		// Verify user6 and user8 were written, user7 was not
		var count int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_table WHERE user_id IN ('user6', 'user8')").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		var emailExists bool
		err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM test_table WHERE user_id = 'user7')").Scan(&emailExists)
		require.NoError(t, err)
		assert.False(t, emailExists)
	})

	t.Run("DoNothing replay: First row kept, no error on duplicate", func(t *testing.T) {
		// Create a sink with OnConflictDoNothing
		sDoNothing, err := sinkpostgres.New(ctx, sinkpostgres.Config{
			ConnString:      connString,
			TableName:       "test_table",
			ConflictColumns: []string{"user_id"},
			OnConflict:      sinkpostgres.OnConflictDoNothing,
		})
		require.NoError(t, err)
		defer sDoNothing.Close(ctx)

		// Insert a row
		_, err = pool.Exec(ctx, "INSERT INTO test_table (user_id, email, val) VALUES ('donothing_user', 'first@test.com', 10)")
		require.NoError(t, err)

		// Try to insert the same user_id with different data
		models := []sink.WriteModel{
			{
				CorrelationKey: "donothing_user",
				Update:         map[string]any{"user_id": "donothing_user", "email": "second@test.com", "val": 20},
			},
		}

		res, err := sDoNothing.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 0) // No error with DoNothing

		// Verify the first row is kept
		var email string
		var val int
		err = pool.QueryRow(ctx, "SELECT email, val FROM test_table WHERE user_id = 'donothing_user'").Scan(&email, &val)
		require.NoError(t, err)
		assert.Equal(t, "first@test.com", email)
		assert.Equal(t, 10, val)
	})
}
