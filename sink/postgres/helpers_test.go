package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// testPostgresSink initializes a Postgres sink with the required TableName and ConflictColumns.
func testPostgresSink(t *testing.T, tableName string, conflictColumns []string) *Sink {
	t.Helper()

	connStr := os.Getenv("TEST_POSTGRES_DSN")

	if connStr == "" {
		// Update this to match YOUR local Postgres credentials
		connStr = "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable"
	}

	s, err := New(context.Background(), Config{
		ConnString:      connStr,
		TableName:       tableName,
		ConflictColumns: conflictColumns,
	})
	require.NoError(t, err)
	return s
}
