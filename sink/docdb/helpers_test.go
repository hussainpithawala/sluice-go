package docdb

import (
	"os"
	"testing"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	return Config{
		URI:        uri,
		Database:   "sluice_test_update_many",
		Collection: "test_collection",
	}
}
