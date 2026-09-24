package docdb

import (
	"github.com/hussainpithawala/sluice-go/source"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DocDBBulkReadModel is the MongoDB/DocumentDB-specific execution plan for bulk reads.
type DocDBBulkReadModel struct {
	// Filter is the MongoDB query filter (e.g., bson.M{"user_id": userID, "status": "active"}).
	Filter interface{}

	// Options allows the operator to specify Sort, Limit, Projection, etc.
	// This is critical for bounding result sets and optimizing cursor performance.
	Options *options.FindOptions

	// Projector iterates over the returned mongo.Cursor and shapes the documents
	// into BulkReadResults.
	Projector func(cursor *mongo.Cursor) ([]source.BulkReadResult, error)
}
