package docdb

import (
	"testing"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestClassifyBulkErr(t *testing.T) {
	tests := []struct {
		name     string
		err      mongo.BulkWriteError
		isUpsert bool
		want     sink.ErrorClass
	}{
		{
			name: "11000 on _id_ during upsert is transient (concurrent pod race)",
			err: mongo.BulkWriteError{
				WriteError: mongo.WriteError{
					Code:    11000,
					Message: `E11000 duplicate key error collection: test.testcoll index: _id_ dup key: { _id: "user1" }`,
				},
			},
			isUpsert: true,
			want:     sink.ClassTransient,
		},
		{
			name: "11000 on business unique index is permanent",
			err: mongo.BulkWriteError{
				WriteError: mongo.WriteError{
					Code:    11000,
					Message: `E11000 duplicate key error collection: test.testcoll index: email_1 dup key: { email: "a@b.com" }`,
				},
			},
			isUpsert: true,
			want:     sink.ClassPermanent,
		},
		{
			name: "11000 on _id_ without upsert is permanent",
			err: mongo.BulkWriteError{
				WriteError: mongo.WriteError{
					Code:    11000,
					Message: `E11000 duplicate key error collection: test.testcoll index: _id_ dup key: { _id: "user1" }`,
				},
			},
			isUpsert: false,
			want:     sink.ClassPermanent,
		},
		{
			name: "121 (DocumentValidation) is permanent",
			err: mongo.BulkWriteError{
				WriteError: mongo.WriteError{
					Code:    121,
					Message: "Document failed validation",
				},
			},
			isUpsert: true,
			want:     sink.ClassPermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyBulkErr(tt.err, tt.isUpsert)
			assert.Equal(t, tt.want, got)
		})
	}
}
