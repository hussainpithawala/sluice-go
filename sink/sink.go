// Package sink defines the FlushSink interface that all document store
// backends must implement to integrate with sluice.
package sink

import (
	"context"

	"github.com/hussainpithawala/sluice-go"
)

// FlushSink is the write-side persistence contract for sluice.
type FlushSink interface {
	BulkWrite(ctx context.Context, models []WriteModel) (*sluice.BulkWriteResult, error)
	Write(ctx context.Context, model WriteModel) error
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// WriteModel is the resolved upsert instruction handed to the FlushSink.
type WriteModel struct {
	CorrelationKey string
	Filter         interface{}
	Update         interface{}
	Upsert         bool
}
