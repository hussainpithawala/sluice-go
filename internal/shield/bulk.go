package shield

import (
	"context"
	"time"
)

// BulkIndexItem represents the index fields for a single correlation key.
type BulkIndexItem struct {
	CorrelationKey string
	Fields         map[string]interface{}
}

// BulkUpdateIndexes updates secondary indexes for multiple keys in two
// pipelines: one to read previous values, one to apply the updates.
func (s *Shield) BulkUpdateIndexes(ctx context.Context, items []BulkIndexItem, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	cks := make([]string, len(items))
	for i, item := range items {
		cks[i] = item.CorrelationKey
	}
	olds, err := s.indexValues(ctx, cks)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	for i, item := range items {
		s.queueIndexUpdate(ctx, pipe, item.CorrelationKey, item.Fields, olds[i], ttl)
	}
	_, err = pipe.Exec(ctx)
	return err
}
