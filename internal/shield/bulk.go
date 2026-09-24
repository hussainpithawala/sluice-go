package shield

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// BulkJournalItem represents a single item to be written to the journal in bulk.
type BulkJournalItem struct {
	CorrelationKey string
	Payload        []byte
}

// BulkWriteJournal writes multiple payloads to the Redis journal in a single pipeline.
// It sets the payload, timestamp, and extends the TTL to the provided duration.
func (s *Shield) BulkWriteJournal(ctx context.Context, items []BulkJournalItem, ts float64, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	for _, item := range items {
		band := s.BandFor(item.CorrelationKey)
		key := PayloadKey(s.namespace, band, item.CorrelationKey)

		pipe.HSet(ctx, key, map[string]interface{}{
			"p":  item.Payload,
			"ts": ts,
		})
		pipe.Expire(ctx, key, ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// BulkIndexItem represents the index fields for a single correlation key.
type BulkIndexItem struct {
	CorrelationKey string
	Fields         map[string]interface{}
}

// BulkUpdateIndexes updates secondary indexes for multiple keys in a single pipeline.
func (s *Shield) BulkUpdateIndexes(ctx context.Context, items []BulkIndexItem, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	for _, item := range items {
		band := s.BandFor(item.CorrelationKey)
		for field, val := range item.Fields {
			switch v := val.(type) {
			case string:
				idxKey := fmt.Sprintf("sl:%s:idx:{%d}:%s:%s", s.namespace, band, field, v)
				pipe.SAdd(ctx, idxKey, item.CorrelationKey)
				pipe.Expire(ctx, idxKey, ttl)
			case float64, int, int64:
				var score float64
				switch n := v.(type) {
				case float64:
					score = n
				case int:
					score = float64(n)
				case int64:
					score = float64(n)
				}
				ridxKey := fmt.Sprintf("sl:%s:ridx:{%d}:%s", s.namespace, band, field)
				pipe.ZAdd(ctx, ridxKey, redis.Z{Score: score, Member: item.CorrelationKey})
				pipe.Expire(ctx, ridxKey, ttl)
			}
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}
