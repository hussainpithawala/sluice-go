package sluice

import (
	"context"
	"fmt"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
)

// ReadBulk executes a set-based read operation, hydrating the L1/L2 journals
// for all returned items in a single atomic Redis pipeline.
func (s *Sluice) ReadBulk(ctx context.Context, lookupKey string) (map[string][]byte, error) {
	payloads, _, err := s.ReadBulkWithTTL(ctx, lookupKey)
	return payloads, err
}

// ReadBulkWithTTL is a variant of ReadBulk that also returns the remaining
// TTL for each hydrated key in the L2 journal. All Redis operations are
// strictly delegated to the shield package.
func (s *Sluice) ReadBulkWithTTL(ctx context.Context, lookupKey string) (map[string][]byte, map[string]int64, error) {
	if s.closed.Load() {
		return nil, nil, ErrLibraryClosed
	}
	if s.readBulkContract == nil {
		return nil, nil, fmt.Errorf("sluice: ReadBulkContract is not configured")
	}
	if s.src == nil {
		return nil, nil, fmt.Errorf("sluice: Source is not configured")
	}

	// 1. Invoke the contract to get the execution plan
	bulkModel, err := s.readBulkContract(lookupKey)
	if err != nil {
		return nil, nil, fmt.Errorf("sluice: read bulk contract failed: %w", err)
	}

	// 2. Execute the adapter-specific bulk read
	results, err := s.src.ReadBulk(ctx, *bulkModel)
	if err != nil {
		return nil, nil, fmt.Errorf("sluice: source read bulk failed: %w", err)
	}

	if len(results) == 0 {
		return map[string][]byte{}, map[string]int64{}, nil
	}

	// 3. Prepare data for bulk journal hydration
	currentTs := float64(time.Now().UnixMilli())
	journalItems := make([]shield.BulkJournalItem, 0, len(results))
	payloadMap := make(map[string][]byte, len(results))
	correlationKeys := make([]string, 0, len(results))

	for _, res := range results {
		payloadMap[res.CorrelationKey] = res.Payload
		correlationKeys = append(correlationKeys, res.CorrelationKey)
		journalItems = append(journalItems, shield.BulkJournalItem{
			CorrelationKey: res.CorrelationKey,
			Payload:        res.Payload,
		})
	}

	// 4. Delegate bulk L2 Journal hydration to the shield package
	err = s.shield.BulkWriteJournal(ctx, journalItems, currentTs, s.cfg.ActivityWindow)
	if err != nil {
		return nil, nil, fmt.Errorf("sluice: bulk journal hydration failed: %w", err)
	}

	// 5. Delegate bulk secondary index updates to the shield package (if configured)
	if s.indexBulkContract != nil {
		indexMap, err := s.indexBulkContract(results)
		if err != nil {
			if s.metrics != nil {
				s.metrics.RecordContractError(s.cfg.Namespace, lookupKey, fmt.Errorf("index bulk contract: %w", err))
			}
		} else if len(indexMap) > 0 {
			indexItems := make([]shield.BulkIndexItem, 0, len(indexMap))
			for crn, fields := range indexMap {
				indexItems = append(indexItems, shield.BulkIndexItem{
					CorrelationKey: crn,
					Fields:         fields,
				})
			}
			if idxErr := s.shield.BulkUpdateIndexes(ctx, indexItems, s.cfg.ActivityWindow); idxErr != nil {
				if s.metrics != nil {
					s.metrics.RecordRedisOp(s.cfg.Namespace, "bulkupdateindexes", 0, idxErr)
				}
			}
		}
	}

	// 6. Hydrate L1 Local Cache (if enabled)
	if s.local != nil {
		tsInt := int64(currentTs)
		for _, res := range results {
			s.local.Put(res.CorrelationKey, res.Payload, tsInt)
		}
	}

	// 7. Delegate TTL fetching entirely to the shield package
	ttlMap, err := s.shield.BulkGetTTL(ctx, correlationKeys)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordRedisOp(s.cfg.Namespace, "bulkgetttl", 0, err)
		}
		// Graceful fallback: approximate TTL as ActivityWindow if pipeline fails
		ttlMap = make(map[string]int64, len(correlationKeys))
		for _, ck := range correlationKeys {
			ttlMap[ck] = s.cfg.ActivityWindow.Milliseconds()
		}
	}

	return payloadMap, ttlMap, nil
}
