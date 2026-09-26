package sluice

import (
	"context"
	"fmt"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/source"
)

// ReadBulk executes a set-based read operation, hydrating the L1/L2 journals
// for all returned items in a single Redis pipeline. For keys already present
// in the journal, the journal payload is returned instead of the Source
// payload, preserving read-your-writes.
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

	// 2. Execute the adapter-specific bulk read. The hydration ts is captured
	//    BEFORE the query so any Write() landing while it is in flight carries
	//    a newer ts and wins the L1 version gate.
	bulkTs := time.Now().UnixMilli()
	results, err := s.src.ReadBulk(ctx, *bulkModel)
	if err != nil {
		return nil, nil, fmt.Errorf("sluice: source read bulk failed: %w", err)
	}

	if len(results) == 0 {
		return map[string][]byte{}, map[string]int64{}, nil
	}

	// 3. Prepare data for bulk journal hydration
	journalItems := make([]shield.BulkJournalItem, 0, len(results))
	correlationKeys := make([]string, 0, len(results))

	for _, res := range results {
		correlationKeys = append(correlationKeys, res.CorrelationKey)
		journalItems = append(journalItems, shield.BulkJournalItem{
			CorrelationKey: res.CorrelationKey,
			Payload:        res.Payload,
		})
	}

	// 4. Delegate bulk L2 hydration to the shield package. Only journal misses
	//    are filled; keys already in the journal (pending flush, flushed, or
	//    dead-lettered) keep that newer entry, which is what we return and
	//    mirror into L1.
	hydrated, err := s.shield.BulkHydrateJournal(ctx, journalItems, bulkTs)
	if err != nil {
		return nil, nil, fmt.Errorf("sluice: bulk journal hydration failed: %w", err)
	}

	payloadMap := make(map[string][]byte, len(results))
	written := make([]source.BulkReadResult, 0, len(results))
	for i, res := range results {
		hr := hydrated[i]
		payloadMap[res.CorrelationKey] = hr.Payload
		if hr.Written {
			written = append(written, res)
		}
	}

	// 5. Delegate bulk secondary index updates to the shield package (if configured).
	//    Only freshly hydrated items are indexed; winning journal entries were
	//    already indexed by the write path.
	if s.indexBulkContract != nil && len(written) > 0 {
		indexMap, err := s.indexBulkContract(written)
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
		for i, res := range results {
			s.local.Put(res.CorrelationKey, hydrated[i].Payload, hydrated[i].Version)
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
