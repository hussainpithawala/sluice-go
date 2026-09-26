package shield

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// BulkJournalItem represents a single Source result to hydrate into the journal.
type BulkJournalItem struct {
	CorrelationKey string
	Payload        []byte
}

// HydrateJournal seeds the journal with a payload read from the Source, but
// only when the journal has no entry for the key (see hydrateLua); otherwise
// the existing, newer entry is returned. ts should be captured BEFORE the
// Source read; it becomes the entry's version for L1 gating. The key is never
// marked dirty, so hydrated data is not flushed back to the sink. No
// broadcast is emitted — peers heal from L2 on their next miss.
func (s *Shield) HydrateJournal(ctx context.Context, correlationKey string, payload []byte, ts int64) (HydrateResult, error) {
	res, err := s.hydrateScript.Run(ctx, s.client, s.hydrateKeys(correlationKey), s.hydrateArgs(payload, ts)...).Result()
	if err != nil {
		return HydrateResult{}, err
	}
	return parseHydrateResult(res, payload, ts)
}

// HydrateHotLoad is HydrateJournal for HotLoad promotions: when the payload
// is written it also emits a kind=seed broadcast so peer L1 caches converge
// on the promoted key (RFC §5.4).
func (s *Shield) HydrateHotLoad(ctx context.Context, correlationKey string, payload []byte, ts int64) (HydrateResult, error) {
	hr, err := s.HydrateJournal(ctx, correlationKey, payload, ts)
	if err != nil || !hr.Written {
		return hr, err
	}
	return hr, s.XAddBroadcast(ctx, correlationKey, ts, broadcastKindSeed, payload)
}

// BulkHydrateJournal applies HydrateJournal to every item in a single Redis
// pipeline. Results are returned in the same order as items.
func (s *Shield) BulkHydrateJournal(ctx context.Context, items []BulkJournalItem, ts int64) ([]HydrateResult, error) {
	if len(items) == 0 {
		return nil, nil
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(items))
	for i, item := range items {
		cmds[i] = pipe.EvalSha(ctx, s.hydrateScriptSHA, s.hydrateKeys(item.CorrelationKey), s.hydrateArgs(item.Payload, ts)...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	results := make([]HydrateResult, len(items))
	for i, cmd := range cmds {
		hr, err := parseHydrateResult(cmd.Val(), items[i].Payload, ts)
		if err != nil {
			return nil, err
		}
		results[i] = hr
	}
	return results, nil
}

func (s *Shield) hydrateKeys(correlationKey string) []string {
	return []string{s.payloadKey(correlationKey)}
}

func (s *Shield) hydrateArgs(payload []byte, ts int64) []interface{} {
	return []interface{}{payload, strconv.FormatInt(ts, 10), s.activityWindow.Milliseconds()}
}

// parseHydrateResult decodes hydrateLua's reply: {1} or {0, payload, ts}.
func parseHydrateResult(res interface{}, payload []byte, ts int64) (HydrateResult, error) {
	reply, ok := res.([]interface{})
	if !ok || len(reply) == 0 {
		return HydrateResult{}, fmt.Errorf("sluice/shield: unexpected hydrate reply %v", res)
	}
	if written, _ := reply[0].(int64); written == 1 {
		return HydrateResult{Written: true, Payload: payload, Version: ts}, nil
	}
	if len(reply) < 3 {
		return HydrateResult{}, fmt.Errorf("sluice/shield: unexpected hydrate reply %v", res)
	}
	cur, _ := reply[1].(string)
	tsStr, _ := reply[2].(string)
	var version int64
	if f, err := strconv.ParseFloat(tsStr, 64); err == nil {
		version = int64(f)
	}
	return HydrateResult{Payload: []byte(cur), Version: version}, nil
}
