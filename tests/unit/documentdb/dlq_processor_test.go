package documentdb_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

const (
	dlqProcBands  = 2
	dlqProcKeyTTL = 10 * time.Second
)

// dlqNoopMetrics satisfies dlq.MetricsRecorder.
type dlqNoopMetrics struct{}

func (dlqNoopMetrics) RecordDLQProcess(string, string, int, int, int) {}

// allFailSink returns per-key permanent errors for every model.
type allFailSink struct {
	calls atomic.Int64
}

func (s *allFailSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	res := &sink.BulkWriteResult{}
	for _, m := range models {
		res.Errors = append(res.Errors, sink.SinkError{
			CorrelationKey: m.CorrelationKey,
			Code:           11000,
			Class:          sink.ClassPermanent,
			Err:            fmt.Errorf("dup"),
		})
	}
	return res, nil
}
func (s *allFailSink) Write(context.Context, sink.WriteModel) error { return errors.New("x") }
func (s *allFailSink) Ping(context.Context) error                   { return nil }
func (s *allFailSink) Close(context.Context) error                  { return nil }
func (s *allFailSink) ClassifiesErrors()                            {}

// keyFailSink fails specific keys permanently, succeeds all others.
type keyFailSink struct {
	calls   atomic.Int64
	failSet map[string]bool
}

func (s *keyFailSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	res := &sink.BulkWriteResult{}
	for _, m := range models {
		if s.failSet[m.CorrelationKey] {
			res.Errors = append(res.Errors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Code:           11000,
				Class:          sink.ClassPermanent,
				Err:            fmt.Errorf("dup"),
			})
		}
	}
	return res, nil
}
func (s *keyFailSink) Write(context.Context, sink.WriteModel) error { return errors.New("x") }
func (s *keyFailSink) Ping(context.Context) error                   { return nil }
func (s *keyFailSink) Close(context.Context) error                  { return nil }
func (s *keyFailSink) ClassifiesErrors()                            {}

func dlqTestShield(t *testing.T, ns string) *shield.Shield {
	t.Helper()
	sh, err := shield.New(shield.RedisConfig{
		Addrs:       []string{testRedisAddr},
		DialTimeout: 5 * time.Second,
		PoolSize:    10,
	}, ns, dlqProcBands, dlqProcKeyTTL, 4*time.Hour, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sh.Close() })
	return sh
}

func seedDLQ(t *testing.T, sh *shield.Shield, ns string, keys []string) {
	t.Helper()
	ctx := context.Background()
	for _, k := range keys {
		require.NoError(t, sh.Write(ctx, k, []byte(`{"v":1}`)))
	}
	// Wait for keys to land in dirty set, then flush to move to DLQ.
	time.Sleep(50 * time.Millisecond)
	for band := 0; band < dlqProcBands; band++ {
		require.NoError(t, sh.MoveToDeadLetter(ctx, band, keysForBand(keys, band), "test"))
	}
}

func keysForBand(keys []string, band int) []string {
	var out []string
	for _, k := range keys {
		if shield.BandForKey(k, dlqProcBands) == band {
			out = append(out, k)
		}
	}
	return out
}

func dlqDepth(t *testing.T, ns string) int64 {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	defer func() { _ = rc.Close() }()
	var total int64
	for band := 0; band < dlqProcBands; band++ {
		n, err := rc.ZCard(context.Background(), shield.DLQKey(ns, band)).Result()
		require.NoError(t, err)
		total += n
	}
	return total
}

func passThroughContract(ck string, payload []byte) (*sink.WriteModel, error) {
	return &sink.WriteModel{
		Filter: map[string]string{"_id": ck},
		Update: string(payload),
		Upsert: true,
	}, nil
}

// TestDLQProcessor_NoProgressExitsLoop proves that when every record in a
// batch fails (per-key sink errors), Process returns after one BulkWrite per
// band — not a tight loop.
func TestDLQProcessor_NoProgressExitsLoop(t *testing.T) {
	const ns = "dlq_noprogress_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("ck-%04d", i)
	}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns), "DLQ must be seeded")

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(dlqProcBands), fs.calls.Load(),
		"BulkWrite should be called at most once per band (no tight loop)")
	assert.Positive(t, result.Failed, "all records should be counted as failed")
	assert.Zero(t, result.Succeeded, "no records should succeed")

	// DLQ entries should remain (they failed, not committed).
	assert.Positive(t, dlqDepth(t, ns), "failed records stay in DLQ for next tick")
}

// TestDLQProcessor_ContractErrorCommitsDuringReplay proves that records
// whose contract fails during DLQ replay are committed (removed from DLQ)
// rather than being re-drained indefinitely.
func TestDLQProcessor_ContractErrorCommitsDuringReplay(t *testing.T) {
	const ns = "dlq_contract_commit_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := []string{"ck-a", "ck-b", "ck-c"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	alwaysFailContract := func(string, []byte) (*sink.WriteModel, error) {
		return nil, errors.New("bad payload")
	}

	// Sink should never be called — all records fail contract first.
	fs := &allFailSink{}
	proc := dlq.NewProcessor(sh, fs, alwaysFailContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Zero(t, fs.calls.Load(), "sink should never be called when all contracts fail")
	assert.Equal(t, len(keys), result.Failed, "all keys should be counted as failed")
	assert.Zero(t, dlqDepth(t, ns), "contract-failed keys must be removed from DLQ")
}

// TestDLQProcessor_PartialProgressContinues proves that when a batch has
// partial success, the loop re-drains for remaining records, then exits
// on the no-progress iteration.
func TestDLQProcessor_PartialProgressContinues(t *testing.T) {
	const ns = "dlq_partial_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("ck-%04d", i)
	}
	failSet := map[string]bool{keys[1]: true, keys[3]: true, keys[5]: true}
	fs := &keyFailSink{failSet: failSet}

	seedDLQ(t, sh, ns, keys)
	initialDepth := dlqDepth(t, ns)
	require.Positive(t, initialDepth)

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Positive(t, result.Succeeded, "some records should succeed")
	assert.Positive(t, result.Failed, "some records should fail")
	finalDepth := dlqDepth(t, ns)
	assert.Less(t, finalDepth, initialDepth, "successful records should be removed from DLQ")
	assert.Positive(t, finalDepth, "failed records should remain in DLQ")
}
