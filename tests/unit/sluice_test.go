package unit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

const testRedisAddr = "localhost:6379"

type testPayload struct {
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

func testContract(key string, payload []byte) (*sluice.WriteModel, error) {
	var p testPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: key}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: p.Value}}}},
		Upsert: true,
	}, nil
}

func mustPayload(t *testing.T, value string) []byte {
	t.Helper()
	b, err := json.Marshal(testPayload{Value: value, UpdatedAt: time.Now()})
	require.NoError(t, err)
	return b
}

func buildSluice(t *testing.T, sk *mock.Sink, opts ...func(*sluice.Builder)) *sluice.Sluice {
	t.Helper()
	b := sluice.New("unit_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(4).
		WithKeyTTL(5 * time.Second)
	for _, opt := range opts {
		opt(b)
	}
	sl, err := b.Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(ctx)
	})
	return sl
}

func TestWrite_SingleKey(t *testing.T) {
	sk := mock.New()
	sl := buildSluice(t, sk)
	require.NoError(t, sl.Write(context.Background(), "key_001", mustPayload(t, "hello")))
	require.Eventually(t, func() bool { return sk.WrittenCount() == 1 }, 2*time.Second, 20*time.Millisecond)
	assert.Contains(t, sk.Written(), "key_001")
}

func TestWrite_UniqueKeys(t *testing.T) {
	const n = 500
	sk := mock.New()
	sl := buildSluice(t, sk)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("crn_%06d", i)
		require.NoError(t, sl.Write(context.Background(), key, mustPayload(t, key)))
	}
	require.Eventually(t, func() bool { return sk.WrittenCount() == n }, 5*time.Second, 50*time.Millisecond)
}

func TestWrite_DeduplicatesSameKey(t *testing.T) {
	sk := mock.New()
	sl := buildSluice(t, sk)
	for i := 0; i < 50; i++ {
		require.NoError(t, sl.Write(context.Background(), "crn_same", mustPayload(t, fmt.Sprintf("v%d", i))))
	}
	require.Eventually(t, func() bool { return sk.WrittenCount() >= 1 }, 3*time.Second, 20*time.Millisecond)
	assert.Equal(t, 1, sk.WrittenCount(), "multiple writes for same key must coalesce to one document")
}

func TestWrite_ConcurrentSafety(t *testing.T) {
	const goroutines, writesEach = 50, 20
	sk := mock.New()
	sl := buildSluice(t, sk)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				_ = sl.Write(context.Background(), fmt.Sprintf("crn_g%d_i%d", gID, i), mustPayload(t, "v"))
			}
		}(g)
	}
	wg.Wait()
	require.Eventually(t, func() bool { return sk.WrittenCount() == goroutines*writesEach }, 10*time.Second, 100*time.Millisecond)
}

func TestWrite_EmptyKeyRejected(t *testing.T) {
	sk := mock.New()
	sl := buildSluice(t, sk)
	assert.ErrorIs(t, sl.Write(context.Background(), "", mustPayload(t, "v")), sluice.ErrEmptyCorrelationKey)
}

func TestWrite_AfterClose(t *testing.T) {
	sk := mock.New()
	sl := buildSluice(t, sk)
	ctx := context.Background()
	require.NoError(t, sl.DrainAndClose(ctx))
	assert.ErrorIs(t, sl.Write(ctx, "key", mustPayload(t, "v")), sluice.ErrLibraryClosed)
}

func TestDrainAndClose_Idempotent(t *testing.T) {
	sk := mock.New()
	sl := buildSluice(t, sk)
	ctx := context.Background()
	assert.NoError(t, sl.DrainAndClose(ctx))
	assert.NoError(t, sl.DrainAndClose(ctx))
}

func TestBuild_MissingSink(t *testing.T) {
	_, err := sluice.New("test").WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).WithWriteContract(testContract).Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingSink)
}

func TestBuild_MissingContract(t *testing.T) {
	_, err := sluice.New("test").WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).WithSink(mock.New()).Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingContract)
}

func TestBuild_MissingNamespace(t *testing.T) {
	_, err := sluice.New("").WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).WithSink(mock.New()).WithWriteContract(testContract).Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingNamespace)
}

func TestBuild_MissingRedis(t *testing.T) {
	_, err := sluice.New("test").WithSink(mock.New()).WithWriteContract(testContract).Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingRedis)
}

func TestWrite_VolumeTrigger(t *testing.T) {
	const batchSize = 20
	sk := mock.New()
	sl, err := sluice.New("vol_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).WithWriteContract(testContract).
		WithFlushWindow(60 * time.Second).WithMaxBatchSize(batchSize).
		WithBandCount(1).WithKeyTTL(10 * time.Second).Build(context.Background())
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel(); _ = sl.DrainAndClose(ctx)
	}()
	for i := 0; i < batchSize+1; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("vol_%d", i), mustPayload(t, "v")))
	}
	require.Eventually(t, func() bool { return sk.WrittenCount() >= batchSize }, 5*time.Second, 50*time.Millisecond)
}

func TestOnFlushCallback(t *testing.T) {
	sk := mock.New()
	var mu sync.Mutex
	var flushedKeys []string
	sl, err := sluice.New("cb_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).WithWriteContract(testContract).
		WithFlushWindow(30 * time.Millisecond).WithBandCount(4).WithKeyTTL(5 * time.Second).
		OnFlush(func(keys []string, _ *sluice.BulkWriteResult, _ error) {
			mu.Lock(); flushedKeys = append(flushedKeys, keys...); mu.Unlock()
		}).Build(context.Background())
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel(); _ = sl.DrainAndClose(ctx)
	}()
	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("cb_%d", i), mustPayload(t, "v")))
	}
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(flushedKeys) == n }, 3*time.Second, 30*time.Millisecond)
}
