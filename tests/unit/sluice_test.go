package unit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	testRedisAddr  = "localhost:6379"
	testMongoURI   = "mongodb://localhost:27017"
	testDatabase   = "sluice_unit_test"
	testCollection = "test_docs"
)

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

func buildSluice(t *testing.T, opts ...func(*sluice.Builder)) (*sluice.Sluice, *docdb.Sink) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI:         testMongoURI,
		Database:    testDatabase,
		Collection:  testCollection,
		MaxPoolSize: 10,
		MinPoolSize: 1,
	})
	require.NoError(t, err)

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
	sl, err := b.Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(ctx)
	})
	return sl, sk
}

func TestWrite_SingleKey(t *testing.T) {
	sl, _ := buildSluice(t)
	require.NoError(t, sl.Write(context.Background(), "key_001", mustPayload(t, "hello")))
}

func TestWrite_UniqueKeys(t *testing.T) {
	const n = 500
	sl, _ := buildSluice(t)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("crn_%06d", i)
		require.NoError(t, sl.Write(context.Background(), key, mustPayload(t, key)))
	}
}

func TestWrite_DeduplicatesSameKey(t *testing.T) {
	sl, _ := buildSluice(t)
	for i := 0; i < 50; i++ {
		require.NoError(t, sl.Write(context.Background(), "crn_same", mustPayload(t, fmt.Sprintf("v%d", i))))
	}
}

func TestWrite_ConcurrentSafety(t *testing.T) {
	const goroutines, writesEach = 50, 20
	sl, _ := buildSluice(t)
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
}

func TestWrite_EmptyKeyRejected(t *testing.T) {
	sl, _ := buildSluice(t)
	assert.ErrorIs(t, sl.Write(context.Background(), "", mustPayload(t, "v")), sluice.ErrEmptyCorrelationKey)
}

func TestWrite_AfterClose(t *testing.T) {
	sl, _ := buildSluice(t)
	ctx := context.Background()
	require.NoError(t, sl.DrainAndClose(ctx))
	assert.ErrorIs(t, sl.Write(ctx, "key", mustPayload(t, "v")), sluice.ErrLibraryClosed)
}

func TestDrainAndClose_Idempotent(t *testing.T) {
	sl, _ := buildSluice(t)
	ctx := context.Background()
	assert.NoError(t, sl.DrainAndClose(ctx))
	assert.NoError(t, sl.DrainAndClose(ctx))
}

func TestBuild_MissingSink(t *testing.T) {
	_, err := sluice.New("test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingSink)
}

func TestBuild_MissingContract(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingContract)
}

func TestBuild_MissingNamespace(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingNamespace)
}

func TestBuild_MissingRedis(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("test").
		WithSink(sk).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingRedis)
}

func TestWrite_VolumeTrigger(t *testing.T) {
	const batchSize = 20
	sl, _ := buildSluice(t, func(b *sluice.Builder) {
		b.WithFlushWindow(60 * time.Second).
			WithMaxBatchSize(batchSize).
			WithBandCount(1).
			WithKeyTTL(10 * time.Second)
	})
	for i := 0; i < batchSize+1; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("vol_%d", i), mustPayload(t, "v")))
	}
}

func TestOnFlushCallback(t *testing.T) {
	var mu sync.Mutex
	var flushedKeys []string
	sl, _ := buildSluice(t, func(b *sluice.Builder) {
		b.WithFlushWindow(30 * time.Millisecond).
			WithBandCount(4).
			WithKeyTTL(5 * time.Second).
			OnFlush(func(keys []string, _ *sluice.BulkWriteResult, _ error) {
				mu.Lock()
				flushedKeys = append(flushedKeys, keys...)
				mu.Unlock()
			})
	})
	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("cb_%d", i), mustPayload(t, "v")))
	}
}
