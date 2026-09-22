// Command reader is "Pod B" in the Local Journal PushPull demo for DynamoDB.
//
// It never writes. It polls Read() for the shared correlation key and logs
// every value transition it observes. Convergence from the writer's
// broadcasts proves the PushPull path works across processes.
//
// Expected observation sequence:
//  1. misses while the writer hasn't published yet
//  2. sees priority=5  (first broadcast convergence)
//  3. sees priority=1  (writer updated; broadcast convergence)
//
// Run against DynamoDB Local + Valkey cluster:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000
//	REDIS_ADDRS=localhost:7001,localhost:7002,localhost:7003,localhost:7004
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/localjournal_pushpull_dynamodb/reader.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
	"github.com/hussainpithawala/sluice-go/source"
	dynsource "github.com/hussainpithawala/sluice-go/source/dynamodb"
)

// ─── Shared constants (must match writer) ───────────────────────────────────
const (
	namespace      = "nudge_inventory_dynamodb"
	correlationKey = "pushpull_demo_session_001"
	bandCount      = 16
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id"`
	Channel       string    `json:"channel"`
	Priority      int       `json:"priority"`
	CampaignID    string    `json:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	LastUpdated   time.Time `json:"last_updated"`
}

// ─── Contracts (identical to writer) ────────────────────────────────────────
func nudgeWriteContract(ck string, raw []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("write contract: %w", err)
	}

	item := map[string]any{
		"PK":              ck,
		"nudge_master_id": p.NudgeMasterID,
		"channel":         p.Channel,
		"priority":        p.Priority,
		"campaign_id":     p.CampaignID,
		"expires_at":      p.ExpiresAt.Format(time.RFC3339),
		"last_updated":    p.LastUpdated.Format(time.RFC3339),
	}

	return &sluice.WriteModel{
		Update: item,
		Upsert: true,
	}, nil
}

func nudgeReadContract(ck string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: map[string]any{"PK": ck},
	}, nil
}

func nudgeIndexContract(ck string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: %w", err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,
		"campaign": p.CampaignID,
		"priority": float64(p.Priority),
		"expires":  float64(p.ExpiresAt.UnixMilli()),
	}, nil
}

// ─── Metrics ────────────────────────────────────────────────────────────────
type logMetrics struct{ log *slog.Logger }

func (m *logMetrics) RecordWrite(string) {}
func (m *logMetrics) RecordDegradedWrite(ns string, err error) {
	m.log.Warn("degraded write", "ns", ns, "err", err)
}
func (m *logMetrics) RecordRedisOp(ns, op string, d time.Duration, err error) {
	if err != nil {
		m.log.Error("redis op", "ns", ns, "op", op, "err", err)
	}
}
func (m *logMetrics) RecordFlush(ns, band string, b int, d time.Duration, err error) {}
func (m *logMetrics) RecordDirtyQueueDepth(ns, band string, depth int)               {}
func (m *logMetrics) RecordContractError(ns, ck string, err error) {
	m.log.Error("contract error", "ns", ns, "ck", ck, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int)                {}
func (m *logMetrics) RecordDLQProcess(ns, s string, p, ok, f int)                {}
func (m *logMetrics) RecordWarmUp(ns string, d time.Duration, err error)         {}
func (m *logMetrics) RecordRead(ns string, d time.Duration, hot bool, err error) {}
func (m *logMetrics) RecordHotSetSize(ns string, size int)                       {}
func (m *logMetrics) RecordLocalCacheHit(ns string)                              {} // silent; transitions are the signal
func (m *logMetrics) RecordLocalCacheMiss(ns string, r localjournal.MissReason)  {}
func (m *logMetrics) RecordLocalSetSize(ns string, size int)                     {}
func (m *logMetrics) RecordBroadcastLag(ns string, lag time.Duration) {
	m.log.Info("broadcast-lag", "ns", ns, "lag_ms", lag.Milliseconds())
}

// ─── Main ───────────────────────────────────────────────────────────────────
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	endpoint := os.Getenv("DYNAMODB_ENDPOINT")

	// 1. Initialize DynamoDB Client
	httpClient := &http.Client{Timeout: 30 * time.Second}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	tableName := "NudgeInventoryPushPull"

	if err := ensureTableExists(ctx, client, tableName, log); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	sk := dynsink.NewSink(client, tableName)
	src := dynsource.NewSource(client, tableName)

	redisAddrs := splitAddrs(getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004"))
	clusterMode, _ := strconv.ParseBool(getEnv("REDIS_CLUSTER_MODE", "true"))

	sl, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{
			Addrs: redisAddrs, ClusterMode: clusterMode, PoolSize: 30,
			DialTimeout: 5 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).
		WithWriteContract(nudgeWriteContract).
		WithSource(src).
		WithReadContract(nudgeReadContract).
		WithIndexContract(nudgeIndexContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(bandCount).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour).
		WithHotAwareFlush(true).
		WithContentDedup(true).
		WithDegradedModeDirect(true).
		WithMetrics(&logMetrics{log: log}).
		WithLocalCache(localjournal.LocalCacheConfig{
			Mode:       localjournal.LocalCachePushPull,
			MaxEntries: 200_000,          // 256 shards × ~782 entries each
			LocalTTL:   60 * time.Second, // max staleness bound
			Broadcast:  shield.BroadcastPayload,
			Retention:  200_000, // stream MAXLEN ~ N
		}).
		Build(ctx)
	if err != nil {
		return fmt.Errorf("build sluice: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
		log.Info("reader drained and closed")
	}()

	log.Info("═══ READER (Pod B) ready — polling for convergence ═══", "namespace", namespace, "key", correlationKey)

	// Poll Read() and log every value transition.
	// Adjusted to match the exact scenarios executed by the DynamoDB writer.
	expectedSequence := []int{5, 1}
	nextExpected := 0 // index into expectedSequence
	lastSeen := -1    // last priority observed
	pollInterval := 50 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("reader interrupted", "last_seen", lastSeen)
			return nil
		case <-ticker.C:
			readStart := time.Now()
			got, err := sl.Read(ctx, correlationKey)
			latency := time.Since(readStart)
			if err != nil {
				if errors.Is(err, sluice.ErrRecordNotFound) {
					// Writer hasn't published yet. Keep polling.
					continue
				}
				log.Error("read error", "err", err)
				continue
			}
			var r NudgeInventoryPayload
			_ = json.Unmarshal(got, &r)

			// Log only on transition to reduce noise.
			if r.Priority != lastSeen {
				log.Info("◆◆◆ VALUE TRANSITION OBSERVED",
					"priority", r.Priority,
					"read_latency_us", latency.Microseconds(),
					"expected_next", nextExpected < len(expectedSequence) && r.Priority == expectedSequence[nextExpected])
				lastSeen = r.Priority

				// Advance the expected-sequence pointer on a match.
				if nextExpected < len(expectedSequence) && r.Priority == expectedSequence[nextExpected] {
					nextExpected++
				}
			}

			// All expected values observed → success.
			if nextExpected == len(expectedSequence) {
				log.Info("═══ READER SUCCESS: all values converged via broadcast ═══",
					"observed_sequence", expectedSequence)
				// Hold briefly to show continued L1 hits, then exit cleanly.
				select {
				case <-ctx.Done():
				case <-time.After(3 * time.Second):
				}
				return nil
			}
		}
	}
}

func splitAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ensureTableExists creates the table if it doesn't exist and waits for it to become ACTIVE.
func ensureTableExists(ctx context.Context, client *dynamodb.Client, tableName string, log *slog.Logger) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		var alreadyExists *types.ResourceInUseException
		switch {
		case errors.As(err, &alreadyExists):
			log.Info("table already exists, skipping creation", "table", tableName)
		case strings.Contains(err.Error(), "ResourceInUseException"):
			log.Info("table already exists (string match), skipping creation", "table", tableName)
		default:
			return fmt.Errorf("create table: %w", err)
		}
	} else {
		log.Info("table creation initiated", "table", tableName)
	}

	log.Info("waiting for table to become active...", "table", tableName)
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 2*time.Minute)

	if err != nil {
		return fmt.Errorf("wait for table active: %w", err)
	}

	log.Info("table is active", "table", tableName)
	return nil
}
