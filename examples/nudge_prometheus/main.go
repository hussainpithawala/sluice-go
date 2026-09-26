// Package main demonstrates how to wire sluice metrics to Prometheus
// for Grafana visualization. It runs a simple nudge inventory consumer
// and exposes a /metrics endpoint on port 2112 for Prometheus scraping.
// Port 9090 is left free for the Prometheus server in docker-compose.yml.
//
// Run:
//
//	MONGO_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379
//	go run ./examples/nudge_prometheus/main.go
//
// Then in another terminal, scrape the metrics:
//
//	curl http://localhost:2112/metrics
//
// Or configure Prometheus to scrape:
//
//	scrape_configs:
//	  - job_name: 'sluice'
//	    static_configs:
//	      - targets: ['localhost:2112']
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/bson"

	sluiceprom "github.com/hussainpithawala/sluice-go/metrics/prometheus"
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id" bson:"nudge_master_id"`
	Channel       string    `json:"channel" bson:"channel"`
	Priority      int       `json:"priority" bson:"priority"`
	CampaignID    string    `json:"campaign_id" bson:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at" bson:"expires_at"`
	LastUpdated   time.Time `json:"last_updated" bson:"last_updated"`
}

func nudgeWriteContract(crn string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for CRN %s: %w", crn, err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: crn}},
		Update: bson.D{{Key: "$set", Value: bson.D{
			{Key: "nudge_master_id", Value: p.NudgeMasterID},
			{Key: "channel", Value: p.Channel},
			{Key: "priority", Value: p.Priority},
			{Key: "campaign_id", Value: p.CampaignID},
			{Key: "expires_at", Value: p.ExpiresAt},
			{Key: "last_updated", Value: p.LastUpdated},
		}}},
		Upsert: true,
	}, nil
}

func nudgeReadContract(crn string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: bson.D{{Key: "_id", Value: crn}},
	}, nil
}

func nudgeIndexContract(crn string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: invalid payload for CRN %s: %w", crn, err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,
		"campaign": p.CampaignID,
		"priority": float64(p.Priority),
		"expires":  float64(p.ExpiresAt.UnixMilli()),
	}, nil
}

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

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	redisAddrs := getEnv("REDIS_ADDRS", "localhost:6379")

	// ── Initialize Prometheus metrics recorder ────────────────────────────
	// This is the key addition: instead of a noop or custom logger,
	// we use the Prometheus-backed recorder.
	metricsRec := sluiceprom.NewRecorder("nudge_inventory")

	// ── Initialize DocumentDB sink and source ─────────────────────────────
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI, Database: "adroll", Collection: "nudge_inventory",
		MaxPoolSize: 100, MinPoolSize: 10,
	})
	if err != nil {
		return fmt.Errorf("connect to MongoDB: %w", err)
	}
	src := sourcedocdb.NewSourceWithClient(sk.Client(), "adroll", "nudge_inventory")

	// ── Build Sluice with Prometheus metrics ──────────────────────────────
	sl, err := sluice.New("nudge_inventory").
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddrs}}).
		WithSink(sk).
		WithSource(src).
		WithWriteContract(nudgeWriteContract).
		WithReadContract(nudgeReadContract).
		WithIndexContract(nudgeIndexContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(16).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour).
		WithHotAwareFlush(true).
		WithContentDedup(true).
		WithMetrics(metricsRec). // ← Prometheus metrics!
		Build(ctx)
	if err != nil {
		return fmt.Errorf("build sluice: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	}()

	// ── Expose Prometheus metrics via HTTP ────────────────────────────────
	// This is the standard Prometheus HTTP handler.
	// Prometheus will scrape this endpoint to collect metrics.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// Optional: add a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    ":2112",
		Handler: mux,
	}

	// Bind synchronously so a port conflict fails startup instead of
	// silently running the workload with no metrics endpoint.
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.Addr, err)
	}

	// Serve in a goroutine
	go func() {
		log.Info("starting Prometheus metrics server", "addr", server.Addr)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", "err", err)
		}
	}()

	// ── Start simulated write workload ────────────────────────────────────
	var wg sync.WaitGroup
	var written atomic.Int64

	// Cold write simulator
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				crn := fmt.Sprintf("crn_%09d", written.Add(1))
				payload, _ := json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: "nm_spring_retarget",
					Channel:       "push",
					Priority:      3,
					CampaignID:    "camp_001",
					ExpiresAt:     time.Now().Add(24 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				if err := sl.Write(ctx, crn, payload); err != nil {
					log.Error("write error", "crn", crn, "err", err)
				}
			}
		}
	}()

	// Hot read simulator (every 2 seconds, simulate a user login)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		sessionCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sessionCount++
				crn := fmt.Sprintf("crn_hot_%06d", sessionCount)

				// HotLoad (fire-and-forget)
				if err := sl.HotLoad(ctx, crn); err != nil {
					log.Debug("hotload failed (expected for new CRN)", "crn", crn)
					// Seed the CRN
					payload, _ := json.Marshal(NudgeInventoryPayload{
						NudgeMasterID: "nm_welcome_bonus",
						Channel:       "push",
						Priority:      5,
						CampaignID:    "camp_onboarding",
						ExpiresAt:     time.Now().Add(48 * time.Hour),
						LastUpdated:   time.Now().UTC(),
					})
					_ = sl.Write(ctx, crn, payload)
				}

				// Read the CRN (should be hot)
				_, err := sl.Read(ctx, crn)
				if err != nil {
					log.Error("read error", "crn", crn, "err", err)
				}

				// Query for compound lookups
				_, _ = sl.Query(ctx, sluice.Query{
					Equality: map[string]string{"channel": "push"},
					RangeMin: map[string]float64{"priority": 3},
				})
			}
		}
	}()

	// ── Stats reporter ────────────────────────────────────────────────────
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	start := time.Now()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				total := written.Load()
				elapsed := time.Since(start).Seconds()
				log.Info("throughput",
					"writes", total,
					"elapsed_s", fmt.Sprintf("%.1f", elapsed),
					"rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed),
				)
			}
		}
	}()

	log.Info("sluice with Prometheus metrics is running",
		"metrics_endpoint", "http://localhost:2112/metrics",
	)

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()

	// Gracefully shutdown the HTTP server
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutCtx)

	log.Info("final summary",
		"total_writes", written.Load(),
		"elapsed", time.Since(start).Round(time.Second),
	)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
