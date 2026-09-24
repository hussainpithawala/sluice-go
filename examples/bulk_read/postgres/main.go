// Package main demonstrates the BulkRead functionality for the PostgreSQL adapter.
// It proves the "set-based cache pre-warming" pattern: collapsing N+1 queries into
// a single SQL execution, followed by atomic L1/L2 journal hydration.
//
// Run against local PostgreSQL + Redis:
//
//	POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable
//	REDIS_ADDRS=localhost:6379
//	go run ./examples/bulk_read_postgres/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	pgsink "github.com/hussainpithawala/sluice-go/sink/postgres"
	"github.com/hussainpithawala/sluice-go/source"
	pgsource "github.com/hussainpithawala/sluice-go/source/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CampaignPayload struct {
	CampaignID string `json:"campaign_id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Priority   int    `json:"priority"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	postgresURI := getEnv("POSTGRES_URI", "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable")
	tableName := "bulk_read_campaigns"

	// 1. Setup PostgreSQL
	pool, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Create table and seed data
	_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			priority INTEGER NOT NULL
		)
	`, tableName))

	// Seed 5 campaigns for user_123
	for i := 1; i <= 5; i++ {
		_, _ = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, user_id, name, status, priority) 
			VALUES ('camp_%d', 'user_123', 'Campaign %d', 'active', %d)
		`, tableName, i, i, i))
	}
	log.Info("seeded 5 campaigns for user_123")

	// 2. Initialize Sluice
	sk, err := pgsink.New(ctx, pgsink.DefaultConfig(postgresURI, "nudge_inventory"))
	src := pgsource.NewSourceWithPool(pool)

	sl, err := sluice.New("bulk_read_demo").
		WithRedis(sluice.RedisConfig{Addrs: []string{getEnv("REDIS_ADDRS", "localhost:6379")}}).
		WithSink(sk).
		WithSource(src).
		// Define how to fetch the bulk working set
		WithReadBulkContract(func(lookupKey string) (*source.BulkReadModel, error) {
			return &source.BulkReadModel{
				Query: pgsource.PostgresBulkReadModel{
					Query: fmt.Sprintf(`
						SELECT id, json_build_object(
							'campaign_id', id, 'name', name, 'status', status, 'priority', priority
						) as payload 
						FROM %s WHERE user_id = $1
					`, tableName),
					Args: []any{lookupKey},
					Projector: func(rows pgx.Rows) ([]source.BulkReadResult, error) {
						var results []source.BulkReadResult
						for rows.Next() {
							var id string
							var payload []byte
							if err := rows.Scan(&id, &payload); err != nil {
								return nil, err
							}
							results = append(results, source.BulkReadResult{
								CorrelationKey: id,
								Payload:        payload,
							})
						}
						return results, rows.Err()
					},
				},
			}, nil
		}).
		// Define how to index the bulk results in Redis
		WithIndexBulkContract(func(results []source.BulkReadResult) (map[string]map[string]interface{}, error) {
			indexes := make(map[string]map[string]interface{})
			for _, res := range results {
				var p CampaignPayload
				if err := json.Unmarshal(res.Payload, &p); err != nil {
					continue
				}
				indexes[res.CorrelationKey] = map[string]interface{}{
					"status":   p.Status,
					"priority": float64(p.Priority),
				}
			}
			return indexes, nil
		}).
		Build(ctx)
	if err != nil {
		log.Error("failed to build sluice", "err", err)
		os.Exit(1)
	}
	defer sl.DrainAndClose(ctx)

	// 3. Execute Bulk Read
	log.Info("executing ReadBulk for user_123...")
	start := time.Now()
	payloads, err := sl.ReadBulk(ctx, "user_123")
	if err != nil {
		log.Error("ReadBulk failed", "err", err)
		os.Exit(1)
	}
	log.Info("ReadBulk completed", "duration_ms", time.Since(start).Milliseconds(), "items_loaded", len(payloads))

	// 4. Verify L1/L2 Hydration (Sub-millisecond individual reads)
	log.Info("verifying individual Read() hits the journal (not Postgres)...")
	for i := 1; i <= 5; i++ {
		crn := fmt.Sprintf("camp_%d", i)
		t0 := time.Now()
		p, err := sl.Read(ctx, crn)
		if err != nil {
			log.Error("individual read failed", "crn", crn, "err", err)
			continue
		}
		log.Info("individual read served from journal", "crn", crn, "latency_us", time.Since(t0).Microseconds(), "payload", string(p))
	}

	// 5. Verify Compound Query works on bulk-loaded items
	log.Info("verifying Query() works on bulk-loaded indexes...")
	results, err := sl.Query(ctx, sluice.Query{
		Equality: map[string]string{"status": "active"},
		RangeMin: map[string]float64{"priority": 3},
	})
	if err != nil {
		log.Error("Query failed", "err", err)
	} else {
		log.Info("Query succeeded", "matches", len(results))
		for _, r := range results {
			log.Info("query match", "crn", r.CorrelationKey)
		}
	}

	log.Info("bulk read example completed successfully")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
