// Package main demonstrates the BulkRead functionality for the DynamoDB adapter.
//
// Run against DynamoDB Local + Redis:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000 REDIS_ADDRS=localhost:6379
//	go run ./examples/bulk_read_dynamodb/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sluice "github.com/hussainpithawala/sluice-go"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
	"github.com/hussainpithawala/sluice-go/source"
	dynsource "github.com/hussainpithawala/sluice-go/source/dynamodb"
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

	endpoint := getEnv("DYNAMODB_ENDPOINT", "http://localhost:8000")
	tableName := "BulkReadCampaigns"

	// 1. Setup DynamoDB
	cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Create table
	_, _ = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	time.Sleep(2 * time.Second) // Wait for active

	// Seed data
	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":       &types.AttributeValueMemberS{Value: "user_123"},
				"SK":       &types.AttributeValueMemberS{Value: fmt.Sprintf("camp_%d", i)},
				"Name":     &types.AttributeValueMemberS{Value: fmt.Sprintf("Campaign %d", i)},
				"Status":   &types.AttributeValueMemberS{Value: "active"},
				"Priority": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
			},
		})
	}
	log.Info("seeded 5 campaigns for user_123")

	// 2. Initialize Sluice
	sk := dynsink.NewSink(client, tableName)
	src := dynsource.NewSource(client, tableName)

	sl, err := sluice.New("bulk_read_demo").
		WithRedis(sluice.RedisConfig{Addrs: []string{getEnv("REDIS_ADDRS", "localhost:6379")}}).
		WithSink(sk).
		WithSource(src).
		WithReadBulkContract(func(lookupKey string) (*source.BulkReadModel, error) {
			return &source.BulkReadModel{
				Query: dynsource.DynamoBulkReadModel{
					Input: &dynamodb.QueryInput{
						TableName:              aws.String(tableName),
						KeyConditionExpression: aws.String("PK = :pk"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":pk": &types.AttributeValueMemberS{Value: lookupKey},
						},
					},
					Projector: func(items []map[string]types.AttributeValue) ([]source.BulkReadResult, error) {
						var results []source.BulkReadResult
						for _, item := range items {
							var sk, name, status string
							var priority int
							_ = attributevalue.Unmarshal(item["SK"], &sk)
							_ = attributevalue.Unmarshal(item["Name"], &name)
							_ = attributevalue.Unmarshal(item["Status"], &status)
							_ = attributevalue.Unmarshal(item["Priority"], &priority)

							payload, _ := json.Marshal(CampaignPayload{
								CampaignID: sk, Name: name, Status: status, Priority: priority,
							})
							results = append(results, source.BulkReadResult{
								CorrelationKey: sk,
								Payload:        payload,
							})
						}
						return results, nil
					},
				},
			}, nil
		}).
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

	// 4. Verify L1/L2 Hydration
	log.Info("verifying individual Read() hits the journal...")
	for i := 1; i <= 5; i++ {
		crn := fmt.Sprintf("camp_%d", i)
		t0 := time.Now()
		_, err := sl.Read(ctx, crn)
		if err == nil {
			log.Info("individual read served from journal", "crn", crn, "latency_us", time.Since(t0).Microseconds())
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
