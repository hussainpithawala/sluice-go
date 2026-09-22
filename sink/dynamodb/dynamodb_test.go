package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDynamoDBSink(t *testing.T) (*Sink, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: "http://localhost:8000"}, nil
		})),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg)
	tableName := "test_sluice_sink_" + t.Name()

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		res, _ := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
		return res != nil && res.Table.TableStatus == types.TableStatusActive
	}, 10*time.Second, 500*time.Millisecond)

	s := NewSink(client, tableName)

	cleanup := func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	}

	return s, cleanup
}

func TestDynamoDBSink_BulkWrite_SuccessAndChunking(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSink(t)
	defer cleanup()

	// Create 30 models with UNIQUE keys to satisfy DynamoDB BatchWriteItem constraints
	var models []sink.WriteModel
	for i := 0; i < 30; i++ {
		models = append(models, sink.WriteModel{
			CorrelationKey: fmt.Sprintf("key_%d", i),
			Update: map[string]any{
				"PK":    fmt.Sprintf("chunk_test_key_%d", i),
				"Index": i,
			},
			Upsert: true,
		})
	}

	res, err := s.BulkWrite(context.Background(), models)
	require.NoError(t, err)
	assert.NotNil(t, res)

	// DynamoDB PutItem doesn't return "Matched" counts like MongoDB,
	// but we track successful submissions via UpsertedCount in our adapter
	assert.Equal(t, int64(30), res.UpsertedCount)
	assert.Empty(t, res.Errors)
}

func TestDynamoDBSink_Write_DegradedMode(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSink(t)
	defer cleanup()

	model := sink.WriteModel{
		CorrelationKey: "key1",
		Update: map[string]any{
			"PK":     "degraded_key",
			"Status": "fallback",
		},
		Upsert: true,
	}

	err := s.Write(context.Background(), model)
	require.NoError(t, err)

	// Verify via direct GetItem
	out, err := s.client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "degraded_key"},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, out.Item)

	var result map[string]any
	_ = attributevalue.UnmarshalMap(out.Item, &result)
	assert.Equal(t, "fallback", result["Status"])
}

func TestDynamoDBSink_Ping(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSink(t)
	defer cleanup()

	err := s.Ping(context.Background())
	require.NoError(t, err)
}
