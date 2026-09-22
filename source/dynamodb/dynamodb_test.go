package dynamodb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hussainpithawala/sluice-go/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDynamoDBSource(t *testing.T) (*Source, func()) {
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
	tableName := "test_sluice_source_" + t.Name()

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

	s := NewSource(client, tableName)

	cleanup := func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	}

	return s, cleanup
}

func TestDynamoDBSource_Read_Success(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSource(t)
	defer cleanup()

	// Seed data
	item := map[string]any{
		"PK":     "user_123",
		"Name":   "Alice",
		"Active": true,
	}
	avMap, _ := attributevalue.MarshalMap(item)
	_, err := s.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      avMap,
	})
	require.NoError(t, err)

	model := source.ReadModel{Filter: map[string]any{"PK": "user_123"}}
	data, err := s.Read(context.Background(), model)

	require.NoError(t, err)
	assert.NotNil(t, data)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result["Name"])
	assert.Equal(t, true, result["Active"])
}

func TestDynamoDBSource_Read_NotFound(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSource(t)
	defer cleanup()

	model := source.ReadModel{Filter: map[string]any{"PK": "non_existent_user"}}
	data, err := s.Read(context.Background(), model)

	assert.Nil(t, data)
	assert.ErrorIs(t, err, source.ErrRecordNotFound)
}

func TestDynamoDBSource_Ping(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDynamoDBSource(t)
	defer cleanup()

	err := s.Ping(context.Background())
	require.NoError(t, err)
}
