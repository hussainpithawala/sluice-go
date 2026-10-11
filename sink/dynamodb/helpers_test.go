package dynamodb

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// testDynamoDBSinkMock constructs a Sink directly to bypass New() and the
// DescribeTable network call. Use this for tests that only verify pre-validation
// rejections (like Many: true).
func testDynamoDBSinkMock(t *testing.T) *Sink {
	t.Helper()
	// Adjust these field names to match your actual Sink struct in dynamodb.go
	return &Sink{
		table:  "sluice-test-update-many",
		pkAttr: "pk",
		// client is intentionally nil
	}
}

// testDynamoDBSink sets up a real local DynamoDB table and returns a fully
// initialized Sink. Use this for tests that actually execute writes (Many: false).
func testDynamoDBSink(t *testing.T) *Sink {
	t.Helper()
	ctx := context.Background()
	endpoint := "http://localhost:8000" // Adjust if using testcontainers
	tableName := "sluice-test-update-many"

	// 1. Load AWS config and create a raw client to provision the table
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	require.NoError(t, err)

	rawClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	_, err = rawClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	// Ignore "table already exists" errors on subsequent runs
	if err != nil {
		var riue *types.ResourceInUseException
		if !errors.As(err, &riue) {
			t.Fatalf("failed to create DynamoDB table: %v", err)
		}
	}

	// 2. Initialize the actual Sink (this will now succeed at DescribeTable)
	s, err := New(ctx, Config{
		Endpoint:    endpoint,
		Region:      "us-east-1",
		TableName:   tableName,
		PKAttribute: "pk",
	})
	require.NoError(t, err)
	return s
}
