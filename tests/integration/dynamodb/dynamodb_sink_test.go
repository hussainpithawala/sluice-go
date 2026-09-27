package dynamodb

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hussainpithawala/sluice-go/sink"
	sinkdynamodb "github.com/hussainpithawala/sluice-go/sink/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestDynamoDBBulkWrite_SilentLossPrevention(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up the OFFICIAL AWS DynamoDB Local container (Free, no auth required)
	req := testcontainers.ContainerRequest{
		Image:        "amazon/dynamodb-local:latest",
		ExposedPorts: []string{"8000/tcp"},
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"},
		WaitingFor:   wait.ForListeningPort("8000/tcp").WithStartupTimeout(2 * time.Minute),
	}
	dynamoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer func(dynamoC testcontainers.Container, ctx context.Context, opts ...testcontainers.TerminateOption) {
		err := dynamoC.Terminate(ctx, opts...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while terminating the DynContainer %e", err))
		}
	}(dynamoC, ctx)

	host, _ := dynamoC.Host(ctx)
	port, _ := dynamoC.MappedPort(ctx, "8000")
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	// 2. Configure AWS SDK v2 for Local
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", "test"),
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// 3. Create the table via real AWS SDK
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("test_table"),
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
	require.NoError(t, err)

	// Wait for table to be active
	for {
		out, _ := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("test_table")})
		if out.Table != nil && out.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 4. Initialize the sluice sink
	s, err := sinkdynamodb.New(ctx, sinkdynamodb.Config{
		Endpoint:    endpoint,
		Region:      "us-east-1",
		TableName:   "test_table",
		PKAttribute: "PK",
		SKAttribute: "SK",
	})
	require.NoError(t, err)

	t.Run("Pre-validation: Type assertion failure is attributed, rest succeed", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "user_good",
				Update:         map[string]any{"PK": "user_good", "SK": "meta", "val": 1},
			},
			{
				CorrelationKey: "user_bad_type",
				Update:         "not_a_map", // Intentional type assertion failure
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err) // No top-level error
		require.Len(t, res.Errors, 1)

		assert.Equal(t, "user_bad_type", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)

		// Verify the good item was actually written to the real local DB
		getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("test_table"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "user_good"},
				"SK": &types.AttributeValueMemberS{Value: "meta"},
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, getOut.Item)
	})

	t.Run("Pre-validation: Duplicate key in batch is caught and attributed", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "user_dup_1",
				Update:         map[string]any{"PK": "dup_key", "SK": "meta", "val": 1},
			},
			{
				CorrelationKey: "user_dup_2",
				Update:         map[string]any{"PK": "dup_key", "SK": "meta", "val": 2}, // Duplicate PK/SK
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)

		assert.Equal(t, "user_dup_2", res.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)
	})

	t.Run("Total Failure: ResourceNotFoundException prevents silent commit", func(t *testing.T) {
		// Delete the table to simulate ResourceNotFoundException on BulkWrite
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String("test_table"),
		})
		require.NoError(t, err)

		// Wait for table to be deleted
		for {
			_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("test_table")})
			if err != nil {
				break // Table is gone
			}
			time.Sleep(200 * time.Millisecond)
		}

		models := []sink.WriteModel{
			{CorrelationKey: "user_fail", Update: map[string]any{"PK": "user_fail", "SK": "meta"}},
		}

		_, err = s.BulkWrite(ctx, models)
		require.Error(t, err) // Top-level error returned
		assert.Contains(t, err.Error(), "unmapped bulkwrite error")

		// Recreate the table for any subsequent tests (though this is the last one)
		_, _ = client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("test_table"),
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
	})
}
