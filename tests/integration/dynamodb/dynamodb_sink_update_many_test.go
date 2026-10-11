// tests/integration/dynamodb/dynamodb_sink_update_many_test.go
package dynamodb

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hussainpithawala/sluice-go/sink"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestDynamoDBSink_UpdateMany(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "amazon/dynamodb-local:latest",
		ExposedPorts: []string{"8000/tcp"},
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"},
		WaitingFor:   wait.ForLog("Initializing DynamoDB Local"),
	}
	dynamoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer func(dynamoC testcontainers.Container, ctx context.Context, opts ...testcontainers.TerminateOption) {
		err := dynamoC.Terminate(ctx, opts...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while terminating the DynamoDBContainer %e", err))
		}
	}(dynamoC, ctx)

	host, _ := dynamoC.Host(ctx)
	port, _ := dynamoC.MappedPort(ctx, "8000")
	endpoint := "http://" + host + ":" + port.Port()

	// Create the test table directly via AWS SDK
	createTestTable(t, ctx, endpoint)

	s, err := dynsink.New(ctx, dynsink.Config{
		Endpoint:    endpoint,
		Region:      "us-east-1",
		TableName:   "test-table",
		PKAttribute: "pk",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	require.NoError(t, err)
	defer func(s *dynsink.Sink, ctx context.Context) {
		err := s.Close(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while closing the sink %e", err))
		}
	}(s, ctx)

	// Create a raw client for verification queries
	verifyClient := dynamodb.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})

	t.Run("Many:true returns permanent SinkError and dead-letters immediately", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "many-rejected-1",
				Filter:         map[string]any{"pk": "many-rejected-1"},
				Update:         map[string]any{"pk": "many-rejected-1", "status": "done"},
				Upsert:         true,
				Many:           true,
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Errors, 1)

		assert.Equal(t, "many-rejected-1", result.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
		assert.True(t, result.Errors[0].IsPermanent())
		assert.Contains(t, result.Errors[0].Err.Error(), "UpdateMany not supported")
	})

	t.Run("Many:true does not write any item to DynamoDB", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "ghost-item",
				Filter:         map[string]any{"pk": "ghost-item"},
				Update:         map[string]any{"pk": "ghost-item", "data": "should-not-exist"},
				Upsert:         true,
				Many:           true,
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, result.Errors, 1)

		// Verify the item was NOT written to DynamoDB
		getOut, err := verifyClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("test-table"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "ghost-item"},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, getOut.Item, "rejected Many write must not create an item in DynamoDB")
	})

	t.Run("Mixed batch: Many:true rejected, Many:false proceeds normally", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "rejected-many",
				Filter:         map[string]any{"pk": "rejected-many"},
				Update:         map[string]any{"pk": "rejected-many", "x": 1},
				Upsert:         true,
				Many:           true, // rejected
			},
			{
				CorrelationKey: "accepted-single",
				Filter:         map[string]any{"pk": "accepted-single"},
				Update:         map[string]any{"pk": "accepted-single", "x": 2},
				Upsert:         true,
				Many:           false, // accepted
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Only the Many model should error
		require.Len(t, result.Errors, 1)
		assert.Equal(t, "rejected-many", result.Errors[0].CorrelationKey)
		assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)

		// The valid model should have been written
		getOut, err := verifyClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("test-table"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "accepted-single"},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, getOut.Item, "Many:false model should be written successfully")
	})

	t.Run("Degraded Write with Many:true returns error", func(t *testing.T) {
		model := sink.WriteModel{
			CorrelationKey: "degraded-many",
			Filter:         map[string]any{"pk": "degraded-many"},
			Update:         map[string]any{"pk": "degraded-many", "val": "x"},
			Upsert:         true,
			Many:           true,
		}

		err := s.Write(ctx, model)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UpdateMany not supported")

		// Verify nothing was written
		getOut, err := verifyClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("test-table"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "degraded-many"},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, getOut.Item)
	})

	t.Run("Many:false default behavior preserved (no regression)", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "regression-check",
				Filter:         map[string]any{"pk": "regression-check"},
				Update: map[string]any{
					"pk":    "regression-check",
					"name":  "test",
					"value": 42,
				},
				Upsert: true,
				Many:   false, // explicit default
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		assert.Empty(t, result.Errors)

		// Verify item exists in DynamoDB
		getOut, err := verifyClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("test-table"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "regression-check"},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, getOut.Item)
	})

	t.Run("All Many batch: every model rejected, no API write attempted", func(t *testing.T) {
		models := []sink.WriteModel{
			{CorrelationKey: "all-many-1", Filter: map[string]any{"pk": "all-many-1"}, Update: map[string]any{"pk": "all-many-1"}, Many: true},
			{CorrelationKey: "all-many-2", Filter: map[string]any{"pk": "all-many-2"}, Update: map[string]any{"pk": "all-many-2"}, Many: true},
			{CorrelationKey: "all-many-3", Filter: map[string]any{"pk": "all-many-3"}, Update: map[string]any{"pk": "all-many-3"}, Many: true},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Errors, 3)

		for i, e := range result.Errors {
			assert.Equal(t, models[i].CorrelationKey, e.CorrelationKey)
			assert.Equal(t, sink.ClassPermanent, e.Class)
			assert.Contains(t, e.Err.Error(), "UpdateMany not supported")
		}
	})

	t.Run("Attribution: CheckAttribution passes for rejected Many writes", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "attr-many",
				Filter:         map[string]any{"pk": "attr-many"},
				Update:         map[string]any{"pk": "attr-many", "data": "x"},
				Many:           true,
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)

		err = sink.CheckAttribution(models, res)
		assert.NoError(t, err, "CheckAttribution should pass; rejected Many writes are attributed, not silently lost")
	})
}

// createTestTable provisions the DynamoDB table used by integration tests.
func createTestTable(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()

	client := dynamodb.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("test-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}
