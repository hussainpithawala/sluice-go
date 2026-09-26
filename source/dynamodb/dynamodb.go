package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hussainpithawala/sluice-go/source"
)

// NewSource creates a new DynamoDB source.
func NewSource(client *dynamodb.Client, tableName string) *Source {
	return &Source{
		client:    client,
		tableName: tableName,
	}
}

// Read implements source.Source.
// It performs a strongly consistent read, satisfying the CP requirement for historical lookups.
func (s *Source) Read(ctx context.Context, model source.ReadModel) ([]byte, error) {
	// The ReadContract should provide the DynamoDB key (Partition Key + optional Sort Key)
	keyMap, ok := model.Filter.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("dynamodb source requires model.Filter to be map[string]any")
	}

	avKey, err := attributevalue.MarshalMap(keyMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key to AttributeValue: %w", err)
	}

	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.tableName),
		Key:            avKey,
		ConsistentRead: aws.Bool(true), // CRITICAL: Ensures CP guarantee for cold reads
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb GetItem failed: %w", err)
	}

	if output.Item == nil {
		return nil, source.ErrRecordNotFound
	}

	// Convert DynamoDB attribute values to a generic map, then to JSON bytes
	// This matches the expected []byte return type of source.Source
	var genericMap map[string]any
	err = attributevalue.UnmarshalMap(output.Item, &genericMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal DynamoDB item: %w", err)
	}

	jsonBytes, err := json.Marshal(genericMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal item to JSON: %w", err)
	}

	return jsonBytes, nil
}

// ReadBulk executes the Query defined in DynamoBulkReadModel and delegates
// the result shaping entirely to the operator's Projector function.
func (s *Source) ReadBulk(ctx context.Context, model source.BulkReadModel) ([]source.BulkReadResult, error) {
	dynModel, ok := model.Query.(DynamoBulkReadModel)
	if !ok {
		return nil, fmt.Errorf("dynamodb source requires Query to be dynamodb.DynamoBulkReadModel")
	}

	if dynModel.Input == nil {
		return nil, fmt.Errorf("dynamodb source requires a non-nil QueryInput")
	}

	if dynModel.Projector == nil {
		return nil, fmt.Errorf("dynamodb source requires a non-nil Projector function")
	}

	// Execute the Query.
	// Note: For cache pre-warming, a single Query page is typically sufficient.
	// If the operator expects massive result sets, they should handle pagination
	// in their ReadBulkContract by making multiple calls, or we can add pagination
	// logic here in Phase 3 if a concrete use-case demands it.
	output, err := s.client.Query(ctx, dynModel.Input)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: bulk read query execution failed: %w", err)
	}

	return dynModel.Projector(output.Items)
}

// Ping verifies connectivity to the DynamoDB table.
func (s *Source) Ping(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})
	return err
}

// Close is a no-op.
func (s *Source) Close(ctx context.Context) error {
	return nil
}
