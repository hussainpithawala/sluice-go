package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hussainpithawala/sluice-go/source"
)

// Source implements source.Source for AWS DynamoDB cold reads.
type Source struct {
	client    *dynamodb.Client
	tableName string
}

// DynamoBulkReadModel is the DynamoDB-specific execution plan for bulk reads.
// It leverages the native QueryInput to efficiently fetch items for a specific Partition Key.
type DynamoBulkReadModel struct {
	// Input is the native DynamoDB QueryInput.
	// Operators should use KeyConditionExpression to target a specific PK.
	Input *dynamodb.QueryInput

	// Projector takes the raw DynamoDB items and shapes them into BulkReadResults.
	// Operators typically use attributevalue.UnmarshalMap inside this function.
	Projector func(items []map[string]types.AttributeValue) ([]source.BulkReadResult, error)
}
