package dynamodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Config holds connection and table parameters for DynamoDB.
type Config struct {
	Endpoint    string // Optional: for LocalStack/DynamoDB Local testing
	Region      string
	TableName   string
	PKAttribute string                  // Partition key attribute name
	SKAttribute string                  // Sort key attribute name (optional)
	Credentials aws.CredentialsProvider // Optional: defaults to dummy "test" creds if not provided
}

// Sink implements sink.FlushSink against AWS DynamoDB.
type Sink struct {
	api       *dynamodb.Client
	tableName string
	pkAttr    string
	skAttr    string
}

// New creates a new DynamoDB sink.
func New(ctx context.Context, cfg Config) (*Sink, error) {
	if cfg.TableName == "" {
		return nil, errors.New("sluice/dynamodb: table name is required")
	}
	if cfg.PKAttribute == "" {
		return nil, errors.New("sluice/dynamodb: PK attribute is required")
	}

	// DynamoDB Local requires signed requests, but ignores the actual credential values.
	// We provide dummy credentials if none are supplied to satisfy the SDK signer.
	creds := cfg.Credentials
	if creds == nil {
		creds = credentials.NewStaticCredentialsProvider("test", "test", "test")
	}

	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: creds,
	}

	opts := []func(*dynamodb.Options){}
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.Region != "" {
		opts = append(opts, func(o *dynamodb.Options) {
			o.Region = cfg.Region
		})
	}

	client := dynamodb.NewFromConfig(awsCfg, opts...)

	// Verify table exists and fetch schema if SK is not provided
	if cfg.SKAttribute == "" {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(cfg.TableName),
		})
		if err != nil {
			return nil, fmt.Errorf("sluice/dynamodb: describe table: %w", err)
		}
		if out.Table == nil || len(out.Table.KeySchema) == 0 {
			return nil, errors.New("sluice/dynamodb: table has no key schema")
		}
		for _, ks := range out.Table.KeySchema {
			if ks.KeyType == types.KeyTypeRange {
				cfg.SKAttribute = aws.ToString(ks.AttributeName)
			}
		}
	}

	return &Sink{
		api:       client,
		tableName: cfg.TableName,
		pkAttr:    cfg.PKAttribute,
		skAttr:    cfg.SKAttribute,
	}, nil
}

// BulkWrite executes all models as a single BatchWriteItem call.
// It handles partial success, unprocessed items, and validation exceptions.
func (s *Sink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	if len(models) == 0 {
		return &sink.BulkWriteResult{}, nil
	}

	// 1. Pre-validate and marshal. Detect duplicates to prevent ValidationException loops.
	seenKeys := make(map[string]bool)
	requestItems := make([]types.WriteRequest, 0, len(models))
	var preErrors []sink.SinkError
	var validModels []sink.WriteModel

	for _, m := range models {
		// RFP Fix: Catch type assertion failures per-item, do not return early.
		item, ok := m.Update.(map[string]any)
		if !ok {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            errors.New("Update field must be map[string]any"),
			})
			continue
		}

		pkVal, ok := item[s.pkAttr]
		if !ok {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            fmt.Errorf("missing partition key attribute %q", s.pkAttr),
			})
			continue
		}

		keyStr := fmt.Sprintf("%v", pkVal)
		if s.skAttr != "" {
			skVal, ok := item[s.skAttr]
			if !ok {
				preErrors = append(preErrors, sink.SinkError{
					CorrelationKey: m.CorrelationKey,
					Class:          sink.ClassPermanent,
					Err:            fmt.Errorf("missing sort key attribute %q", s.skAttr),
				})
				continue
			}
			keyStr += "#" + fmt.Sprintf("%v", skVal)
		}

		// RFP Fix: Catch duplicates in the same batch to prevent unattributable ValidationExceptions.
		if seenKeys[keyStr] {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            errors.New("duplicate key in batch"),
			})
			continue
		}
		seenKeys[keyStr] = true

		avItem, err := attributevalue.MarshalMap(item)
		if err != nil {
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Class:          sink.ClassPermanent,
				Err:            fmt.Errorf("marshal item: %w", err),
			})
			continue
		}

		requestItems = append(requestItems, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: avItem},
		})
		validModels = append(validModels, m)
	}

	if len(requestItems) == 0 {
		return &sink.BulkWriteResult{Errors: preErrors}, nil
	}

	// 2. Execute BatchWriteItem
	out, err := s.api.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{s.tableName: requestItems},
	})

	if err != nil {
		// RFP Fix: On whole-call ValidationException, fall back to per-item PutItem.
		// Use smithy.APIError to correctly identify the exception in AWS SDK Go v2.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ValidationException" {
			return s.fallbackPutItems(ctx, validModels, preErrors)
		}

		// RFP Fix: Context cancel or retries exhausted. Treat all valid items as transient.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			for _, m := range validModels {
				preErrors = append(preErrors, sink.SinkError{
					CorrelationKey: m.CorrelationKey,
					Class:          sink.ClassTransient,
					Err:            err,
				})
			}
			return &sink.BulkWriteResult{Errors: preErrors}, nil
		}

		// RFP Fix: Unmapped errors return top-level error to prevent silent loss.
		return nil, fmt.Errorf("sluice/dynamodb: unmapped bulkwrite error: %w", err)
	}

	// 3. Handle UnprocessedItems
	// 3. Handle UnprocessedItems
	successCount := int64(len(requestItems))
	if len(out.UnprocessedItems[s.tableName]) > 0 {
		successCount -= int64(len(out.UnprocessedItems[s.tableName]))
		for _, wr := range out.UnprocessedItems[s.tableName] {
			corrKey := findCorrelationKey(validModels, wr.PutRequest.Item, s.pkAttr, s.skAttr)
			if corrKey == "" {
				// RFP Fix: If an item can't be mapped, return top-level error.
				// The engine will abort the commit, keeping all keys dirty.
				return nil, errors.New("sluice/dynamodb: unprocessed item could not be mapped to correlation key")
			}
			preErrors = append(preErrors, sink.SinkError{
				CorrelationKey: corrKey,
				Class:          sink.ClassTransient,
				Err:            errors.New("unprocessed item"),
			})
		}
	}

	return &sink.BulkWriteResult{
		UpsertedCount: successCount, // Track successful submissions
		Errors:        preErrors,
	}, nil
}

// fallbackPutItems executes per-item PutItem calls when BatchWriteItem fails with ValidationException.
// fallbackPutItems executes per-item PutItem calls when BatchWriteItem fails with ValidationException.
func (s *Sink) fallbackPutItems(ctx context.Context, models []sink.WriteModel, existingErrors []sink.SinkError) (*sink.BulkWriteResult, error) {
	var errs []sink.SinkError
	errs = append(errs, existingErrors...)
	var successCount int64

	for _, m := range models {
		item, _ := m.Update.(map[string]any)
		avItem, err := attributevalue.MarshalMap(item)
		if err != nil {
			errs = append(errs, sink.SinkError{CorrelationKey: m.CorrelationKey, Class: sink.ClassPermanent, Err: err})
			continue
		}

		_, err = s.api.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.tableName), Item: avItem})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				errs = append(errs, sink.SinkError{CorrelationKey: m.CorrelationKey, Class: sink.ClassTransient, Err: err})
			} else {
				errs = append(errs, sink.SinkError{CorrelationKey: m.CorrelationKey, Class: sink.ClassPermanent, Err: err})
			}
		} else {
			successCount++ // Track successful individual puts
		}
	}
	return &sink.BulkWriteResult{
		UpsertedCount: successCount,
		Errors:        errs,
	}, nil
}

// Write performs a single-document put. Used in degraded mode only.
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
	item, ok := model.Update.(map[string]any)
	if !ok {
		return errors.New("Update field must be map[string]any")
	}
	avItem, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}

	_, err = s.api.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.tableName), Item: avItem})
	return err
}

func (s *Sink) Ping(ctx context.Context) error {
	_, err := s.api.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.tableName)})
	return err
}

func (s *Sink) Close(ctx context.Context) error { return nil }

// findCorrelationKey matches a DynamoDB AttributeValue map back to the original CorrelationKey.
func findCorrelationKey(models []sink.WriteModel, avItem map[string]types.AttributeValue, pkAttr, skAttr string) string {
	for _, m := range models {
		item, _ := m.Update.(map[string]any)

		// Simple string comparison for PK
		pkAv, ok1 := avItem[pkAttr].(*types.AttributeValueMemberS)
		pkStr := fmt.Sprintf("%v", item[pkAttr])
		if !ok1 || pkAv.Value != pkStr {
			// Fallback for Number types if needed, but string is most common for correlation
			if numAv, okNum := avItem[pkAttr].(*types.AttributeValueMemberN); okNum && numAv.Value == pkStr {
				// match
			} else {
				continue
			}
		}

		if skAttr != "" {
			skAv, ok2 := avItem[skAttr].(*types.AttributeValueMemberS)
			skStr := fmt.Sprintf("%v", item[skAttr])
			if !ok2 || skAv.Value != skStr {
				if numAv, okNum := avItem[skAttr].(*types.AttributeValueMemberN); okNum && numAv.Value == skStr {
					// match
				} else {
					continue
				}
			}
		}
		return m.CorrelationKey
	}
	return ""
}

// Client returns the underlying dynamodb.Client, allowing it to be shared
// with tests or other components that need direct access.
func (s *Sink) Client() *dynamodb.Client {
	return s.api
}
