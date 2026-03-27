//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

type sqsEvent struct {
	CRN           string `json:"crn"`
	NudgeMasterID string `json:"nudge_master_id"`
}

func newLocalStackSQS(t *testing.T) *sqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: sqsEndpoint(), HostnameImmutable: true}, nil
			},
		)),
	)
	require.NoError(t, err)
	return sqs.NewFromConfig(cfg)
}

func createSQSQueue(t *testing.T, client *sqs.Client, name string) string {
	t.Helper()
	out, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)
	return *out.QueueUrl
}

func publishSQSMessages(t *testing.T, client *sqs.Client, queueURL string, n int, nudgeMasterID string) {
	t.Helper()
	ctx := context.Background()
	for start := 0; start < n; start += 10 {
		end := start + 10
		if end > n { end = n }
		entries := make([]types.SendMessageBatchRequestEntry, 0, end-start)
		for i := start; i < end; i++ {
			crn := fmt.Sprintf("crn_sqs_%07d", i)
			body, _ := json.Marshal(sqsEvent{CRN: crn, NudgeMasterID: nudgeMasterID})
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id: aws.String(fmt.Sprintf("msg_%d", i)), MessageBody: aws.String(string(body)),
			})
		}
		_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(queueURL), Entries: entries})
		require.NoError(t, err)
	}
	t.Logf("published %d messages to SQS", n)
}

func runSQSConsumer(ctx context.Context, t *testing.T, client *sqs.Client, queueURL string,
	sl *sluice.Sluice, processed *atomic.Int64, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
		})
		if err != nil {
			select {
			case <-stopCh: return
			default: time.Sleep(100 * time.Millisecond); continue
			}
		}
		for _, msg := range out.Messages {
			var evt sqsEvent
			if err := json.Unmarshal([]byte(*msg.Body), &evt); err != nil { continue }
			if writeErr := sl.Write(ctx, evt.CRN, makePayload(evt.NudgeMasterID)); writeErr == nil { processed.Add(1) }
			_, _ = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: msg.ReceiptHandle})
		}
	}
}

func TestSQSConsumer_BatchFlush(t *testing.T) {
	const totalMessages, consumerCount, nudgeMasterID = 5_000, 4, "nm_sqs_test"
	sqsClient := newLocalStackSQS(t)
	queueURL  := createSQSQueue(t, sqsClient, "sluice-integration-test")
	sl, _     := buildIntegrationSluice(t, "sqs_inventory")
	coll      := mongoCollection(t, "sqs_inventory")
	_, _       = coll.DeleteMany(context.Background(), bson.M{})
	publishSQSMessages(t, sqsClient, queueURL, totalMessages, nudgeMasterID)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	var processed atomic.Int64
	for i := 0; i < consumerCount; i++ { wg.Add(1); go runSQSConsumer(ctx, t, sqsClient, queueURL, sl, &processed, stopCh, &wg) }
	require.Eventually(t, func() bool { return processed.Load() >= int64(totalMessages) }, 45*time.Second, 500*time.Millisecond)
	close(stopCh); wg.Wait()
	waitForCount(t, coll, bson.M{}, int64(totalMessages), 30*time.Second)
	require.Equal(t, int64(totalMessages), countDocs(t, coll, bson.M{}))
	t.Logf("SQS test passed: %d docs in MongoDB", totalMessages)
}

func TestSQSConsumer_SpikeLoad(t *testing.T) {
	const totalMessages = 10_000
	sqsClient := newLocalStackSQS(t)
	queueURL  := createSQSQueue(t, sqsClient, "sluice-spike-test")
	sl, _     := buildIntegrationSluice(t, "sqs_spike")
	coll      := mongoCollection(t, "sqs_spike")
	_, _       = coll.DeleteMany(context.Background(), bson.M{})
	publishSQSMessages(t, sqsClient, queueURL, totalMessages, "nm_spike")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	var processed atomic.Int64
	for i := 0; i < 8; i++ { wg.Add(1); go runSQSConsumer(ctx, t, sqsClient, queueURL, sl, &processed, stopCh, &wg) }
	require.Eventually(t, func() bool { return processed.Load() >= int64(totalMessages) }, 75*time.Second, 500*time.Millisecond)
	close(stopCh); wg.Wait()
	waitForCount(t, coll, bson.M{}, int64(totalMessages), 30*time.Second)
	t.Logf("SQS spike test passed: %d docs in MongoDB", totalMessages)
}
