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

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

type kafkaEvent struct {
	CRN           string `json:"crn"`
	NudgeMasterID string `json:"nudge_master_id"`
	SequenceNo    int    `json:"seq"`
}

func newKafkaWriter(t *testing.T, topic string) *kafka.Writer {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(kafkaBroker()), Topic: topic, Balancer: &kafka.Hash{},
		BatchSize: 100, BatchTimeout: 10 * time.Millisecond, RequiredAcks: kafka.RequireOne}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func newKafkaReader(t *testing.T, topic, groupID string) *kafka.Reader {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker()}, GroupID: groupID, Topic: topic,
		MinBytes: 1, MaxBytes: 10e6, CommitInterval: 100 * time.Millisecond, StartOffset: kafka.FirstOffset,
	})
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func ensureKafkaTopic(t *testing.T, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", kafkaBroker())
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Logf("CreateTopics (may already exist): %v", err)
	}

	// Wait for topic metadata to propagate so the writer doesn't hit
	// "Unknown Topic Or Partition".
	require.Eventually(t, func() bool {
		c, dialErr := kafka.Dial("tcp", kafkaBroker())
		if dialErr != nil {
			return false
		}
		defer c.Close()
		parts, readErr := c.ReadPartitions(topic)
		return readErr == nil && len(parts) >= partitions
	}, 30*time.Second, 500*time.Millisecond, "topic %s not ready with %d partitions", topic, partitions)
}

func publishKafkaMessages(t *testing.T, w *kafka.Writer, n int, nudgeMasterID string) {
	t.Helper()
	ctx := context.Background()
	msgs := make([]kafka.Message, 0, n)
	for i := 0; i < n; i++ {
		crn := fmt.Sprintf("crn_kafka_%07d", i)
		body, _ := json.Marshal(kafkaEvent{CRN: crn, NudgeMasterID: nudgeMasterID, SequenceNo: i})
		msgs = append(msgs, kafka.Message{Key: []byte(crn), Value: body})
	}
	for start := 0; start < len(msgs); start += 200 {
		end := start + 200
		if end > len(msgs) {
			end = len(msgs)
		}
		require.NoError(t, w.WriteMessages(ctx, msgs[start:end]...))
	}
	t.Logf("published %d messages to Kafka topic %s", n, w.Topic)
}

func runKafkaConsumer(ctx context.Context, t *testing.T, reader *kafka.Reader,
	sl *sluice.Sluice, processed *atomic.Int64, target int64, wg *sync.WaitGroup) {
	defer wg.Done()
	for processed.Load() < target {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var evt kafkaEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if writeErr := sl.Write(ctx, evt.CRN, makePayload(evt.NudgeMasterID)); writeErr == nil {
			processed.Add(1)
		}
		_ = reader.CommitMessages(ctx, msg)
	}
}

func TestKafkaConsumer_BatchFlush(t *testing.T) {
	const totalMessages, partitions, consumerCount = 5_000, 8, 4
	const topic, groupID, nudgeMasterID = "sluice-nudge-inventory", "sluice-integration", "nm_kafka_test"
	ensureKafkaTopic(t, topic, partitions)
	sl, _ := buildIntegrationSluice(t, "kafka_inventory")
	coll := mongoCollection(t, "kafka_inventory")
	_, _ = coll.DeleteMany(context.Background(), bson.M{})
	writer := newKafkaWriter(t, topic)
	publishKafkaMessages(t, writer, totalMessages, nudgeMasterID)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var processed atomic.Int64
	for i := 0; i < consumerCount; i++ {
		reader := newKafkaReader(t, topic, groupID)
		wg.Add(1)
		go runKafkaConsumer(ctx, t, reader, sl, &processed, int64(totalMessages), &wg)
	}
	require.Eventually(t, func() bool { return processed.Load() >= int64(totalMessages) }, 75*time.Second, 500*time.Millisecond)
	wg.Wait()
	waitForCount(t, coll, bson.M{}, int64(totalMessages), 30*time.Second)
	t.Logf("Kafka test passed: %d docs in MongoDB", totalMessages)
}

func TestKafkaConsumer_HighThroughput(t *testing.T) {
	const totalMessages, partitions, consumerCount = 50_000, 16, 8
	const topic, groupID, nudgeMasterID = "sluice-high-throughput", "sluice-perf", "nm_perf_test"
	ensureKafkaTopic(t, topic, partitions)
	sl, _ := buildIntegrationSluice(t, "kafka_perf")
	coll := mongoCollection(t, "kafka_perf")
	_, _ = coll.DeleteMany(context.Background(), bson.M{})
	start := time.Now()
	writer := newKafkaWriter(t, topic)
	publishKafkaMessages(t, writer, totalMessages, nudgeMasterID)
	t.Logf("published %d messages in %s", totalMessages, time.Since(start).Round(time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var processed atomic.Int64
	for i := 0; i < consumerCount; i++ {
		reader := newKafkaReader(t, topic, groupID)
		wg.Add(1)
		go runKafkaConsumer(ctx, t, reader, sl, &processed, int64(totalMessages), &wg)
	}
	require.Eventually(t, func() bool { return processed.Load() >= int64(totalMessages) }, 100*time.Second, time.Second)
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("ingest complete: %d msgs in %s (%.0f msg/s)", totalMessages, elapsed.Round(time.Millisecond), float64(totalMessages)/elapsed.Seconds())
	waitForCount(t, coll, bson.M{}, int64(totalMessages), 30*time.Second)
	require.Equal(t, int64(totalMessages), countDocs(t, coll, bson.M{}))
	t.Logf("high-throughput test passed: %d docs in MongoDB", totalMessages)
}
