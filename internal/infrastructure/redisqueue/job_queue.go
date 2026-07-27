package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	domainjob "distributed-job-platform/internal/domain/job"
	"distributed-job-platform/internal/workers"
	"github.com/redis/go-redis/v9"
)

const (
	defaultStream = "jobs"
)

// JobQueue implements application/job.Queue using Redis Streams.
//
// Why Redis Streams?
// Streams support durable entries, consumer groups, acknowledgements,
// pending messages, and reclaiming stuck work. That is exactly what workers need.
type JobQueue struct {
	client *redis.Client
	stream string
}

type JobConsumer struct {
	client *redis.Client
	stream string
	group  string
	block  time.Duration
}

func NewJobQueue(client *redis.Client, stream string) (*JobQueue, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}

	if stream == "" {
		stream = defaultStream
	}

	return &JobQueue{
		client: client,
		stream: stream,
	}, nil
}

func (q *JobQueue) Enqueue(ctx context.Context, msg domainjob.QueueMessage) error {
	if msg.JobID == "" {
		return errors.New("job id is required")
	}

	if msg.Type == "" {
		return errors.New("job type is required")
	}

	// Why XAdd?
	// XAdd appends a message to a Redis Stream.
	// Workers can later consume it using consumer groups.
	_, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"job_id": msg.JobID,
			"type":   msg.Type,
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	return nil
}

func EnsureConsumerGroup(ctx context.Context, client *redis.Client, stream string, group string) error {
	if client == nil {
		return errors.New("redis client cannot be nil")
	}
	if stream == "" {
		stream = defaultStream
	}
	if group == "" {
		return errors.New("consumer group is required")
	}

	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !errors.Is(err, redis.BusyGroupErr) {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

func NewJobConsumer(client *redis.Client, stream string, group string, block time.Duration) (*JobConsumer, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}
	if stream == "" {
		stream = defaultStream
	}
	if group == "" {
		return nil, errors.New("consumer group is required")
	}
	if block <= 0 {
		block = 5 * time.Second
	}

	return &JobConsumer{
		client: client,
		stream: stream,
		group:  group,
		block:  block,
	}, nil
}

func (c *JobConsumer) Read(ctx context.Context, consumer string, count int) ([]workers.Message, error) {
	if consumer == "" {
		return nil, errors.New("consumer is required")
	}
	if count <= 0 {
		count = 1
	}

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: consumer,
		Streams:  []string{c.stream, ">"},
		Count:    int64(count),
		Block:    c.block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("read stream: %w", err)
	}

	return parseMessages(streams), nil
}

func (c *JobConsumer) Ack(ctx context.Context, messageID string) error {
	if messageID == "" {
		return errors.New("message id is required")
	}
	if err := c.client.XAck(ctx, c.stream, c.group, messageID).Err(); err != nil {
		return fmt.Errorf("ack stream message: %w", err)
	}
	return nil
}

func (c *JobConsumer) Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]workers.Message, error) {
	if consumer == "" {
		return nil, errors.New("consumer is required")
	}
	if minIdle <= 0 {
		minIdle = time.Minute
	}
	if count <= 0 {
		count = 100
	}

	messages, _, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("autoclaim stream messages: %w", err)
	}

	return parseStreamMessages(messages), nil
}

func parseMessages(streams []redis.XStream) []workers.Message {
	out := make([]workers.Message, 0)
	for _, stream := range streams {
		out = append(out, parseStreamMessages(stream.Messages)...)
	}
	return out
}

func parseStreamMessages(messages []redis.XMessage) []workers.Message {
	out := make([]workers.Message, 0, len(messages))
	for _, message := range messages {
		jobID, _ := message.Values["job_id"].(string)
		jobType, _ := message.Values["type"].(string)
		if jobID == "" || jobType == "" {
			continue
		}
		out = append(out, workers.Message{
			ID:    message.ID,
			JobID: jobID,
			Type:  jobType,
		})
	}
	return out
}
