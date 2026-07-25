package redisqueue

import (
	"context"
	"errors"
	"fmt"


	appjob "distributed-job-platform/internal/application/job"

	"github.com/redis/go-redis/v9"
)

const (
	defaultStream = "jobs"
)


// JobQueue implements application/job.Queue using Redis Streams.
//
// Why Redis Streams?
// Streams support durable entries, consumer groups, acknowledgements,
// pending messages, and reclaiming stuck work. That is exactly what workers need

type JobQueue struct {
	client *redis.Client
	stream string
}

pfunc JobQueue(client *redis.Client, stream string)(*JobQueue, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}

	if stream = "" {
		stream = defaultStream
	}

	return &JobQueue {
		client: client,
		stream: stream,
	}, nil
}

func(q *JobQueue) Enqueue(ctx context.Context, msg appjob.QueueMessage) error {
	if msg.JobID == "" {
		return errors.New("job id is required")
	}

	if msg.Type == "" {
		return errors.New("job type is required")
	}

	
	// Why XADD?
	// XADD appends a message to a Redis Stream.
	// Workers can later consume it using consumer groups.
	_, err := q.client.XADD(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"job_id": msg.JobID,
			"type": msg.Type,
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	return nil
}