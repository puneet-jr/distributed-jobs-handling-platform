package dispatcher

import "context"

// JobDispatcher abstracts the message queue. 
// In production, this would be implemented by Redis, RabbitMQ, or Kafka.
type JobDispatcher interface {
	Enqueue(ctx context.Context, jobID string) error
	Subscribe() <-chan string
}