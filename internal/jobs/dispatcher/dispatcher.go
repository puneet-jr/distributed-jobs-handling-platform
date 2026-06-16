package dispatcher

import "context"

// JobMessage wraps the payload and the acknowledgement functions.
// This allows the worker to tell the message broker when a job is truly done.
type JobMessage struct {
	JobID string
	Ack   func() error
	Nack  func(requeue bool) error
}

type Dispatcher interface {
	Enqueue(ctx context.Context, jobID string) error
	Jobs(ctx context.Context) <-chan JobMessage
}