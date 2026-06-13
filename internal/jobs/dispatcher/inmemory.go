package dispatcher 

import (
	"context"
)

type InMemoryDispatcher struct {
	queue chan string
}

func NewInMemoryDispatcher(bufferSize int) *InMemoryDispatcher {
	return &InMemoryDispatcher{
		queue: make(chan string, bufferSize),
	}
}

func(d *InMemoryDispatcher) Enqueue(ctx context.Context,jobID string) error {
	select {
	case d.queue <- jobID:
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func(d *InMemoryDispatcher) Subscribe() <- chan string {
	return d.queue
}
