package worker

import (
	"context"
	"log"
	"sync"

	"distributed-job-platform/internal/jobs/dispatcher"
	"distributed-job-platform/internal/jobs/executor"
	"distributed-job-platform/internal/jobs/model"
	"distributed-job-platform/internal/jobs/repository"
)

type WorkerPool struct {
	dispatcher dispatcher.JobDispatcher
	repo repository.JobRepository
	registry *executor.ExecutorRegistry
	wg     sync.WaitGroup
	numWorkers int
}

func NewWorkerPool(
	dispatcher dispatcher.JobDispatcher,
	repo repository.JobRepository,
	registry *executor.ExecutorRegistry,
	numWorkers int,
) *WorkerPool {
	return &WorkerPool{
		dispatcher: dispatcher,
		repo: repo,
		registry: registry,
		numWorkers: numWorkers,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	//Having a note on no of workers starting with
	log.Printf("Starting worker pool with %d workers...",wp.numWorkers)

	for i := 0; i< wp.numWorkers;i++ {
		// Adding to the waiting period of a waiting group and go - run the channel
		wp.wg.Add(1)
		go wp.run(ctx,i)
	}
}

func(wp *WorkerPool) Stop() {

	log.Println("Waiting for workers to finish the current jobs")
	wp.wg.Wait()
	log.Println("All workers stopped")
}

func (wp *WorkerPool) run(ctx context.Context, workerID int) {
	defer wp.wg.Done()
	ch := wp.dispatcher.Subscribe()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d received shutdown signal. Stopping...", workerID)
			return
		case jobID, ok := <-ch:
			if !ok {
				log.Printf("Worker %d: channel closed. Exiting.", workerID)
				return
			}
			
			// Use a background context for processing so it isn't abruptly cancelled 
			// during a graceful shutdown, allowing the current job to finish.
			wp.processJob(context.Background(), workerID, jobID)
		}
	}
}

for(wp *WorkerPool) processJob(ctx context.Context, workerID int,jobID string) {
	// Panic recovery to keep the worker alive if an executor crashes

	defer func() {
		if r := recover(); r!= nil {
			log.Printf("Worker %d: PANIC recovered while processing job %s: %v", workerID,jobID,r)
			wp.repo.UpdateStatus(ctx,jobID,model.StatusFailed)
		}
	}()
}

// fetch job from DB

job,err := wp.repo.GetByID(ctx,jobID)

if err!= nil {
	log.Printf("Woeker %d: Failed to fetch job %s: %v",workerID,jobID,err)
	return
}

// update status to running

if err := wp.repo.UpdateStatus(ctx,jobID,model.StatusRunning); err != nil {
	log.Printf("Worker %d: Failed to update job %s to Running: %v",workerID,jobID,err)
	return
}
