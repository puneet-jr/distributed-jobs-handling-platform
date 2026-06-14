package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-platform/internal/jobs/dispatcher"
	"distributed-job-platform/internal/jobs/executor"
	"distributed-job-platform/internal/jobs/handler"
	"distributed-job-platform/internal/jobs/model"
	"distributed-job-platform/internal/jobs/repository"
	"distributed-job-platform/internal/jobs/service"
	"distributed-job-platform/internal/jobs/worker"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

func main() {
	// 1. Database Connection
	db, err := sql.Open("postgres", "postgres://user:password@localhost:5432/jobdb?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 2. Initialize Dependencies
	jobRepo := repository.NewJobRepository(db)
	
	// In-Memory Dispatcher (Buffer of 100 jobs)
	jobDispatcher := dispatcher.NewInMemoryDispatcher(100)
	
	// Strategy Registry
	executorRegistry := executor.NewExecutorRegistry()
	executorRegistry.Register(model.TypeEmail, executor.NewEmailExecutor())
	// Future: executorRegistry.Register(model.TypePDF, executor.NewPdfExecutor())

	// Service
	jobService := service.NewJobService(jobRepo, jobDispatcher)

	// 3. Start Worker Pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 5 concurrent background workers
	workerPool := worker.NewWorkerPool(jobDispatcher, jobRepo, executorRegistry, 5)
	workerPool.Start(ctx)

	// 4. Setup HTTP Server
	router := gin.Default()
	jobHandler := handler.NewJobHandler(jobService)

	api := router.Group("/api/v1")
	{
		jobs := api.Group("/jobs")
		{
			jobs.POST("", jobHandler.Create)
			jobs.GET("", jobHandler.List)
			jobs.GET("/:id", jobHandler.GetByID)
			jobs.PATCH("/:id/status", jobHandler.UpdateStatus)
		}
	}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// 5. Start Server in Goroutine
	go func() {
		log.Println("Server is running on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// 6. Graceful Shutdown Logic
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server and workers...")

	// A. Stop accepting new HTTP requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	// B. Signal workers to stop picking up NEW jobs from the queue
	cancel()

	// C. Wait for currently RUNNING jobs to finish processing
	workerPool.Stop()

	log.Println("Server and workers exited gracefully")
}