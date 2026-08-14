package main

import (
	"context"
	"distributed-job-system/internal/handlers"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/producer"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/worker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func startReaper(ctx context.Context, q *queue.RedisQueue) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	logger.Log.Info("Reaper started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Reaper shutting down")
			return
		case <-ticker.C:
			n, err := q.ReapExpired(ctx)
			if err != nil {
				logger.Log.Error(
					"Reaper failed",
					"error", err)
				continue
			}
			if n > 0 {
				metrics.JobsReaped.Add(float64(n))
				logger.Log.Warn(
					"Reaped expired jobs (visibility timeout)",
					"count", n,
				)
			}
		}
	}
}

func main() {
	metrics.Init()

	REDIS_ADDR := os.Getenv("REDIS_ADDR")
	q := queue.NewRedisQueue(REDIS_ADDR)

	registry := worker.NewRegistry()

	registry.Register("print", &worker.PrintHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	WorkerCount := 1 //Safe number to start at if the .env isnt able to load

	if value := os.Getenv("WORKER_COUNT"); value != "" {

		n, err := strconv.Atoi(value)

		if err == nil && n > 0 {
			WorkerCount = n
		}
	}

	maxRetries, _ := strconv.Atoi(os.Getenv("MAX_RETRIES"))

	for i := 1; i <= WorkerCount; i++ {

		w := worker.NewWorker(i, q, maxRetries)

		go w.Start(ctx, registry)

	}

	go startReaper(ctx, q)

	mux := http.NewServeMux()

	mux.HandleFunc("/jobs", producer.Handler(q))
	mux.HandleFunc("/jobs/", handlers.GetJobHandler(q))
	mux.HandleFunc("/dead-jobs", handlers.DeadJobHandler(q))
	mux.HandleFunc("/dead-jobs/{id}/replay", handlers.ReplayDeadJobHandler(q))

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Log.Info(
			"Server running",
			"PORT", 8080,
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	shutdownSignal := <-quit

	logger.Log.Info(
		"Server shutdown",
		"shutdown_signal", shutdownSignal,
	)

	cancel()

	if err := server.Shutdown(context.Background()); err != nil {
		logger.Log.Error(
			"Server shutdown failed",
			"error", err,
		)
	}
}
