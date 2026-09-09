package main

import (
	"context"
	"distributed-job-system/internal/config"
	"distributed-job-system/internal/handlers"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/middleware"
	"distributed-job-system/internal/producer"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/worker"

	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-system/internal/drivers/rabbit"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startReaper(ctx context.Context, q *queue.RedisQueue, timer time.Duration) {
	ticker := time.NewTicker(timer)
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

func startDelayedMover(ctx context.Context, q *queue.RedisQueue, timer time.Duration) {
	ticker := time.NewTicker(timer)

	defer ticker.Stop()

	logger.Log.Info("Delayed job mover started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Delayed job mover shutting down")
			return
		case <-ticker.C:
			n, err := q.MoveReadyDelayedJobs(ctx)
			if err != nil {
				logger.Log.Error("Delayed mover failed", "error", err)
				continue
			}
			if n > 0 {
				metrics.JobsDelayedMoved.Add(float64(n))
				logger.Log.Info("Moved delayed jobs to main queue", "count", n)
			}
		}
	}
}

func startMetricsSampler(ctx context.Context, q *queue.RedisQueue, timer time.Duration) {
	ticker := time.NewTicker(timer)

	defer ticker.Stop()

	logger.Log.Info("Metrics sampler started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Metrics sampler shutting down")
			return
		case <-ticker.C:
			stats, err := q.Stats(ctx)
			if err != nil {
				logger.Log.Error("metrics sampler failed", "error", err)
				continue
			}
			metrics.QueueDepth.Set(float64(stats.MainDepth))
			metrics.JobsInFlight.Set(float64(stats.Processing))
			metrics.JobsDelayed.Set(float64(stats.Delayed))
			metrics.DeadLetterDepth.Set(float64(stats.DeadLetter))
		}
	}
}

func main() {
	cfg, err := config.Load()

	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger.Log.Info("config loaded",
		"file", cfg.ConfigFile,
		"http_addr", cfg.HTTPAddr,
		"redis_addr", cfg.RedisAddr,
		"worker_count", cfg.WorkerCount,
		"max_retries", cfg.MaxRetries,
		"visibility_timeout", cfg.VisibilityTimeout.String(),
	)
	metrics.Init()

	rb, err := rabbit.New(cfg.RabbitURL)
	if err != nil {
		log.Fatalf("rabbit: %v", err)
	}
	defer func() {

		if err := rb.Close(context.Background()); err != nil {
			logger.Log.Error(
				"Closing RabbitMq connection",
				"Error", err,
			)
		}
	}()

	REDIS_ADDR := os.Getenv("REDIS_ADDR")
	q := queue.NewRedisQueue(REDIS_ADDR)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()
	if err := q.Ping(pingCtx); err != nil {
		log.Fatalf("redis not reachable at %s: %v", cfg.RedisAddr, err)
	}

	registry := worker.NewRegistry()

	registry.Register("print", &worker.PrintHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i <= cfg.WorkerCount; i++ {

		w := worker.NewWorker(i, q, cfg.MaxRetries)

		go w.Start(ctx, registry, cfg.VisibilityTimeout)

	}

	go startReaper(ctx, q, cfg.ReaperInterval)

	go startDelayedMover(ctx, q, cfg.DelayedMoverInterval)

	go startMetricsSampler(ctx, q, cfg.MetricsInterval)

	mux := http.NewServeMux()

	mux.HandleFunc("/job", producer.Handler(q))
	mux.HandleFunc("/jobs/", handlers.GetJobHandler(q))
	mux.HandleFunc("/dead-jobs/all", handlers.DeadJobHandler(q))
	mux.HandleFunc("/dead-jobs/{id}/replay", handlers.ReplayDeadJobHandler(q))
	mux.HandleFunc("/delayed-jobs", handlers.DelayedJobsHandler(q))
	mux.HandleFunc("/dead-jobs", handlers.DeadJobsPagedHandler(q))
	mux.HandleFunc("/stats", handlers.StatsHandler(q))

	mux.HandleFunc("/health", handlers.HealthHandler())
	mux.HandleFunc("/ready", handlers.ReadyHandler(q))

	mux.Handle("/metrics", promhttp.Handler())

	var handler http.Handler = mux
	handler = middleware.RequestID(handler)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: handler,
	}

	go func() {
		logger.Log.Info(
			"Server running",
			"PORT", cfg.HTTPAddr,
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
