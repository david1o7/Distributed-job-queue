package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributed-job-system/internal/broker"
	"distributed-job-system/internal/config"
	"distributed-job-system/internal/drivers/rabbit"
	"distributed-job-system/internal/handlers"
	"distributed-job-system/internal/jobservice"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/middleware"
	"distributed-job-system/internal/producer"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/worker"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startReaper(ctx context.Context, r broker.LeaseReaper, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Log.Info("Reaper started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Reaper shutting down")
			return
		case <-ticker.C:
			n, err := r.ReapExpired(ctx)
			if err != nil {
				logger.Log.Error("Reaper failed", "error", err)
				continue
			}
			if n > 0 {
				metrics.JobsReaped.Add(float64(n))
				logger.Log.Warn("Reaped expired jobs (visibility timeout)", "count", n)
			}
		}
	}
}

func startDelayedMover(ctx context.Context, m broker.DelayedMover, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Log.Info("Delayed job mover started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Delayed job mover shutting down")
			return
		case <-ticker.C:
			n, err := m.MoveReadyDelayedJobs(ctx)
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

func startMetricsSampler(ctx context.Context, store *queue.RedisQueue, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Log.Info("Metrics sampler started")

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Metrics sampler shutting down")
			return
		case <-ticker.C:
			stats, err := store.Stats(ctx)
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

	logger.Init(cfg.LogFormat, cfg.LogLevel)
	metrics.Init()

	logger.Log.Info("config loaded",
		"file", cfg.ConfigFile,
		"http_addr", cfg.HTTPAddr,
		"redis_addr", cfg.RedisAddr,
		"broker", cfg.Broker,
		"worker_count", cfg.WorkerCount,
		"max_retries", cfg.MaxRetries,
		"visibility_timeout", cfg.VisibilityTimeout.String(),
	)

	store := queue.NewRedisQueue(cfg.RedisAddr)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	if err := store.Ping(pingCtx); err != nil {
		cancelPing()
		log.Fatalf("redis not reachable at %s: %v", cfg.RedisAddr, err)
	}
	cancelPing()

	var q broker.Queue
	switch strings.ToLower(strings.TrimSpace(cfg.Broker)) {
	case "rabbitmq", "rabbit":
		rb, err := rabbit.New(cfg.RabbitURL)
		if err != nil {
			log.Fatalf("rabbit: %v", err)
		}
		q = rb
		logger.Log.Info("using RabbitMQ broker")
	case "kafka":
		log.Fatal("kafka adapter not wired yet — use redis or rabbitmq")
	default:
		q = store
		logger.Log.Info("using Redis broker")
	}

	svc := jobservice.New(store, q)

	registry := worker.NewRegistry()
	registry.Register("print", &worker.PrintHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i <= cfg.WorkerCount; i++ {
		w := worker.NewWorker(i, q, store, cfg.MaxRetries)
		go w.Start(ctx, registry, cfg.VisibilityTimeout)
	}

	if r, ok := q.(broker.LeaseReaper); ok {
		go startReaper(ctx, r, cfg.ReaperInterval)
	}
	if m, ok := q.(broker.DelayedMover); ok {
		go startDelayedMover(ctx, m, cfg.DelayedMoverInterval)
	}
	go startMetricsSampler(ctx, store, cfg.MetricsInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", producer.Handler(svc))
	mux.HandleFunc("POST /job", producer.Handler(svc)) 
	mux.HandleFunc("GET /jobs/{id}", handlers.GetJobHandler(store))
	mux.HandleFunc("GET /dead-jobs/all", handlers.DeadJobHandler(store))
	mux.HandleFunc("POST /dead-jobs/{id}/replay", handlers.ReplayDeadJobHandler(store))
	mux.HandleFunc("GET /delayed-jobs", handlers.DelayedJobsHandler(store))
	mux.HandleFunc("GET /dead-jobs", handlers.DeadJobsPagedHandler(store))
	mux.HandleFunc("GET /stats", handlers.StatsHandler(store))
	mux.HandleFunc("GET /health", handlers.HealthHandler())
	mux.HandleFunc("GET /ready", handlers.ReadyHandler(store))
	mux.Handle("GET /metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: middleware.RequestID(mux),
	}

	go func() {
		logger.Log.Info("Server running", "PORT", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	logger.Log.Info(
		"Server shutdown", 
		"shutdown_signal", sig,
	)

	cancel()
	_ = q.Close(context.Background())
	if err := server.Shutdown(context.Background()); err != nil {
		logger.Log.Error(
			"Server shutdown failed", 
			"error", err,
		)
	}
}