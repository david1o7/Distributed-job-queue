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
)

func main() {
	metrics.Init()
	
	REDIS_ADDR := os.Getenv("REDIS_ADDR")
	q := queue.NewRedisQueue(REDIS_ADDR)

	registry := worker.NewRegistry()

	registry.Register("print", &worker.PrintHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	WorkerCount := 1  //Safe number to start at if the .env isnt able to load 

	if value := os.Getenv("WORKER_COUNT"); value != "" {

	n, err := strconv.Atoi(value)

	if err == nil && n > 0 {
		WorkerCount = n
	}
	}

	maxRetries, _ := strconv.Atoi(os.Getenv("MAX_RETRIES"))

	for i := 1; i <= WorkerCount; i++{

		w := worker.NewWorker(WorkerCount,q,maxRetries)

		go w.Start(ctx, registry)

	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", producer.Handler(q))
	mux.HandleFunc("/jobs/",handlers.GetJobHandler(q))

	server := &http.Server{
		Addr: ":8080",
		Handler: mux,
	}

	go func ()  {
		logger.Log.Info(
			"Server running",
			"PORT",8080,
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed{
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit
	
	shutdownSignal := <- quit
	logger.Log.Info(
		"Server shutdown",
		"Shutdown_Signal",shutdownSignal,
	)
	cancel()

	server.Shutdown(context.Background())
}