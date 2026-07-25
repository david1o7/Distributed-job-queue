package main

import (
	"context"
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
	q := queue.NewRedisQueue("localhost:6379")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	WorkerCount := 3

	if value := os.Getenv("WORKER_COUNT"); value != "" {

	n, err := strconv.Atoi(value)

	if err == nil && n > 0 {
		WorkerCount = n
	}
	}

	for i := 1; i <= WorkerCount; i++{

		w := worker.NewWorker(WorkerCount,q)

		go w.Start(ctx)

	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", producer.Handler(q))

	server := &http.Server{
		Addr: ":8080",
		Handler: mux,
	}

	go func ()  {
		log.Println("api running on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed{
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit

	log.Println("Shutdown Signal recieved")
	cancel()

	server.Shutdown(context.Background())
}