.PHONY: test test-race cover lint build k6-smoke

test:
	go test ./... -count=1 -timeout=120s

test-race:
	go test ./... -race -count=1 -timeout=180s

cover:
	go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -20

lint:
	golangci-lint run --timeout=5m

build:
	go build -o bin/server ./cmd/server

k6-smoke:
	k6 run -e BASE_URL=$${BASE_URL:-http://localhost:8080} load/k6/smoke.js



**Kafka** (in progress) | Partitioned log | Throughput, replay, consumer groups | Different model: offsets ≠ Redis leases; delay/DLQ via topics |