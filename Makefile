.PHONY: build run test test-race bench lint vet docker-build docker-up docker-down clean

# ── Go ────────────────────────────────────────────────────────────────────────

build:
	go build ./...

run:
	go run ./cmd/server/

test:
	go test ./...

test-race:
	go test -race ./...

bench:
	cd benchmarks && go test -bench="." -benchmem -benchtime=3s .

vet:
	go vet ./...

lint: vet
	staticcheck ./...

# ── Docker ───────────────────────────────────────────────────────────────────

docker-build:
	docker build -t go-message-queue:latest .

docker-up:
	docker compose --profile default up --build -d

docker-up-small:
	docker compose --profile small up --build -d

docker-up-large:
	docker compose --profile large up --build -d

docker-down:
	docker compose --profile default down
	docker compose --profile small down
	docker compose --profile large down

docker-logs:
	docker compose --profile default logs -f

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	go clean ./...
	docker compose --profile default down --rmi local 2>/dev/null || true
	docker compose --profile small down --rmi local 2>/dev/null || true
	docker compose --profile large down --rmi local 2>/dev/null || true