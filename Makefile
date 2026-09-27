# ── Umurinzi Backend Makefile ─────────────────────────────────────────────────
#
# Prerequisites (install once):
#   go          >= 1.22         https://go.dev/dl/
#   golangci-lint               https://golangci-lint.run/usage/install/
#   migrate     (CLI)           https://github.com/golang-migrate/migrate
#   docker + docker compose     https://docs.docker.com/
#
# Quick start (local dev with Docker):
#   make dev          # start postgres + redis + run migrations + start server
#
# Quick start (native Go):
#   cp .env.example .env && source .env
#   make infra-up migrate run

# ── Variables ─────────────────────────────────────────────────────────────────

BINARY        := umurinzi
CMD_PATH      := ./cmd/server
BUILD_DIR     := ./bin
BUILD_FLAGS   := -trimpath -ldflags="-s -w"
GONOSUMDB     := *
GOPROXY_LOCAL := file://$(HOME)/go/pkg/mod/cache/download,direct

# Migration settings (override via environment or .env)
DB_URL        ?= postgres://umurinzi:secret@localhost:5432/umurinzi?sslmode=disable
MIGRATE_PATH  := ./migrations

# Docker Compose project name
COMPOSE_PROJECT := umurinzi

.PHONY: all build run dev test vet lint fmt \
        infra-up infra-down infra-logs \
        migrate migrate-down migrate-status \
        docker-build docker-up docker-down docker-logs \
        clean help

# ── Default ───────────────────────────────────────────────────────────────────

all: vet build

# ── Build ─────────────────────────────────────────────────────────────────────

## build: Compile the server binary into ./bin/umurinzi
build:
	@mkdir -p $(BUILD_DIR)
	GONOSUMDB=$(GONOSUMDB) \
	go build $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY) $(CMD_PATH)
	@echo "✓ built $(BUILD_DIR)/$(BINARY)"

## run: Run the server directly (requires .env to be sourced or env vars set)
run: build
	$(BUILD_DIR)/$(BINARY)

# ── Code quality ──────────────────────────────────────────────────────────────

## vet: Run go vet across all packages
vet:
	GONOSUMDB=$(GONOSUMDB) go vet ./...
	@echo "✓ vet passed"

## fmt: Format all Go source files
fmt:
	gofmt -w -s .
	@echo "✓ fmt done"

## lint: Run golangci-lint (requires golangci-lint to be installed)
lint:
	golangci-lint run ./...
	@echo "✓ lint passed"

## test: Run all tests (none written yet — placeholder for CI)
test:
	GONOSUMDB=$(GONOSUMDB) go test -race -count=1 ./...

## tidy: Sync go.mod / go.sum
tidy:
	GONOSUMDB=$(GONOSUMDB) GOPROXY=$(GOPROXY_LOCAL) go mod tidy -e

# ── Local infrastructure (Docker Compose, infra only) ─────────────────────────

## infra-up: Start postgres and redis only (no backend service)
infra-up:
	docker compose -p $(COMPOSE_PROJECT) up -d postgres redis
	@echo "✓ postgres and redis started"

## infra-down: Stop and remove infra containers (keeps volumes)
infra-down:
	docker compose -p $(COMPOSE_PROJECT) stop postgres redis
	docker compose -p $(COMPOSE_PROJECT) rm -f postgres redis

## infra-logs: Tail infra logs
infra-logs:
	docker compose -p $(COMPOSE_PROJECT) logs -f postgres redis

# ── Database migrations ───────────────────────────────────────────────────────

## migrate: Apply all pending up migrations
migrate:
	migrate -path $(MIGRATE_PATH) -database "$(DB_URL)" up
	@echo "✓ migrations applied"

## migrate-down: Roll back the last migration
migrate-down:
	migrate -path $(MIGRATE_PATH) -database "$(DB_URL)" down 1
	@echo "✓ rolled back 1 migration"

## migrate-reset: Roll back ALL migrations (destructive — dev only)
migrate-reset:
	@echo "WARNING: this will destroy all data. Press Ctrl-C to cancel."
	@sleep 3
	migrate -path $(MIGRATE_PATH) -database "$(DB_URL)" down -all
	@echo "✓ all migrations rolled back"

## migrate-status: Show current migration version
migrate-status:
	migrate -path $(MIGRATE_PATH) -database "$(DB_URL)" version

# ── Full Docker Compose stack ─────────────────────────────────────────────────

## docker-build: Build the backend Docker image
docker-build:
	docker compose -p $(COMPOSE_PROJECT) build backend

## docker-up: Start the full stack (postgres, redis, migrate, backend)
docker-up:
	docker compose -p $(COMPOSE_PROJECT) up --build -d
	@echo "✓ full stack started — backend at http://localhost:8080"

## docker-down: Stop and remove all containers (keeps volumes)
docker-down:
	docker compose -p $(COMPOSE_PROJECT) down

## docker-destroy: Stop all containers AND delete volumes (wipes DB)
docker-destroy:
	docker compose -p $(COMPOSE_PROJECT) down -v

## docker-logs: Tail all service logs
docker-logs:
	docker compose -p $(COMPOSE_PROJECT) logs -f

## docker-logs-backend: Tail backend logs only
docker-logs-backend:
	docker compose -p $(COMPOSE_PROJECT) logs -f backend

# ── Dev convenience target ────────────────────────────────────────────────────

## dev: Start infra, run migrations, then run the server natively
dev: infra-up
	@echo "Waiting for postgres to be ready..."
	@until docker compose -p $(COMPOSE_PROJECT) exec postgres \
		pg_isready -U umurinzi -d umurinzi > /dev/null 2>&1; do sleep 1; done
	@$(MAKE) migrate DB_URL=$(DB_URL)
	@$(MAKE) run

# ── Clean ─────────────────────────────────────────────────────────────────────

## clean: Remove compiled binaries
clean:
	rm -rf $(BUILD_DIR)
	@echo "✓ cleaned"

# ── Help ──────────────────────────────────────────────────────────────────────

## help: Print this help message
help:
	@echo ""
	@echo "Umurinzi Backend — available targets:"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | column -t -s ':'
	@echo ""
