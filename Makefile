SHELL := /bin/bash
.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN := bin
IMAGE ?= beacon:local

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/ { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build beacon and loadgen into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN)/beacon ./cmd/beacon
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN)/loadgen ./cmd/loadgen

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	go test -race -tags integration ./...

.PHONY: bench
bench: ## Run benchmarks
	go test -run '^$$' -bench . -benchmem ./...

.PHONY: fuzz
fuzz: ## Fuzz the UTM parser for 30s
	go test -run '^$$' -fuzz FuzzParseUTM -fuzztime 30s ./internal/enrich

.PHONY: lint
lint: ## Run go vet and golangci-lint
	go vet ./...
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format the code with gofumpt and goimports
	golangci-lint fmt ./...

.PHONY: cover
cover: ## Run unit tests with coverage and print a summary
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out | tail -n 1

.PHONY: run
run: ## Run beacon locally with the stdout sink
	go run ./cmd/beacon -sink stdout

.PHONY: loadgen
loadgen: ## Fire synthetic traffic at a running beacon for 30s
	go run ./cmd/loadgen -duration 30s

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -t $(IMAGE) .

.PHONY: up
up: ## Start postgres and beacon with docker compose
	docker compose up -d --build

.PHONY: down
down: ## Stop the docker compose stack and remove volumes
	docker compose down -v

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN) dist coverage.out
