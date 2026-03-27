SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE    := github.com/hussainpithawala/sluice-go
BINARY    := sluice
GO        := go
GOTEST    := $(GO) test
GOLINT    := golangci-lint
GORELEASER := goreleaser

DC        := docker compose
DC_FILE   := docker-compose.yml

REDIS_TIMEOUT      := 30
MONGO_TIMEOUT      := 45
KAFKA_TIMEOUT      := 90
LOCALSTACK_TIMEOUT := 60

GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
RESET  := \033[0m

.PHONY: help
help:
	@echo "" && echo "  sluice — wide-breadth Redis-shielded write batcher" && echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "  Usage: make \033[36m<target>\033[0m\n\n  Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "    \033[36m%-26s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

build:     ## Compile all packages
	$(GO) build ./...

vet:       ## Run go vet
	$(GO) vet ./...

tidy:      ## Tidy go.mod and go.sum
	$(GO) mod tidy && $(GO) mod verify

lint:      ## Run golangci-lint
	$(GOLINT) run ./...

fmt:       ## Format all Go source files
	$(GO) fmt ./...

test-unit: ## Run unit tests (starts Redis if needed)
	@$(DC) -f $(DC_FILE) up -d redis && $(MAKE) _wait-redis
	$(GOTEST) -v -race -count=1 -timeout=120s ./tests/unit/... 2>&1 | tee /tmp/sluice-unit.log

test-integration: docker-up ## Run all integration tests
	REDIS_ADDR=localhost:6379 MONGO_URI=mongodb://localhost:27017 \
	SQS_ENDPOINT=http://localhost:4566 KAFKA_BROKER=localhost:9092 \
	$(GOTEST) -v -race -count=1 -timeout=300s -tags=integration \
		./tests/integration/... 2>&1 | tee /tmp/sluice-integration.log

test-integration-sqs: docker-up ## Run only SQS integration tests
	REDIS_ADDR=localhost:6379 MONGO_URI=mongodb://localhost:27017 SQS_ENDPOINT=http://localhost:4566 \
	$(GOTEST) -v -race -count=1 -timeout=180s -tags=integration -run TestSQS ./tests/integration/...

test-integration-kafka: docker-up ## Run only Kafka integration tests
	REDIS_ADDR=localhost:6379 MONGO_URI=mongodb://localhost:27017 KAFKA_BROKER=localhost:9092 \
	$(GOTEST) -v -race -count=1 -timeout=240s -tags=integration -run TestKafka ./tests/integration/...

test-all: test-unit test-integration ## Run unit + integration then tear down
	@$(MAKE) docker-down && printf "$(GREEN)▶ All tests passed.$(RESET)\n"

coverage: ## Generate HTML coverage report
	$(GOTEST) -coverprofile=/tmp/sluice-coverage.out -covermode=atomic ./tests/unit/... ./...
	$(GO) tool cover -html=/tmp/sluice-coverage.out -o /tmp/sluice-coverage.html
	@$(GO) tool cover -func=/tmp/sluice-coverage.out | tail -1

docker-up: ## Start Redis, MongoDB, Kafka, LocalStack
	@printf "$(GREEN)▶ Starting integration stack...$(RESET)\n"
	$(DC) -f $(DC_FILE) up -d --remove-orphans
	@$(MAKE) _wait-redis && $(MAKE) _wait-mongo && $(MAKE) _wait-kafka && $(MAKE) _wait-localstack
	@printf "$(GREEN)▶ All services healthy.$(RESET)\n"

docker-down: ## Stop all integration services
	$(DC) -f $(DC_FILE) down -v --remove-orphans

docker-logs: ## Tail logs from all services
	$(DC) -f $(DC_FILE) logs -f

docker-restart: docker-down docker-up ## Restart the full stack

release-snapshot: ## Build release snapshot locally
	$(GORELEASER) release --snapshot --clean

release-check: ## Validate .goreleaser.yml
	$(GORELEASER) check

clean: ## Remove build artefacts and test cache
	$(GO) clean -testcache -cache
	rm -rf dist/ /tmp/sluice-*.log /tmp/sluice-*.out /tmp/sluice-*.html

check: tidy vet lint test-unit ## Full pre-commit check

_wait-redis:
	@printf "$(YELLOW)  Waiting for Redis$(RESET)"; \
	for i in $$(seq 1 $(REDIS_TIMEOUT)); do \
		docker exec sluice_redis redis-cli ping 2>/dev/null | grep -q PONG && printf " $(GREEN)ready$(RESET)\n" && exit 0; \
		printf "."; sleep 1; done; printf "\n$(RED)Redis timeout$(RESET)\n"; exit 1

_wait-mongo:
	@printf "$(YELLOW)  Waiting for MongoDB$(RESET)"; \
	for i in $$(seq 1 $(MONGO_TIMEOUT)); do \
		docker exec sluice_mongo mongosh --eval "db.adminCommand('ping').ok" --quiet 2>/dev/null | grep -q 1 && printf " $(GREEN)ready$(RESET)\n" && exit 0; \
		printf "."; sleep 1; done; printf "\n$(RED)MongoDB timeout$(RESET)\n"; exit 1

_wait-kafka:
	@printf "$(YELLOW)  Waiting for Kafka$(RESET)"; \
	for i in $$(seq 1 $(KAFKA_TIMEOUT)); do \
		docker exec sluice_kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && printf " $(GREEN)ready$(RESET)\n" && exit 0; \
		printf "."; sleep 1; done; printf "\n$(RED)Kafka timeout$(RESET)\n"; exit 1

_wait-localstack:
	@printf "$(YELLOW)  Waiting for LocalStack$(RESET)"; \
	for i in $$(seq 1 $(LOCALSTACK_TIMEOUT)); do \
		curl -sf http://localhost:4566/_localstack/health 2>/dev/null | grep -q '"sqs": "available"' && printf " $(GREEN)ready$(RESET)\n" && exit 0; \
		printf "."; sleep 1; done; printf "\n$(RED)LocalStack timeout$(RESET)\n"; exit 1
