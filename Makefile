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
KAFKA_TIMEOUT      := 60

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
BLUE   := $(shell tput -Txterm setaf 4)
RESET  := $(shell tput -Txterm sgr0)

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


install-lint: ## Install golangci-lint if not present
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "golangci-lint not found. Installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	else \
		echo "golangci-lint is already installed."; \
	fi

install-releaser: ## Install goreleaser if not present
	@if ! command -v goreleaser &> /dev/null; then \
		echo "goreleaser not found. Installing..."; \
		go install github.com/goreleaser/goreleaser/v2@latest; \
	else \
		echo "goreleaser is already installed."; \
	fi

# Code quality
lint: install-lint ## Run linter
	@echo "${GREEN}Running linter...${RESET}"
	@golangci-lint run --timeout=5m --config=.golangci.yml

fmt:       ## Format all Go source files
	$(GO) fmt ./...

test-unit: ## Run unit tests (starts Redis + MongoDB if needed)
	@$(DC) -f $(DC_FILE) up -d redis mongodb && $(MAKE) _wait-redis && $(MAKE) _wait-mongo
	$(GOTEST) -v -race -count=1 -timeout=120s ./tests/unit/... 2>&1 | tee /tmp/sluice-unit.log

test-integration: docker-up ## Run integration tests
	REDIS_ADDR=localhost:6379 MONGO_URI=mongodb://localhost:27017 \
	$(GOTEST) -v -race -count=1 -timeout=300s -tags=integration \
		./tests/integration/... 2>&1 | tee /tmp/sluice-integration.log

test-all: test-unit test-integration ## Run unit + integration then tear down
	@$(MAKE) docker-down && printf "$(GREEN)▶ All tests passed.$(RESET)\n"

coverage: ## Generate HTML coverage report
	$(GOTEST) -coverprofile=/tmp/sluice-coverage.out -covermode=atomic ./tests/unit/... ./...
	$(GO) tool cover -html=/tmp/sluice-coverage.out -o /tmp/sluice-coverage.html
	@$(GO) tool cover -func=/tmp/sluice-coverage.out | tail -1

docker-up: ## Start Redis, MongoDB, Kafka, LocalStack
	@printf "$(GREEN)▶ Starting integration stack...$(RESET)\n"
	$(DC) -f $(DC_FILE) up -d --remove-orphans
	@$(MAKE) _wait-redis && $(MAKE) _wait-mongo && $(MAKE) _wait-kafka
	@printf "$(GREEN)▶ All services healthy.$(RESET)\n"

docker-down: ## Stop all integration services
	$(DC) -f $(DC_FILE) down -v --remove-orphans

docker-logs: ## Tail logs from all services
	$(DC) -f $(DC_FILE) logs -f

docker-restart: docker-down docker-up ## Restart the full stack

release-snapshot: install-releaser ## Build release snapshot locally
	$(GORELEASER) release --snapshot --clean

release: install-releaser ## Create and push a release tag (triggers GitHub Actions)
	@echo "$(YELLOW)⚠️  Creating release tag...$(RESET)"
	@if [ -z "$(TAG)" ]; then \
		echo "$(RED)✗ Error: TAG is required. Usage: make release TAG=v1.0.0$(RESET)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ Tag: $(TAG)$(RESET)"
	@echo ""
	@echo "$(YELLOW)1. Validating .goreleaser.yml...$(RESET)"
	$(GORELEASER) check
	@echo ""
	@echo "$(YELLOW)2. Creating git tag...$(RESET)"
	git tag -a $(TAG) -m "Release $(TAG)"
	@echo ""
	@echo "$(YELLOW)3. Pushing tag to remote...$(RESET)"
	git push origin $(TAG)
	@echo ""
	@echo "$(GREEN)✓ Tag $(TAG) pushed successfully!$(RESET)"
	@echo "$(GREEN)  GitHub Actions will now build and publish the release.$(RESET)"
	@echo "$(GREEN)  Monitor: https://github.com/hussainpithawala/sluice-go/actions$(RESET)"

release-check: install-releaser ## Validate .goreleaser.yml
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
		docker exec sluice_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && printf " $(GREEN)ready$(RESET)\n" && exit 0; \
		printf "."; sleep 1; done; printf "\n$(RED)Kafka timeout$(RESET)\n"; exit 1
