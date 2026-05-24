# Sleipnir — developer Makefile.
# All targets are idempotent and run from the repo root.

GO            ?= go
GOLANGCI_LINT ?= golangci-lint
DOCKER        ?= docker
COMPOSE       ?= docker compose
JSONNET       ?= jsonnet

DASHBOARD_SRC := telemetry/dashboards/src/sleipnir.jsonnet
DASHBOARD_OUT := telemetry/grafana/provisioning/dashboards/dashboard.json

.PHONY: help build test test-race test-cover lint fmt vet ci \
        compose-up compose-down compose-logs \
        integration clean tidy dashboard-regen

help: ## Print this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build all binaries to ./bin.
	$(GO) build -o bin/sleipnir ./cmd/sleipnir
	$(GO) build -o bin/mock_huginn ./cmd/mock_huginn
	$(GO) build -o bin/mock_muninn ./cmd/mock_muninn

test: ## Run unit tests.
	$(GO) test ./...

test-race: ## Run unit tests with the race detector.
	$(GO) test -race ./...

test-cover: ## Run unit tests with coverage; writes cover.out.
	$(GO) test -race -coverprofile=cover.out ./...
	$(GO) tool cover -func=cover.out | tail -1

vet: ## Run go vet.
	$(GO) vet ./...

lint: ## Run golangci-lint.
	$(GOLANGCI_LINT) run

fmt: ## Run gofmt on all .go files.
	$(GO) fmt ./...

tidy: ## go mod tidy.
	$(GO) mod tidy

ci: vet test-race lint ## Mirror what .github/workflows/ci.yml runs on every PR.

compose-up: ## Bring the local stack up (sleipnir + redpanda + mocks + prom + grafana).
	$(COMPOSE) up -d --build

compose-down: ## Tear the local stack down and remove volumes.
	$(COMPOSE) down -v

compose-logs: ## Tail the gateway logs.
	$(COMPOSE) logs -f sleipnir

integration: ## Run integration tests (Phase 4 — build tag `integration`).
	$(GO) test -tags=integration -race ./...

clean: ## Remove build outputs.
	rm -rf bin/ cover.out

dashboard-regen: ## Compile Jsonnet source → provisioning dashboard JSON (requires: brew install jsonnet).
	$(JSONNET) $(DASHBOARD_SRC) > $(DASHBOARD_OUT)
