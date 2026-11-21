SHELL := /bin/bash
.DEFAULT_GOAL := help

ROOT := $(CURDIR)
GO ?= go
PNPM ?= pnpm
WEB := web
BIN := bin
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help build go-build test test-race vet proto proto-check proto-golden web-install web-dev web-lint web-format-check web-typecheck web-test web-build dev dev-photon dev-down dev-reset ci test-echo-restart clean eval eval-collect eval-label llm-smoke

help: ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: go-build ## Build bin/dor and bin/probe

go-build:
	@mkdir -p $(BIN)
	$(GO) build -o $(BIN)/dor ./cmd/dor
	$(GO) build -o $(BIN)/probe ./tools/probe

test: ## Go tests
	$(GO) test ./...

test-race: ## Go tests with the race detector
	$(GO) test -race ./...

vet: ## go vet
	$(GO) vet ./...

web-install: ## Install web dependencies from the lockfile
	cd $(WEB) && $(PNPM) install --frozen-lockfile

proto: ## Regenerate Go and TypeScript protobuf code
	@test -x $(WEB)/node_modules/.bin/buf || (cd $(WEB) && $(PNPM) install --frozen-lockfile)
	@test -x $(WEB)/node_modules/.bin/protoc-gen-es || (cd $(WEB) && $(PNPM) install --frozen-lockfile)
	PATH="$(ROOT)/$(WEB)/node_modules/.bin:$$PATH" $(WEB)/node_modules/.bin/buf generate
	@echo "proto: generated internal/pb/dorv1 and web/src/gen"

proto-check: ## Fail if regenerating protobuf changes the checked-in output
	./tools/proto-check.sh

proto-golden: ## Rewrite proto/testdata golden binaries (explicit update only)
	$(GO) test ./internal/pb/dorv1 -run TestWriteGolden -update

web-dev: ## Run the Vite dev server
	cd $(WEB) && $(PNPM) dev

web-lint: ## ESLint
	cd $(WEB) && $(PNPM) lint

web-format-check: ## Prettier check
	cd $(WEB) && $(PNPM) format:check

web-typecheck: ## tsc --noEmit
	cd $(WEB) && $(PNPM) typecheck

web-test: ## Vitest
	cd $(WEB) && $(PNPM) test

web-build: ## Production web build
	cd $(WEB) && $(PNPM) build

dev: ## Start the default compose services (Postgres + TEI) and wait
	$(COMPOSE) up -d --wait
	$(COMPOSE) ps

dev-photon: ## Start Photon too (profile: photon)
	$(COMPOSE) --profile photon up -d --wait
	$(COMPOSE) ps

dev-down: ## Stop compose services, keep volumes
	$(COMPOSE) down

dev-reset: ## Stop compose services and delete volumes
	$(COMPOSE) down -v

ci: web-install proto-check vet test-race go-build web-lint web-format-check web-typecheck web-test web-build ## Everything CI runs
	@echo "ci: all checks passed"

test-echo-restart: ## Kill -9 the echo agent mid-stream and verify no duplicate output
	./scripts/test-echo-restart.sh

eval: ## Run the model eval (mock backends when no API keys are set)
	$(GO) run ./eval/cmd/eval

eval-collect: ## Rebuild eval/data candidates from public RSS (network; reviewed diff)
	$(GO) run ./eval/cmd/collect

eval-label: ## Label the eval set interactively (SET=articles|pairs LABELER=human:<name>)
	@test -n "$(LABELER)" || (echo "LABELER is required, e.g. make eval-label LABELER=human:your-name" && exit 1)
	$(GO) run ./eval/cmd/label -set $(or $(SET),articles) -labeler "$(LABELER)"

llm-smoke: ## Verify Claude prompt caching; skips without ANTHROPIC_API_KEY
	$(GO) run ./eval/cmd/llm-smoke

clean: ## Remove build outputs
	rm -rf $(BIN) $(WEB)/dist
