COMPOSE      := docker compose
PG_URL       := postgres://orderflow:orderflow@postgres:5432/orderflow?sslmode=disable
PSQL         := $(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U orderflow -d orderflow

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- lifecycle ---------------------------------------------------------------

.PHONY: up
up: ## Start infra, run migrations + topic init, wait until ready
	$(COMPOSE) up -d
	$(COMPOSE) wait migrate redpanda-init
	@$(COMPOSE) ps -a

.PHONY: down
down: ## Stop containers (keeps data volumes)
	$(COMPOSE) down

.PHONY: reset
reset: ## Stop containers AND delete volumes (wipes DB + topics)
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps -a

.PHONY: logs
logs: ## Tail logs (svc=postgres to filter)
	$(COMPOSE) logs -f $(svc)

# --- database ----------------------------------------------------------------

.PHONY: migrate-up
migrate-up: ## Apply all pending migrations
	$(COMPOSE) run --rm migrate -path=/migrations -database "$(PG_URL)" up

.PHONY: migrate-down
migrate-down: ## Roll back ONE migration
	$(COMPOSE) run --rm migrate -path=/migrations -database "$(PG_URL)" down 1

.PHONY: migrate-version
migrate-version: ## Print current migration version
	$(COMPOSE) run --rm migrate -path=/migrations -database "$(PG_URL)" version

.PHONY: migrate-new
migrate-new: ## Create a migration pair: make migrate-new name=add_foo
	@test -n "$(name)" || (echo "usage: make migrate-new name=<snake_case>" && exit 1)
	$(COMPOSE) run --rm --no-deps migrate create -ext sql -dir /migrations -seq -digits 6 $(name)

.PHONY: seed
seed: ## Load dev fixtures (idempotent; resets stock levels)
	$(PSQL) < db/seed/dev_seed.sql

.PHONY: psql
psql: ## Open an interactive psql shell
	$(COMPOSE) exec postgres psql -U orderflow -d orderflow

# --- broker ------------------------------------------------------------------

.PHONY: topics
topics: ## Describe topics
	$(COMPOSE) exec redpanda rpk topic describe orders.events inventory.events -p

.PHONY: consume
consume: ## Tail a topic: make consume topic=orders.events
	@test -n "$(topic)" || (echo "usage: make consume topic=<name>" && exit 1)
	$(COMPOSE) exec redpanda rpk topic consume $(topic) --format 'p=%p o=%o key=%k headers=%h\n%v\n\n'

.PHONY: groups
groups: ## List consumer groups and their state
	$(COMPOSE) exec redpanda rpk group list

.PHONY: group-lag
group-lag: ## Per-partition offsets + lag: make group-lag group=inventory-service
	@test -n "$(group)" || (echo "usage: make group-lag group=<name>  (see: make groups)" && exit 1)
	$(COMPOSE) exec redpanda rpk group describe $(group)
