# outbox-order-flow

An event-driven order processing system in Go, built as a **learning project** for
distributed-systems mechanics: the transactional outbox, concurrency control on
shared inventory, idempotent consumers, and Kafka delivery semantics.

A customer places an order → the system records it, reserves inventory, updates the
order status, and sends a notification. Services never call each other directly;
they communicate only through events.

> Built step by step as a pair-programming exercise. Each phase explains the
> trade-offs before writing code. See [Roadmap](#roadmap) for progress.

---



## What this project teaches


| Pattern                  | Where it shows up                                                                                                                                                                       |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transactional outbox** | Business row + event row commit in one Postgres transaction; a relay publishes to Kafka afterwards. No dual-write bug.                                                                  |
| **Concurrency control**  | Inventory reservation under concurrent orders: `SELECT … FOR UPDATE` (pessimistic) vs `version` column (optimistic). A `CHECK (available >= 0)` constraint is the last line of defense. |
| **Idempotency**          | Kafka is at-least-once. Consumers record `(consumer, event_id)` in a `processed_events` table *in the same transaction* as their side effect, so a redelivery becomes a no-op.          |
| **Broker semantics**     | Partition keys and per-order ordering, consumer groups and rebalancing, offset commits, retries, dead-letter topics.                                                                    |
| **Eventual consistency** | An order is `pending` until the inventory result flows back.                                                                                                                            |
| **Operability**          | Graceful shutdown, structured logs, metrics, Docker Compose.                                                                                                                            |


---



## Architecture

```mermaid
flowchart LR
    client([HTTP client]) -->|POST /orders| OS[Order Service]
    OS -->|"one tx: orders + outbox"| ODB[(orders schema)]
    ODB -.->|outbox relay| OT{{orders.events}}
    OT --> IS[Inventory Service]
    IS -->|"one tx: dedupe + stock + reservation + outbox"| IDB[(inventory schema)]
    IDB -.->|outbox relay| IT{{inventory.events}}
    IT --> OS
    IT --> NS[Notification Service]
    NS -->|"dedupe + log"| NDB[(notification schema)]
```





### Services and data ownership

One Postgres database with **one schema per service**, and one Postgres role per service
(migration `000004`) that can reach only its own schema. A service reads and writes only
its own schema and learns about other services' data only through events. (That's why
`inventory.reservations.order_id` is deliberately *not* a foreign key.) Why sharing one
database is acceptable here, and what it costs:
[ADR-0002](docs/adr/0002-shared-database-schema-per-service.md).

Each service runs as a separate process with its own connection pool, Kafka client,
and consumer group. The services share the servers and the `internal/platform` source
code, but never runtime state
([ADR-0005](docs/adr/0005-single-module-shared-platform.md)). The Order Service
consuming `inventory.events` to update order status is still a proposal
([open decisions](AGENTS.md#open-decisions)).


| Service              | Kind                | Owns schema    | Consumes                              | Publishes to       |
| -------------------- | ------------------- | -------------- | ------------------------------------- | ------------------ |
| Order Service        | HTTP API + consumer | `orders`       | `inventory.events` (to update status) | `orders.events`    |
| Inventory Service    | Consumer            | `inventory`    | `orders.events`                       | `inventory.events` |
| Notification Service | Consumer            | `notification` | `inventory.events`                    | —                  |




### Topics

One topic per publishing service. The event type travels in a message header, and
the message **key is the order ID**, so every event for one order lands on the same
partition in order. Per-type topics would split one order's events across topics that
have no order between them
([ADR-0009](docs/adr/0009-one-topic-per-publishing-service.md)). How messages flow, what
each delivery guarantee means, what each event's payload holds, and how failures play
out: [docs/kafka.md](docs/kafka.md).


| Topic              | Partitions | Event types                                                                  |
| ------------------ | ---------- | ---------------------------------------------------------------------------- |
| `orders.events`    | 3          | `order.created`                                                              |
| `inventory.events` | 3          | stock reserved / reservation failed (names settled at the Step 4 checkpoint) |


Topic auto-creation is **disabled**: producing to a misspelled topic fails loudly
instead of silently creating a new one.

### Database schema

ER diagrams with every column, key, constraint, and cross-service reference:
[docs/database-schema.md](docs/database-schema.md). Summary:

```
orders.orders              id (uuidv7, from the service), customer_id, status, failure_reason, idempotency_key, version, timestamps
orders.order_items         (order_id, sku) → quantity
orders.outbox              id = event id, aggregate_id (→ Kafka key), event_type, topic, payload, published_at, attempts
orders.processed_events    (consumer, event_id)

inventory.products         sku → available CHECK (>= 0), reserved, version
inventory.reservations     (order_id, sku) → quantity, status
inventory.outbox           same shape as orders.outbox
inventory.processed_events (consumer, event_id)

notification.notifications id, event_id, order_id, channel, template, payload — UNIQUE (event_id, channel)
```

Migrations live in [`migrations/`](migrations/) as flat `.up.sql` / `.down.sql` pairs
(golang-migrate reads a single directory); each file has a comment on every
non-obvious choice.

### Design decisions

The decisions that shape this architecture, with the alternatives rejected, are recorded
as ADRs:

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-kafka-protocol-via-redpanda.md) | Kafka protocol for events, served by Redpanda locally |
| [0002](docs/adr/0002-shared-database-schema-per-service.md) | One Postgres database: one schema and one role per service |
| [0003](docs/adr/0003-transactional-outbox.md) | Publish events through a transactional outbox |
| [0004](docs/adr/0004-idempotent-consumers.md) | Idempotent consumers: dedupe in the side-effect transaction, then commit the offset |
| [0005](docs/adr/0005-single-module-shared-platform.md) | One Go module: shared platform code, service packages isolated by import rules |
| [0006](docs/adr/0006-outbox-relay-polling-retries.md) | Outbox relay: polling with backoff retries and per-order ordering |
| [0007](docs/adr/0007-event-contracts-consumer-defined.md) | Event contracts: each consumer defines the fields it reads |
| [0008](docs/adr/0008-go-libraries.md) | Go libraries: pgx, franz-go, and the standard library for the rest (superseded by 0011) |
| [0009](docs/adr/0009-one-topic-per-publishing-service.md) | One topic per publishing service, event type in a header |
| [0010](docs/adr/0010-http-idempotency-post-orders.md) | HTTP idempotency for POST /orders: a required key, detected by the unique index |
| [0011](docs/adr/0011-go-libraries-gin-uuid.md) | Go libraries: pgx, franz-go, Gin for HTTP, google/uuid for order IDs |
| [0012](docs/adr/0012-hexagonal-layout-per-service.md) | Hexagonal layout per service: handler → service → domain, repository adapter, the transaction as a port |

Decisions still open are listed in [AGENTS.md](AGENTS.md#open-decisions).

---



## Getting started



### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Go 1.26+
- `make`



### Run the infrastructure

```bash
make up      # start Postgres + Redpanda, apply migrations, create topics
make seed    # load sample inventory (safe to re-run)
```

`make up` waits until the one-shot `migrate` and `redpanda-init` containers exit
successfully.

### Run the services

Each service runs on your host, in its own terminal, and connects to Postgres as its own
role:

```bash
make run-order          # Order Service, HTTP on :8080
make run-inventory      # Inventory Service
make run-notification   # Notification Service
```

Until Step 2b the services only start, connect, check that they landed in their own schema,
and shut down cleanly on Ctrl+C. The Order Service's `GET /healthz` returns 200 while its
database is reachable and 503 while it isn't.

### Endpoints


| What                  | From your host                                                            | From inside compose |
| --------------------- | ------------------------------------------------------------------------- | ------------------- |
| PostgreSQL (admin)    | `postgres://orderflow:orderflow@localhost:5432/orderflow?sslmode=disable` | `postgres:5432`     |
| PostgreSQL (services) | `postgres://order_svc:order_svc@localhost:5432/orderflow?sslmode=disable`, likewise `inventory_svc` and `notification_svc` | `postgres:5432` |
| Kafka API (Redpanda)  | `localhost:19092`                                                         | `redpanda:9092`     |
| Redpanda Admin API    | `http://localhost:9644`                                                   | `redpanda:9644`     |
| Redpanda Console (UI) | [http://localhost:8090](http://localhost:8090)                            | —                   |
| Order Service HTTP    | `http://localhost:8080` (`GET /healthz`)                                  | —                   |


Redpanda advertises **two listeners** because a Kafka client first connects to a
bootstrap address, then reconnects to whatever address the broker *advertises*.
Containers need `redpanda:9092`; processes on your host need `localhost:19092`.

### Check it works

```bash
make ps                         # postgres + redpanda healthy, migrate + redpanda-init Exited (0)
make migrate-version            # 4
make topics                     # orders.events / inventory.events, 3 partitions each
make psql                       # then: SELECT * FROM inventory.products;
make psql-svc svc=order         # same query: permission denied for schema inventory
make test                       # unit tests, including the import-boundary test
```



### Make targets


| Target                                  | Does                                              |
| --------------------------------------- | ------------------------------------------------- |
| `make up` / `make down`                 | Start / stop (data kept)                          |
| `make reset`                            | Stop **and delete volumes** (wipes DB and topics) |
| `make seed`                             | Reset sample stock levels                         |
| `make psql`                             | Interactive psql shell                            |
| `make migrate-new name=add_foo`         | Create a new up/down migration pair               |
| `make migrate-up` / `make migrate-down` | Apply all / roll back one                         |
| `make topics`                           | Describe topics                                   |
| `make consume topic=orders.events`      | Tail a topic with partition, offset, key, headers |
| `make groups`                           | List consumer groups and their state              |
| `make group-lag group=<name>`           | Per-partition committed offset, high watermark, lag |
| `make logs svc=postgres`                | Tail logs                                         |
| `make run-order` / `run-inventory` / `run-notification` | Run a service on the host as its own Postgres role |
| `make build`                            | Build all three services into `bin/`              |
| `make test` / `make vet`                | Unit tests with `-race`, including the import-boundary test / `go vet` |
| `make tidy`                             | Sync `go.mod` and `go.sum` with the imports       |
| `make psql-svc svc=order`               | psql as a service role, to try its limits         |




### Sample inventory


| SKU            | Stock | Purpose                                                       |
| -------------- | ----- | ------------------------------------------------------------- |
| `SKU-KEYBOARD` | 100   | Happy path                                                    |
| `SKU-MOUSE`    | 250   | Happy path                                                    |
| `SKU-MONITOR`  | 10    | Runs out under load                                           |
| `SKU-CABLE`    | 0     | Always fails → exercises the failure path                     |
| `SKU-LIMITED`  | 1     | Concurrency test: fire N orders at once, exactly one must win |


---



## Tech stack


| Component        | Version | Why                                                        |
| ---------------- | ------- | ---------------------------------------------------------- |
| Go               | 1.26    | Services, consumers, relays                                |
| PostgreSQL       | 18.6    | Built-in `uuidv7()` gives time-ordered IDs for outbox rows (one clock for the relay's ordering) |
| Redpanda         | v26.2.2 | Kafka API-compatible, single binary, fast local startup    |
| Redpanda Console | v3.8.0  | Browse messages and consumer lag                           |
| golang-migrate   | v4.19.1 | Plain SQL up/down migrations                               |
| pgx              | v5.11.0 | Postgres driver and pool with typed SQLSTATE errors ([ADR-0011](docs/adr/0011-go-libraries-gin-uuid.md)) |
| Gin              | from Step 2b | HTTP router, middleware, and request binding with validator tags ([ADR-0011](docs/adr/0011-go-libraries-gin-uuid.md)) |
| google/uuid      | v1.6.0, from Step 2b | UUIDv7 order IDs generated by the service ([ADR-0011](docs/adr/0011-go-libraries-gin-uuid.md)) |
| franz-go         | from Step 3 | Kafka client for the outbox relay and consumers ([ADR-0011](docs/adr/0011-go-libraries-gin-uuid.md)) |


---



## Roadmap

- [x] **Step 1: Infrastructure and schema.** Docker Compose, migrations, topics, seed data.
- [x] **Step 2a: Go skeleton.** One module with `internal/platform` (config, pg, shutdown, httpserver); three services that start, check their Postgres role, and shut down cleanly; per-service Postgres roles (migration `000004`); import-boundary test.
- [ ] **Step 2b: Order Service and outbox write.** `POST /orders` writes order + outbox in one transaction; HTTP idempotency key.
- [ ] **Step 3: Outbox relay.** `internal/platform/outbox`: polling every 250 ms with `FOR UPDATE SKIP LOCKED`, batching, publish-then-mark, capped exponential backoff, parking events that can't be published, per-order ordering ([ADR-0006](docs/adr/0006-outbox-relay-polling-retries.md)); migration `000006` for retry columns.
- [ ] **Step 4: Inventory Service.** Consume `order.created`, lock rows, reserve stock, dedupe, publish the result via its own outbox.
- [ ] **Step 5: Status update and Notification Service.** Close the loop; idempotent side effects.
- [ ] **Step 6: Query endpoints.** Order status and current inventory.
- [ ] **Step 7: Failure handling.** Retries with backoff, dead-letter topics, poison messages.
- [ ] **Step 8: Concurrency lab.** Pessimistic vs optimistic locking under load; prove no overselling.
- [ ] **Step 9: Observability.** Structured logs, metrics for processed events and outbox lag.
- [ ] **Step 10: Order history.** Status-transition and event log table.

### Stretch steps

- [ ] **Stretch A: `LISTEN/NOTIFY` wake-up.** Add a doorbell trigger to the outbox relay, then compare end-to-end latency and outbox lag against polling alone (uses the Step 9 metrics).
- [ ] **Stretch B: CDC relay with Debezium.** Replace the polling relay with Debezium reading outbox inserts from the WAL. The outbox table and consumers stay unchanged.



## Repository layout

```
.
├── cmd/                   # one main package per service: wiring only
├── internal/              # service packages + internal/platform (shared mechanics)
├── go.mod, go.sum         # one Go module (ADR-0005)
├── docker-compose.yml     # postgres, migrate, redpanda, redpanda-init, console
├── Makefile               # dev workflow (make help)
├── migrations/            # golang-migrate SQL: NNNNNN_<name>.up.sql + .down.sql pairs, flat
├── db/seed/               # local-only fixtures (not migrations)
├── docs/adr/              # architecture decision records
├── docs/database-schema.md  # ER diagrams for every schema
├── docs/kafka.md            # topics, message flow, delivery semantics
├── AGENTS.md              # architecture, structure, and conventions for AI agents
└── CLAUDE.md              # Claude's pair-programming role; imports AGENTS.md
```

The Go layout and its import rules are in [AGENTS.md](AGENTS.md#project-structure) and
[ADR-0005](docs/adr/0005-single-module-shared-platform.md); `internal/archtest` enforces
them.

