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

One Postgres database with **one schema per service**, and (from Step 2) one Postgres
role per service that can reach only its own schema. A service reads and writes only
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

One topic per publishing aggregate. The event type travels in a message header, and
the message **key is the order ID**, so every event for one order lands on the same
partition in order. This layout is implemented but still to be confirmed
([open decisions](AGENTS.md#open-decisions)). How messages flow, what each delivery
guarantee means, and how failures play out: [docs/kafka.md](docs/kafka.md).


| Topic              | Partitions | Event types                                                                  |
| ------------------ | ---------- | ---------------------------------------------------------------------------- |
| `orders.events`    | 3          | `order.created`                                                              |
| `inventory.events` | 3          | stock reserved / reservation failed (names settled at the Step 2 checkpoint) |


Topic auto-creation is **disabled**: producing to a misspelled topic fails loudly
instead of silently creating a new one.

### Database schema

ER diagrams with every column, key, constraint, and cross-service reference:
[docs/database-schema.md](docs/database-schema.md). Summary:

```
orders.orders              id (uuidv7), customer_id, status, failure_reason, idempotency_key, version, timestamps
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

Decisions still open are listed in [AGENTS.md](AGENTS.md#open-decisions).

---



## Getting started



### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Go 1.26+ (from Step 2 onwards)
- `make`



### Run the infrastructure

```bash
make up      # start Postgres + Redpanda, apply migrations, create topics
make seed    # load sample inventory (safe to re-run)
```

`make up` waits until the one-shot `migrate` and `redpanda-init` containers exit
successfully.

### Endpoints


| What                  | From your host                                                            | From inside compose |
| --------------------- | ------------------------------------------------------------------------- | ------------------- |
| PostgreSQL            | `postgres://orderflow:orderflow@localhost:5432/orderflow?sslmode=disable` | `postgres:5432`     |
| Kafka API (Redpanda)  | `localhost:19092`                                                         | `redpanda:9092`     |
| Redpanda Admin API    | `http://localhost:9644`                                                   | `redpanda:9644`     |
| Redpanda Console (UI) | [http://localhost:8090](http://localhost:8090)                            | —                   |


Redpanda advertises **two listeners** because a Kafka client first connects to a
bootstrap address, then reconnects to whatever address the broker *advertises*.
Containers need `redpanda:9092`; processes on your host need `localhost:19092`.

### Check it works

```bash
make ps                         # postgres + redpanda healthy, migrate + redpanda-init Exited (0)
make migrate-version            # 3
make topics                     # orders.events / inventory.events, 3 partitions each
make psql                       # then: SELECT * FROM inventory.products;
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
| PostgreSQL       | 18.6    | Built-in `uuidv7()` gives time-ordered IDs for outbox rows |
| Redpanda         | v26.2.2 | Kafka API-compatible, single binary, fast local startup    |
| Redpanda Console | v3.8.0  | Browse messages and consumer lag                           |
| golang-migrate   | v4.19.1 | Plain SQL up/down migrations                               |


---



## Roadmap

- [x] **Step 1: Infrastructure and schema.** Docker Compose, migrations, topics, seed data.
- [ ] **Step 2: Order Service and outbox write.** Per-service Postgres roles; Go module skeleton with the import-boundary test; `POST /orders` writes order + outbox in one transaction; HTTP idempotency key.
- [ ] **Step 3: Outbox relay.** `internal/platform/outbox`: polling with `FOR UPDATE SKIP LOCKED`, batching, publish-then-mark, ordering guarantees.
- [ ] **Step 4: Inventory Service.** Consume `order.created`, lock rows, reserve stock, dedupe, publish the result via its own outbox.
- [ ] **Step 5: Status update and Notification Service.** Close the loop; idempotent side effects.
- [ ] **Step 6: Query endpoints.** Order status and current inventory.
- [ ] **Step 7: Failure handling.** Retries with backoff, dead-letter topics, poison messages.
- [ ] **Step 8: Concurrency lab.** Pessimistic vs optimistic locking under load; prove no overselling.
- [ ] **Step 9: Observability.** Structured logs, metrics for processed events and outbox lag.
- [ ] **Step 10: Order history.** Status-transition and event log table.



## Repository layout

```
.
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

Go code arrives in Step 2 as `cmd/<service>/` plus
`internal/{order,inventory,notification,platform}/`. The full layout and import rules
are in [AGENTS.md](AGENTS.md#project-structure) and
[ADR-0005](docs/adr/0005-single-module-shared-platform.md).

