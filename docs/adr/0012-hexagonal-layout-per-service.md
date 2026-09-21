---
status: accepted
---

# Hexagonal layout per service, with the transaction as an explicit port

Inside each service, code is split into packages by role. Inbound adapters (an HTTP handler,
a Kafka consumer) translate a request into a call on the service. The service holds the use
cases and business logic and declares the ports it needs as Go interfaces. The domain holds
the business types and their rules. The repository is the outbound adapter that satisfies
the service's ports with Postgres. Dependencies point inward: adapters know the core, and
the core knows no adapter, no Gin, and no pgx.

This extends [ADR-0005](0005-single-module-shared-platform.md), whose rules all still hold:
`cmd/<svc>` wires, `internal/platform` is mechanics only, and no service imports another.

## Packages

```
internal/order/
  handler/      inbound adapter: Gin routes, DTOs, error → problem+json mapping
  service/      use cases, ports (interfaces), commands, event contracts
  domain/       Order, Item, business rules, typed errors
  repository/   outbound adapter: pgx, row entities, UnitOfWork
internal/<consumer-svc>/
  consumer/     inbound adapter in place of handler/ (Steps 4 and 5)
```

The Order Service moves to this layout in Step 2b. The Inventory and Notification services
take it when Steps 4 and 5 give them code; until then they stay as their Step 2a skeletons.

| Package | May import | Must not import |
|---|---|---|
| `domain` | standard library, `google/uuid` | anything else |
| `service` | `domain`, `internal/platform` mechanics that aren't adapters | `handler`, `consumer`, `repository`, Gin, pgx |
| `handler` / `consumer` | `service`, `domain`, Gin, `platform/consumer` | `repository`, pgx |
| `repository` | `service` (to satisfy its ports), `domain`, `platform/pg`, `platform/outbox`, pgx | `handler`, `consumer`, Gin |
| `cmd/<svc>` | all of the above, to wire them | another service |

`internal/archtest` enforces this table, including the third-party rows.

## Models

Each layer converts at its own edge:

| Model | Package | Converted by |
|---|---|---|
| DTO with `json` and `binding` tags | `handler` | handler → service command |
| Command struct, e.g. `service.PlaceOrder` | `service` | service → `domain.NewOrder(id, …)`, which enforces the rules |
| Row entities with column fields | `repository` | repository ↔ domain |

The handler passes a command, not a domain object, so any other inbound adapter reaches the
same rules through the same service call.

## Where Kafka sits

Kafka is **never** a port of a service. [ADR-0003](0003-transactional-outbox.md) and
invariant 3 forbid a use case from producing to Kafka, because that is the dual-write
problem. Instead:

| Kafka role | Hexagon side | Code |
|---|---|---|
| Consuming a topic | inbound adapter, a sibling of the HTTP handler | `internal/<svc>/consumer`, on `platform/consumer` |
| A use case emitting an event | an outbox row written through the repository, in the use case's transaction | `internal/<svc>/repository`, via `platform/outbox.Insert` |
| Producing to Kafka | outside every service's hexagon | `platform/outbox` relay (Step 3) |

An outbound adapter other than the repository is something like the Notification Service's
mock sender, never a Kafka producer.

## Events

The **service** decides that an event happens and builds it. The event's wire contract is a
struct in the service package (`service/events.go`) with `json` tags matching
[docs/kafka.md](../kafka.md), next to its event-type and topic constants. The domain has no
notion of events. The repository only marshals the payload and inserts the outbox row through
`platform/outbox.Insert`, which is shared because both `outbox` tables have the same columns
and the relay depends on that shape. `platform/outbox` therefore arrives in Step 2b with the
insert, and Step 3 adds the relay beside it.

This puts the contract in the code that decides to publish, as
[ADR-0007](0007-event-contracts-consumer-defined.md) asks of publishers.

## Transactions

The transaction boundary is a port the service calls, so it is visible in the use case:

```go
err := s.uow.Do(ctx, pg.ReadCommitted, func(r service.Repos) error {
    // every repository call in here shares one pgx.Tx
})
```

- **Two levels.** `platform/pg.WithTx` holds the mechanics: begin, commit when the function
  returns nil, roll back on an error, a panic, or a cancelled context. Each service's
  `repository.UnitOfWork` wraps it and hands the function a `service.Repos` bound to that one
  transaction. The service sees only the `UnitOfWork` interface.
- **Isolation is always explicit**, READ COMMITTED unless a use case says otherwise.
  ADR-0010's replay path depends on READ COMMITTED: after `ON CONFLICT DO NOTHING`, its
  `SELECT` sees the winning row only because each statement takes a fresh snapshot. The
  Step 4 lock-strategy checkpoint may choose differently for reservations.
- **No network I/O inside the function.** No HTTP call and no Kafka produce while the
  transaction holds locks.
- **No automatic retries yet.** Retrying on serialization failure or deadlock (SQLSTATE
  `40001`, `40P01`) is settled at Step 4, where they first become possible, and Step 7.
- **Repositories exist only inside `Do`.** There is no way to get one outside a transaction,
  so an order can't be written without the chance to write its outbox row alongside it.
  Read-only queries also go through `Do`.

### POST /orders inside `Do`

The service generates the order ID (`newID`, `uuid.NewV7` in production) before the
transaction, then:

1. `Orders().Create`: `INSERT … ON CONFLICT (customer_id, idempotency_key) DO NOTHING
   RETURNING created_at`, reporting whether a row was inserted.
2. **Not inserted** (a replay, ADR-0010): `Orders().Get` the existing order and compare its
   items with the request. The same items return the existing order as a replay; different
   items return `ErrIdempotencyKeyReused`. Nothing else is written, so there's no second
   outbox row. The generated ID is discarded.
3. **Inserted**: insert the items, build the `order.created` event with
   `occurred_at = created_at`, and append it to the outbox.
4. Return; `Do` commits.

`occurred_at` comes from the row's `created_at`, the transaction's start time in Postgres, so
the event and the order agree on when it happened and the service needs no clock.

## IDs

- **Order IDs come from the service**, injected as `newID func() uuid.UUID` so tests can fix
  them. Migration `000005` drops the `DEFAULT uuidv7()` on `orders.orders.id`: an insert
  that forgets the ID fails with a NOT NULL violation instead of Postgres quietly making one.
- **Outbox event IDs stay with Postgres** (`DEFAULT uuidv7()`), for the relay's ordering
  ([ADR-0011](0011-go-libraries-gin-uuid.md)).

## Considered options

- **One package per service, a file per layer.** Fewer exported names, but the layering is a
  convention nothing checks. Rejected: the rules are the point, so the compiler and
  `archtest` should hold them.
- **A separate `port` package.** Rejected: Go interfaces belong with their consumer, which
  keeps each one minimal.
- **Fat repository methods** that own the transaction (`CreateOrderWithEvent`). Simple for
  POST /orders, but Step 4's reserve-or-fail decision would move into the repository or SQL.
- **The transaction carried in `context.Context`.** Less plumbing, but a missing transaction
  fails silently at runtime, and it hides the boundary this project exists to study.
- **A domain event serialized by the repository.** Rejected in favour of the service owning
  the event and its contract.

## Consequences

- More packages and exported names than Step 2a's flat `internal/order`, and three structs
  for one order. The conversions are small and each sits at one edge.
- Tests follow the layers: pure unit tests for `domain`, a fake `UnitOfWork` for `service`,
  `httptest` against a `gin.Engine` with a fake service for `handler`, and integration tests
  for `repository` against the real `make up` Postgres, behind the `integration` build tag
  (`make test-integration`). The service role has no DELETE grant, so each integration test
  uses a fresh random `customer_id` instead of cleaning up.
- The relay's retry-column migration becomes `000006`, since `000005` is taken by the
  order-ID default.
