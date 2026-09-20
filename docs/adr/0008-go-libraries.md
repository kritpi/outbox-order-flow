---
status: accepted
---

# Go libraries: pgx, franz-go, and the standard library for the rest

Services talk to Postgres through `jackc/pgx/v5` and its `pgxpool`, and to Kafka through
`twmb/franz-go`. HTTP routing, logging, configuration, and shutdown use the standard library
(`net/http`, `log/slog`, `os`, `os/signal`). The mechanisms this project studies show up in
database error codes, transaction control, and producer and consumer settings, so the two
libraries are chosen to expose those directly, and everything else stays dependency-free.

pgx is pinned at v5.11.0 from Step 2a. franz-go is added when the outbox relay first
produces (Step 3), where its producer settings are their own checkpoint.

## Considered options

- **`database/sql` with a driver**: the portable interface. Rejected: it hides what this
  project needs to see. pgx returns `*pgconn.PgError` with the SQLSTATE code (a unique
  violation from a dedupe insert, a check violation from overselling, a serialization failure
  under optimistic locking), and its native pool and `pgx.Tx` avoid `database/sql`'s
  connection-affinity pitfalls.
- **`confluent-kafka-go`**: wraps librdkafka, the most widely deployed client. Rejected: cgo
  complicates builds, and behaviour is configured through librdkafka property strings rather
  than Go code you can read.
- **`segmentio/kafka-go`**: a small, friendly API. Rejected: it lacks the idempotent producer
  ADR-0006 relies on, and gives less control over offset commits than franz-go.
- **A router (chi) and a config library (envconfig, viper)**: convenient, but Go's
  `ServeMux` already matches methods and path patterns, and a few environment variables don't
  need a library.

## Consequences

- Repository code uses pgx types (`pgxpool.Pool`, `pgx.Tx`, `pgconn.PgError`) directly.
  Swapping the driver later touches every repository; accepted, since portability across
  databases isn't a goal.
- Any further dependency is a checkpoint (CLAUDE.md), weighed against writing the few lines
  it would save.
