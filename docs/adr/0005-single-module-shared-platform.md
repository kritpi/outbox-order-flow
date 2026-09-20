---
status: accepted
---

# One Go module: shared platform code, service packages isolated by import rules

The three services live in one repository and one Go module. Each is built as its own
program from `cmd/<service>`, and its business logic lives in its own package
(`internal/order`, `internal/inventory`, `internal/notification`). Plumbing that would
otherwise be identical in every service is written once in `internal/platform`: the
Postgres pool, the Kafka client, config, shutdown, and above all the outbox relay and
the idempotent consumer loop. Copying that plumbing per service would mean fixing a
delivery bug in several drifting copies. Separate modules would add `go.work` and
versioning overhead with no deployment or team boundary to justify it.

## Rules

- Imports point one way: `cmd/<svc>` → `internal/<svc>` → `internal/platform`. A service
  package never imports another service's package, and `platform` never imports a service
  package. The import-boundary test in `internal/archtest` enforces this.
  (Go's `internal/` does not prevent imports between packages in the same module.)
- `platform` provides constructors and reusable mechanics. It contains no business logic
  and no package-level state. Each service's `main` creates its own pool, Kafka client,
  and consumer group, passes them down, and closes them on shutdown.
- Services share infrastructure *servers* (one Postgres, one Redpanda) and platform
  *source code*. At runtime they share nothing: separate processes, pools, credentials,
  and consumer groups.

## Considered options

- **One Go module (or repository) per service**: services really are independent in
  dependencies and releases, but we pay for deployment and team boundaries that don't
  exist here. If the import rules hold, splitting later is a mechanical move.
- **Copy the boilerplate into each service**: no shared code, but several drifting
  copies of exactly the code this project is about.

## Consequences

- Event contracts are defined by each consumer, with no shared package
  ([ADR-0007](0007-event-contracts-consumer-defined.md)).
