---
status: accepted
---

# One Postgres database: one schema and one role per service

The three services share one Postgres instance and one database (`orderflow`). Each
service owns exactly one schema (`orders`, `inventory`, `notification`) and connects as
its own role, which can reach only that schema. Owning separate data is what makes them
independent services. Physically separate databases would add containers and
operational overhead on a laptop without teaching anything extra.

## Rules

- A service reads and writes only its own schema. Other services' data arrives only
  through events: no cross-schema foreign keys, joins, or queries.
- **A transaction touches exactly one service's schema.** Sharing a database makes a
  cross-schema transaction *possible*, and one would quietly remove the dual-write
  problem the outbox exists to solve ([ADR-0003](0003-transactional-outbox.md)). Design as
  if each schema were a separate database.
- Migrations run as the admin user. Services connect as `order_svc`, `inventory_svc`, and
  `notification_svc`, each with `search_path` set to its own schema.

## Considered options

- **Shared tables**: services read each other's tables. Rejected: every schema change
  couples services.
- **One database per service, one instance**: stricter, since cross-database queries
  are impossible, but more migration and connection plumbing for the same lesson.
- **One Postgres instance per service**: production-grade isolation, at the cost of
  three Postgres containers on a 4 GB Docker VM.

## Consequences

- Accepted costs: a Postgres outage takes down every service; a noisy neighbour (lock
  contention, connection exhaustion) slows the others; there is no independent scaling
  or upgrade.
- Moving a service to its own database later only changes its connection string, as
  long as the one-schema-per-transaction rule holds.
- **Not implemented yet.** Today every connection uses the `orderflow` admin user. Add
  the per-service roles (a migration plus per-service connection strings) before Step 2
  code connects to the database.
