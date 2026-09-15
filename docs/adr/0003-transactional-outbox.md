---
status: accepted
---

# Publish events through a transactional outbox

A service that changes state and announces it cannot commit to Postgres and produce to
Kafka atomically. Whichever write happens second can fail, leaving either a committed
order with no event or an event for an order that rolled back (the dual-write problem).
So each publishing service inserts its event into its own `outbox` table in the **same
transaction** as the state change. A separate relay then produces unpublished rows to
Kafka and marks them published.

## Considered options

- **Produce to Kafka inside or after the DB transaction**: this is the dual-write
  problem itself.
- **Kafka transactions**: atomic across Kafka writes only; they cannot include a
  Postgres commit.
- **Change data capture (Debezium reading the WAL)**: no polling, but it adds Kafka
  Connect infrastructure and hides the mechanism this project wants to build and study.
- **Postgres `LISTEN/NOTIFY`**: notifications are not queued for disconnected listeners,
  so events are lost whenever the listener is down.

## Consequences

- Delivery is **at-least-once**. A relay crash between publishing and marking republishes
  those rows, so consumers must be idempotent ([ADR-0004](0004-idempotent-consumers.md)).
- The event ID is the outbox row's `uuidv7` id. The Kafka message key is `aggregate_id`
  (the order ID), so one order's events stay ordered within a partition.
- Events lag by up to the relay's poll interval, so order status is eventually
  consistent.
- Published rows accumulate and eventually need a cleanup job. Until then, the partial
  index on unpublished rows keeps relay polling cheap.
- The relay itself (batching, `FOR UPDATE SKIP LOCKED`, several relay instances vs
  per-order ordering) is the Step 3 checkpoint.
