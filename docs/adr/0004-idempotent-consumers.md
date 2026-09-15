---
status: accepted
---

# Idempotent consumers: dedupe in the side-effect transaction, then commit the offset

Both the outbox relay and Kafka consumer groups deliver at-least-once, so every
consumer will eventually see duplicates. A consumer records the event's ID in its own
schema **in the same transaction as the side effect**:
- the Order and Inventory services insert `(consumer, event_id)` into `processed_events`;
- the Notification Service relies on `UNIQUE (event_id, channel)` on its notification log.

The consumer commits the Kafka offset only after that transaction commits. A redelivered
event hits the unique key, the handler skips it, and the offset still advances.

## Considered options

- **Kafka exactly-once semantics** (transactional consume-and-produce): only covers
  read-process-write inside Kafka. The side effects here are Postgres writes.
- **Natural-key idempotency only** (for example the `(order_id, sku)` primary key on
  `inventory.reservations`): works per handler, but has to be re-derived for every event
  type. It stays as a second guard, not the mechanism.
- **Commit the offset before processing**: this is at-most-once, and a crash loses the
  event.

## Consequences

- The guarantee is effectively-once *effects*, not exactly-once delivery. A crash after
  the DB commit but before the offset commit redelivers the event, and the dedupe key
  turns the retry into a no-op.
- Effects outside Postgres (a real email or SMS) cannot join the transaction and remain
  at-least-once. How the Notification Service orders "record" vs "send" is the Step 5
  checkpoint.
- `processed_events` grows without bound. Its retention must outlast the longest
  possible redelivery window.
