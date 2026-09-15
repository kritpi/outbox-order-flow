---
status: accepted
---

# Outbox relay: polling with backoff retries and per-order ordering

Each publishing service runs a relay that polls its own outbox table; nothing in the
request path publishes to Kafka. The relay wakes every 250 ms (configurable), and also
immediately while batches keep coming back full or when the earliest retry falls due. It
claims due, unpublished rows in `id` order, produces them, and marks them published.
Polling alone keeps a single publish path and nothing coupling the relay back to request
transactions. At this project's load its latency cost is a few hundred milliseconds on a
flow that is eventually consistent anyway.

## Retry policy

- **Two layers.** The Kafka client retries brief broker errors within one produce call.
  The idempotent producer means those retries neither duplicate nor reorder messages. The
  relay retries rows the client gave up on, across polls.
- A failed row gets `attempts + 1`, `last_error`, and
  `next_attempt_at = now() + min(60 s, 1 s × 2^attempts)` plus random jitter.
- **Transient errors** (broker unavailable, timeouts, leader changes) retry indefinitely.
  **Errors retrying can't fix** (a message over `max.message.bytes`, a payload that can't
  be serialised) **park** the row: no more retries, and an alert. How each client error
  maps to these two classes is settled along with the Go client at Step 3.
- **Per-order ordering beats throughput.** The relay never publishes an event while an
  earlier event for the same `aggregate_id` is still unpublished, backing off, or parked.
  A stuck row holds back only that order's later events; other orders keep flowing.

## Considered options

- **Publish from the request path, poll as a fallback**: lowest latency, but two publish
  paths. One order's events can go out of order when the first attempt fails, duplicates
  become routine, and API latency depends on the broker.
- **Polling plus a `LISTEN/NOTIFY` wake-up**: near-instant publishing with one path.
  Rejected for now because a stuck listener can fill Postgres's notification queue and
  make order transactions fail, and each relay needs a dedicated connection. It can be
  added without changing the relay's core, so it's a measured stretch step.
- **CDC with Debezium**: captures in commit order with no polling, but needs Kafka
  Connect and a replication slot that can fill the database disk if the connector stops.
  It's a stretch step that swaps the relay and keeps the outbox table.
- **Keep flowing past a stuck row**: better availability, but consumers can see an
  order's later event before its earlier one.

## Consequences

- Needs migration `000004`: `next_attempt_at` and a parked marker on both outbox tables,
  with the partial index covering due, unpublished rows.
- **Outbox lag** (age of the oldest unpublished row) and the **parked-row count** are the
  relay's health signals. A parked row holds its order until a person fixes it.
- Delivery stays at-least-once. A crash between producing and marking publishes the rows
  again ([ADR-0004](0004-idempotent-consumers.md)).
- Left for the Step 3 checkpoint: batch size, whether row locks are held while producing
  or replaced by a lease, and running several relay instances without breaking
  per-order ordering.
