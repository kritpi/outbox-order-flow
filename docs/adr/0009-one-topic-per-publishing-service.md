---
status: accepted
---

# One topic per publishing service, event type in a header

Each publishing service produces every event it emits to a single topic:
`orders.events` for the Order Service and `inventory.events` for the Inventory Service.
The event type travels in an `event_type` message header, and the key is the order ID
([ADR-0003](0003-transactional-outbox.md)). Kafka orders messages only within one partition
of one topic. Keeping all of an order's events in one topic is what puts them on one
partition, so a consumer sees `order.created` before a later `order.cancelled` for the same
order. That per-order ordering is what the relay protects ([ADR-0006](0006-outbox-relay-polling-retries.md))
and what architecture invariant 8 requires.

## Considered options

- **One topic per event type** (`orders.created`, `orders.cancelled`, …): a consumer
  subscribes to exactly the types it handles, and each type can have its own retention.
  Rejected because two events for the same order would sit in different topics, with no
  order between them. A consumer would have to rebuild per-order ordering itself (buffer,
  sequence numbers), which is the problem the key was chosen to avoid.

## Consequences

- A consumer receives every event type on the topics it subscribes to, and skips the ones
  it doesn't handle by reading the `event_type` header before it decodes the payload.
  Skipping an event still counts as processing it: the offset advances.
- Retention and partition count are per topic, so every event type a service publishes
  shares them.
- The relay decides where a row goes from `outbox.topic`. Handlers write the topic name
  of their own service, never a per-type name.
- Reversing this means new topics, moving every consumer, and replaying history, so it
  gets a superseding ADR rather than an edit.
