---
status: accepted
---

# Kafka protocol for events, served by Redpanda locally

Services communicate through a Kafka-API broker rather than Google Pub/Sub or direct
HTTP calls. Per-key partition ordering, consumer groups, and explicit offset commits are
the broker semantics this project exists to learn. Locally the broker is Redpanda: it
speaks the Kafka protocol from a single binary, with no ZooKeeper or KRaft setup, and
starts in seconds.

## Considered options

- **Google Pub/Sub**: per-message acknowledgement and ordering keys instead of
  partitions and offsets. That's a different delivery model from the one this project
  studies.
- **Apache Kafka (KRaft)**: the reference implementation, with the same API but a heavier
  container and slower startup. Switching to it later only changes configuration.
- **Synchronous HTTP between services**: couples availability and removes the event log.
  This is the design the project exists to move away from.

## Consequences

- Go code uses only the Kafka API and no Redpanda-specific features, so the broker stays
  swappable.
- Topics are declared explicitly in `redpanda-init` (`docker-compose.yml`) and broker
  auto-creation is off, so producing to a misspelled topic fails instead of creating one.
