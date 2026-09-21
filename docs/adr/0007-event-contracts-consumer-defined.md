---
status: accepted
---

# Event contracts: each consumer defines the fields it reads

A publisher defines the event it writes, next to the code that inserts the outbox row. Each
consumer declares its own struct with only the fields it reads, decodes the payload into it,
and ignores everything else. There is no shared event package. [ADR-0005](0005-single-module-shared-platform.md)
forbids imports between services, and a shared contracts package would be the one
exception: every service would compile against every other service's event shapes, and
splitting a service out later would mean versioning that package.

## Rules

- A publisher owns its event's name and payload shape, and changes them additively. Adding
  a field is safe. Renaming or removing a field, or changing its meaning, is a breaking change
  and gets a new event type.
- A consumer decodes only the fields it uses and ignores unknown ones (a tolerant reader). A
  payload missing a field it needs is a poison message, not a panic.
- [docs/kafka.md](../kafka.md) documents each event's payload; that page is the contract
  both sides are written against.
- The import-boundary test (`internal/archtest`) allows no shared package under `internal/`.

## Considered options

- **Shared `internal/events` package**: renames fail at compile time, and there is one
  definition to read. Rejected: it is an import edge between every pair of services, a change
  to one event rebuilds every consumer at once, and it breaks ADR-0005's promise that splitting
  a service out is a mechanical move.
- **Schema registry with Avro or Protobuf**: compatibility is checked by tooling instead of
  by discipline. Rejected for now: another container and a code-generation step for three
  services and a handful of events. It is the upgrade path if contracts start to drift.

## Consequences

- Nothing checks at compile time that a publisher and its consumers agree. Drift surfaces
  in a consumer's tests, which should decode an example payload from docs/kafka.md, or at
  runtime as a poison message.
- Similar structs in several services are intentional, not duplication to clean up.
