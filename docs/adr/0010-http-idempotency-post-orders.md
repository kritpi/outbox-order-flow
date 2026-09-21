---
status: accepted
---

# HTTP idempotency for POST /orders: a required key, detected by the unique index

`POST /orders` requires an `Idempotency-Key` header, so a client that retries after a
timeout gets back the order from its first attempt instead of a second order. The key is
stored on `orders.orders`, scoped per customer by the partial unique index on
`(customer_id, idempotency_key)` from migration `000001`. This is HTTP-level idempotency.
It is separate from consumer dedupe on event IDs ([ADR-0004](0004-idempotent-consumers.md)).

## Behaviour

| Request | Response |
|---|---|
| No `Idempotency-Key` header | `400` |
| New key | `201 Created`, `Location: /orders/{id}`, the order as the body |
| Known key, same items | `200 OK`, the same `Location` and body shape, and `Idempotent-Replayed: true` |
| Known key, different items | `422` |

Errors use RFC 9457 `application/problem+json`. A replay returns the order **as it is
now**, not a stored copy of the first response. By the time a retry arrives, its status may
have moved on from `pending`.

## Mechanism

The handler inserts the order with
`INSERT … ON CONFLICT (customer_id, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING RETURNING id`.

- **A row comes back:** insert `order_items` and the outbox row, commit, and return `201`.
- **No row comes back:** the key exists. `SELECT` that order and its items in the same
  transaction, and write nothing else. Under READ COMMITTED each statement takes a fresh
  snapshot, so the `SELECT` sees the row that won. Because the replay path never writes,
  one accepted order produces exactly one `order.created`.
- **Two requests with the same key at once:** the second waits on the unique index entry
  until the first transaction ends. If the first commits, the second takes the replay
  path. If it rolls back, the second inserts. So there is no "request still in progress"
  response.

**Same key, different body** is detected by comparing the request with the stored order,
without a stored hash. `customer_id` is part of the key's scope. The items are compared as
sets of `(sku, quantity)`. This is exact because every field of the request is stored:
unknown JSON fields and a SKU listed twice are both rejected before the database.

## Considered options

- **Optional key** (a missing key means no protection, which the partial index already
  allows): a client that forgets the header and retries after a timeout creates two
  orders, which is the bug this exists to prevent.
- **Plain `INSERT`, then catch unique violation `23505`**: race-free, but the error aborts
  the Postgres transaction, so re-reading the existing order needs a second transaction.
- **`SELECT`, then `INSERT`**: two concurrent requests can both see no row. One of them
  then hits `23505` anyway.
- **Store a hash of the canonicalised request** (new migration): it covers request fields
  that are never stored, but needs canonicalisation code and a migration for no benefit
  while every request field is stored. It becomes the right choice the day the request
  gains one.
- **`201` on a replay**: the client code only needs one success branch, but it says
  "created" for a request that created nothing.
- **Expire keys after a period** (Stripe uses 24 hours): needs a cleanup job, and gives
  nothing at this scale.

## Consequences

- **Keys never expire.** A key is stored on its order row, so it lasts as long as the order.
  Reusing it much later returns the old order, or a `422`.
- **The comparison depends on storing every request field.** A request field that isn't
  persisted (a delivery note, say) must be added to the comparison, or the design must move
  to a stored hash in a new migration. The handler carries a comment saying so.
- **The scope is per customer, and there is no auth yet.** `customer_id` comes from the
  request body, so the same key with a different `customer_id` creates a new order.
- `idempotency_key` is always non-NULL from the API. The index's `WHERE … IS NOT NULL`
  clause now only covers rows written some other way.
