-- Order Service owns the `orders` schema. No other service writes here.

CREATE SCHEMA orders;

-- ---------------------------------------------------------------------------
-- Aggregate: orders
-- ---------------------------------------------------------------------------
CREATE TABLE orders.orders (
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),
    customer_id     uuid        NOT NULL,
    -- text + CHECK instead of a PG enum: enums can't drop values and are awkward
    -- to evolve inside migrations; a CHECK is a one-line ALTER.
    status          text        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'reserved', 'failed', 'cancelled')),
    failure_reason  text,
    -- Client-supplied Idempotency-Key header for POST /orders. Protects against
    -- *HTTP-level* retries creating duplicate orders (distinct from consumer idempotency).
    idempotency_key text,
    -- Bumped on every status transition; lets us do optimistic "UPDATE ... WHERE version = $n".
    version         integer     NOT NULL DEFAULT 1,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX orders_customer_idempotency_key_uq
    ON orders.orders (customer_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX orders_customer_created_idx
    ON orders.orders (customer_id, created_at DESC);

CREATE TABLE orders.order_items (
    order_id uuid    NOT NULL REFERENCES orders.orders (id) ON DELETE CASCADE,
    sku      text    NOT NULL,           -- no FK: inventory lives in another service's schema
    quantity integer NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, sku)
);

-- ---------------------------------------------------------------------------
-- Transactional outbox: written in the SAME transaction as orders/order_items.
-- A relay (Step 3) polls unpublished rows and produces them to Kafka.
-- ---------------------------------------------------------------------------
CREATE TABLE orders.outbox (
    -- Doubles as the event ID that consumers dedupe on. uuidv7 is time-ordered,
    -- so ORDER BY id ~= insertion order (but NOT commit order -- see Step 3).
    id             uuid        PRIMARY KEY DEFAULT uuidv7(),
    aggregate_type text        NOT NULL,          -- 'order'
    aggregate_id   uuid        NOT NULL,          -- used as the Kafka message key -> partition
    event_type     text        NOT NULL,          -- 'order.created'
    topic          text        NOT NULL,          -- 'orders.events'
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- trace ids, etc.
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,                   -- NULL = still owed to the broker
    attempts       integer     NOT NULL DEFAULT 0,
    last_error     text
);

-- Partial index: stays tiny no matter how much published history accumulates,
-- so the relay's "WHERE published_at IS NULL ORDER BY id LIMIT n" stays O(batch).
CREATE INDEX outbox_unpublished_idx
    ON orders.outbox (id)
    WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Inbox / dedupe table for events this service consumes (inventory results, if the
-- Order Service owns status updates: AGENTS.md open decision #1).
-- Insert (consumer, event_id) in the same tx as the side effect; a PK conflict
-- means "already processed" -> skip and commit the offset.
-- ---------------------------------------------------------------------------
CREATE TABLE orders.processed_events (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
