-- Inventory Service owns the `inventory` schema.

CREATE SCHEMA inventory;

CREATE TABLE inventory.products (
    sku        text        PRIMARY KEY,
    name       text        NOT NULL,
    -- Units that can still be promised to new orders.
    -- The CHECK is the last line of defense: even if app-level locking is buggy,
    -- Postgres will reject an UPDATE that would oversell.
    available  integer     NOT NULL CHECK (available >= 0),
    -- Units held by reservations that aren't shipped/released yet.
    reserved   integer     NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    -- Unused by pessimistic locking (SELECT ... FOR UPDATE); present so we can
    -- implement and compare optimistic locking later without a migration.
    version    bigint      NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (order, sku) that inventory has reserved.
-- order_id is deliberately NOT a foreign key: inventory only knows about orders
-- through events, never through the orders schema.
CREATE TABLE inventory.reservations (
    order_id   uuid        NOT NULL,
    sku        text        NOT NULL REFERENCES inventory.products (sku),
    quantity   integer     NOT NULL CHECK (quantity > 0),
    status     text        NOT NULL DEFAULT 'reserved'
                           CHECK (status IN ('reserved', 'released', 'committed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, sku)
);

-- Same shape as orders.outbox -- each publishing service owns its own outbox,
-- because the outbox row must commit atomically with that service's state change.
CREATE TABLE inventory.outbox (
    id             uuid        PRIMARY KEY DEFAULT uuidv7(),
    aggregate_type text        NOT NULL,
    aggregate_id   uuid        NOT NULL,
    event_type     text        NOT NULL,
    topic          text        NOT NULL,
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    attempts       integer     NOT NULL DEFAULT 0,
    last_error     text
);

CREATE INDEX outbox_unpublished_idx
    ON inventory.outbox (id)
    WHERE published_at IS NULL;

CREATE TABLE inventory.processed_events (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
