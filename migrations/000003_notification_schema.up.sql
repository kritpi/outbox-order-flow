-- Notification Service owns the `notification` schema.
-- It only consumes events and performs side effects; it publishes nothing,
-- so it has no outbox.

CREATE SCHEMA notification;

-- A log of (mock) notifications sent. The UNIQUE (event_id, channel) constraint
-- is the idempotency guard: a redelivered event can't send the same email twice.
CREATE TABLE notification.notifications (
    id         uuid        PRIMARY KEY DEFAULT uuidv7(),
    event_id   uuid        NOT NULL,
    order_id   uuid        NOT NULL,
    channel    text        NOT NULL CHECK (channel IN ('email', 'sms', 'log')),
    template   text        NOT NULL,
    payload    jsonb       NOT NULL,
    sent_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, channel)
);

CREATE INDEX notifications_order_idx ON notification.notifications (order_id);
