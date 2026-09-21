-- One Postgres role per service (ADR-0002). Migrations keep running as the `orderflow`
-- admin; each service connects as its own role, which can reach only its own schema.
-- A cross-schema query from service code then fails with "permission denied" instead of
-- quietly working because every connection happened to be the admin.
--
-- Dev-only credentials: each password equals the role name. A real deployment creates
-- roles and secrets out-of-band, never in a committed migration.

CREATE ROLE order_svc        LOGIN PASSWORD 'order_svc';
CREATE ROLE inventory_svc    LOGIN PASSWORD 'inventory_svc';
CREATE ROLE notification_svc LOGIN PASSWORD 'notification_svc';

-- search_path = the service's own schema only. An unqualified table name resolves there
-- or fails; it can never land in `public` or another service's schema.
ALTER ROLE order_svc        SET search_path = orders;
ALTER ROLE inventory_svc    SET search_path = inventory;
ALTER ROLE notification_svc SET search_path = notification;

-- No role gets CREATE: DDL stays with migrations. No role gets DELETE: nothing deletes
-- rows yet, and the first need (e.g. outbox or processed_events retention) grants it in
-- its own migration.
--
-- GRANT ... ON ALL TABLES covers only tables that exist now. ALTER DEFAULT PRIVILEGES
-- extends the same grant to tables that later migrations (run as orderflow) create, so a
-- new table doesn't start out unreachable by its own service.

-- ---------------------------------------------------------------------------
-- order_svc -> orders
-- UPDATE: status transitions on orders, and the relay marking outbox rows published.
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA orders TO order_svc;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA orders TO order_svc;
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA orders
    GRANT SELECT, INSERT, UPDATE ON TABLES TO order_svc;

-- ---------------------------------------------------------------------------
-- inventory_svc -> inventory
-- UPDATE: stock levels on products, reservation status, and outbox marking.
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA inventory TO inventory_svc;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA inventory TO inventory_svc;
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA inventory
    GRANT SELECT, INSERT, UPDATE ON TABLES TO inventory_svc;

-- ---------------------------------------------------------------------------
-- notification_svc -> notification
-- No UPDATE: the notification log is append-only. Its dedupe is
-- INSERT ... ON CONFLICT (event_id, channel) DO NOTHING, which needs only INSERT
-- (DO UPDATE would need UPDATE).
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA notification TO notification_svc;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA notification TO notification_svc;
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA notification
    GRANT SELECT, INSERT ON TABLES TO notification_svc;
