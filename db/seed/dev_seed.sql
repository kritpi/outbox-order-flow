-- Dev fixtures. Kept out of migrations on purpose: migrations describe schema
-- that must exist in every environment; seed data is local-only.
-- Idempotent: safe to run repeatedly (resets stock levels to these values).

INSERT INTO inventory.products (sku, name, available) VALUES
    ('SKU-KEYBOARD', 'Mechanical Keyboard',  100),
    ('SKU-MOUSE',    'Wireless Mouse',       250),
    ('SKU-MONITOR',  '27" Monitor',           10),
    ('SKU-CABLE',    'USB-C Cable',            0),   -- always fails -> reservation-failure path
    ('SKU-LIMITED',  'Limited Edition Drop',   1)    -- concurrency test: fire N orders, exactly 1 wins
ON CONFLICT (sku) DO UPDATE
SET name       = EXCLUDED.name,
    available  = EXCLUDED.available,
    reserved   = 0,
    version    = inventory.products.version + 1,
    updated_at = now();
