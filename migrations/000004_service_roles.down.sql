-- DROP ROLE fails while a role still holds any privilege. The default-privilege entries
-- belong to orderflow, so revoke them explicitly; DROP OWNED BY then revokes the grants
-- on existing schemas and tables in this database.
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA orders
    REVOKE ALL ON TABLES FROM order_svc;
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA inventory
    REVOKE ALL ON TABLES FROM inventory_svc;
ALTER DEFAULT PRIVILEGES FOR ROLE orderflow IN SCHEMA notification
    REVOKE ALL ON TABLES FROM notification_svc;

DROP OWNED BY order_svc, inventory_svc, notification_svc;
DROP ROLE order_svc, inventory_svc, notification_svc;
