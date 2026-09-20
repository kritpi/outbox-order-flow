// Package inventory is the Inventory Service: it consumes order.created, reserves stock,
// and publishes the result through its own outbox.
package inventory

import "github.com/kritpi/outbox-order-flow/internal/platform/config"

// Schema is the only Postgres schema this service may use (ADR-0002).
const Schema = "inventory"

// Config is everything the Inventory Service reads from its environment.
type Config struct {
	DatabaseURL string
	DBMaxConns  int
}

// LoadConfig reads Config from env and reports every missing or invalid variable at once.
func LoadConfig(env *config.Env) (Config, error) {
	cfg := Config{
		DatabaseURL: env.Required("DATABASE_URL"),
		DBMaxConns:  env.Int("DB_MAX_CONNS", 4),
	}
	return cfg, env.Err()
}
