// Package order is the Order Service: the HTTP API that accepts orders, and (Step 5) the
// consumer that updates order status from inventory results.
package order

import (
	"time"

	"github.com/kritpi/outbox-order-flow/internal/platform/config"
)

// Schema is the only Postgres schema this service may use (ADR-0002).
const Schema = "orders"

// Config is everything the Order Service reads from its environment.
type Config struct {
	DatabaseURL     string
	DBMaxConns      int
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

// LoadConfig reads Config from env and reports every missing or invalid variable at once.
func LoadConfig(env *config.Env) (Config, error) {
	cfg := Config{
		DatabaseURL:     env.Required("DATABASE_URL"),
		DBMaxConns:      env.Int("DB_MAX_CONNS", 4),
		HTTPAddr:        env.String("HTTP_ADDR", ":8080"),
		ShutdownTimeout: env.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	return cfg, env.Err()
}
