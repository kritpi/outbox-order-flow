// Command inventory-service runs the Inventory Service. main only wires dependencies
// together; behaviour lives in internal/inventory.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/kritpi/outbox-order-flow/internal/inventory"
	"github.com/kritpi/outbox-order-flow/internal/platform/config"
	"github.com/kritpi/outbox-order-flow/internal/platform/pg"
	"github.com/kritpi/outbox-order-flow/internal/platform/shutdown"
)

const serviceName = "inventory-service"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("service", serviceName)
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := shutdown.SignalContext(context.Background(), log)
	defer stop()

	cfg, err := inventory.LoadConfig(config.FromOS())
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	pool, err := pg.NewPool(ctx, pg.Config{URL: cfg.DatabaseURL, AppName: serviceName, MaxConns: cfg.DBMaxConns})
	if err != nil {
		return err
	}
	// Runs once run returns: the pool closes after every component has stopped.
	defer func() {
		pool.Close()
		log.Info("database pool closed")
	}()

	user, schema, err := pg.Identity(ctx, pool)
	if err != nil {
		return err
	}
	if schema != inventory.Schema {
		return fmt.Errorf("connected as role %q with schema %q, want schema %q: DATABASE_URL must use role inventory_svc (migration 000004)", user, schema, inventory.Schema)
	}
	log.Info("connected to postgres", "user", user, "schema", schema)

	g := shutdown.NewGroup(ctx, log)
	g.Go("inventory", func(ctx context.Context) error {
		return inventory.Run(ctx, pool, log)
	})
	// Step 3 adds the outbox relay here.

	err = g.Wait()
	log.Info("all components stopped")
	return err
}
