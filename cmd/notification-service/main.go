// Command notification-service runs the Notification Service. main only wires
// dependencies together; behaviour lives in internal/notification.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/kritpi/outbox-order-flow/internal/notification"
	"github.com/kritpi/outbox-order-flow/internal/platform/config"
	"github.com/kritpi/outbox-order-flow/internal/platform/pg"
	"github.com/kritpi/outbox-order-flow/internal/platform/shutdown"
)

const serviceName = "notification-service"

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

	cfg, err := notification.LoadConfig(config.FromOS())
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
	if schema != notification.Schema {
		return fmt.Errorf("connected as role %q with schema %q, want schema %q: DATABASE_URL must use role notification_svc (migration 000004)", user, schema, notification.Schema)
	}
	log.Info("connected to postgres", "user", user, "schema", schema)

	g := shutdown.NewGroup(ctx, log)
	g.Go("notification", func(ctx context.Context) error {
		return notification.Run(ctx, pool, log)
	})

	err = g.Wait()
	log.Info("all components stopped")
	return err
}
