// Command order-service runs the Order Service. main only wires dependencies together;
// behaviour lives in internal/order.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/kritpi/outbox-order-flow/internal/order"
	"github.com/kritpi/outbox-order-flow/internal/platform/config"
	"github.com/kritpi/outbox-order-flow/internal/platform/httpserver"
	"github.com/kritpi/outbox-order-flow/internal/platform/pg"
	"github.com/kritpi/outbox-order-flow/internal/platform/shutdown"
)

const serviceName = "order-service"

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

	cfg, err := order.LoadConfig(config.FromOS())
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	pool, err := pg.NewPool(ctx, pg.Config{URL: cfg.DatabaseURL, AppName: serviceName, MaxConns: cfg.DBMaxConns})
	if err != nil {
		return err
	}
	// Deferred after stop, so it runs before it, but only once run returns: the pool
	// closes after every component that uses it has stopped.
	defer func() {
		pool.Close()
		log.Info("database pool closed")
	}()

	user, schema, err := pg.Identity(ctx, pool)
	if err != nil {
		return err
	}
	if schema != order.Schema {
		return fmt.Errorf("connected as role %q with schema %q, want schema %q: DATABASE_URL must use role order_svc (migration 000004)", user, schema, order.Schema)
	}
	log.Info("connected to postgres", "user", user, "schema", schema)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: order.NewHandler(pool, log),
		// Bounds how long a client may take to send headers, so slow clients can't
		// hold connections open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
	}

	g := shutdown.NewGroup(ctx, log)
	g.Go("http", func(ctx context.Context) error {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		return httpserver.Run(ctx, srv, cfg.ShutdownTimeout)
	})
	// Step 3 adds the outbox relay here; Step 5 adds the inventory.events consumer.

	err = g.Wait()
	log.Info("all components stopped")
	return err
}
