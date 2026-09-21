package notification

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run is the service's main loop. Until the inventory.events consumer arrives in Step 5,
// it only holds the process open, so the role, pool, and shutdown wiring can be
// exercised end to end.
func Run(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	log.Info("idle until shutdown; the inventory.events consumer arrives in Step 5")
	<-ctx.Done()
	return nil
}
