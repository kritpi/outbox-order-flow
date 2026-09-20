package inventory

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run is the service's main loop. Until the orders.events consumer arrives in Step 4, it
// only holds the process open, so the role, pool, and shutdown wiring can be exercised
// end to end.
func Run(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	log.Info("idle until shutdown; the orders.events consumer arrives in Step 4")
	<-ctx.Done()
	return nil
}
