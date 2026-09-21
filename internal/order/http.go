package order

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewHandler returns the Order Service's HTTP routes. POST /orders arrives in Step 2b.
func NewHandler(pool *pgxpool.Pool, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", healthz(pool, log))
	return mux
}

type pinger interface {
	Ping(ctx context.Context) error
}

// healthz reports whether this instance can reach its database. The short timeout turns
// a hung Postgres into a prompt 503 instead of a probe that never answers.
func healthz(db pinger, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			log.Warn("healthz: database unreachable", "err", err)
			http.Error(w, "database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
}
