// Package pg builds pgx connection pools.
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config describes one service's pool. Every service builds its own pool with its own
// role; pools are never shared between services (ADR-0005).
type Config struct {
	URL string
	// AppName is sent as application_name, so pg_stat_activity shows which service
	// holds each connection on the shared Postgres instance.
	AppName string
	// MaxConns caps the pool; zero keeps pgx's default.
	MaxConns int
	// PingTimeout bounds the startup ping; zero means 5 s.
	PingTimeout time.Duration
}

// NewPool creates the pool and pings it once. pgxpool connects lazily, so without the
// ping a wrong password or host would surface on the first request instead of at
// startup.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		// pgx redacts the password in parse errors, so the URL is safe to log.
		return nil, fmt.Errorf("pg: parse config: %w", err)
	}
	if cfg.AppName != "" {
		pcfg.ConnConfig.RuntimeParams["application_name"] = cfg.AppName
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = int32(cfg.MaxConns)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pg: create pool: %w", err)
	}

	timeout := cfg.PingTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// Identity reports the role and schema the pool's connections run as. Services check it
// at startup to prove their per-service role and search_path (ADR-0002) took effect.
// schema is empty when the role cannot use any schema on its search_path.
func Identity(ctx context.Context, pool *pgxpool.Pool) (user, schema string, err error) {
	err = pool.QueryRow(ctx, "SELECT current_user, coalesce(current_schema(), '')").Scan(&user, &schema)
	if err != nil {
		return "", "", fmt.Errorf("pg: identity: %w", err)
	}
	return user, schema, nil
}
