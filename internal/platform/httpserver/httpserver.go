// Package httpserver runs an http.Server for exactly as long as a context lives.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Run listens on srv.Addr and serves until ctx is done. Listening before serving makes
// a port conflict a synchronous startup error rather than a failure from a goroutine.
func Run(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration) error {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", srv.Addr, err)
	}
	return Serve(ctx, srv, ln, shutdownTimeout)
}

// Serve serves on ln until ctx is done, then shuts down gracefully. Shutdown closes the
// listener at once, so no new requests are accepted, and gives in-flight requests up to
// shutdownTimeout to finish. Requests still running after that are cut off.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		// Serve returned before shutdown was requested: the listener failed.
		return fmt.Errorf("httpserver: serve: %w", err)
	case <-ctx.Done():
	}

	// ctx is already cancelled, so the shutdown deadline needs a context of its own.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	// Serve returns ErrServerClosed as soon as Shutdown begins.
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("httpserver: serve: %w", err)
	}
	return nil
}
