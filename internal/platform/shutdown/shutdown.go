// Package shutdown ties a service's long-lived components to one lifetime. They start
// together, and when a signal arrives or any one of them exits, all are told to stop.
package shutdown

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// SignalContext returns a context that is cancelled on the first SIGINT or SIGTERM.
// After that signal, default handling is restored, so a second Ctrl+C kills a shutdown
// that hangs instead of being swallowed. Call stop to release the handler.
func SignalContext(parent context.Context, log *slog.Logger) (ctx context.Context, stop context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-sigc:
			signal.Stop(sigc)
			log.Info("shutdown signal received", "signal", sig.String())
		case <-ctx.Done():
			signal.Stop(sigc)
		}
		cancel()
	}()
	return ctx, cancel
}

// Group runs a service's long-lived components (the HTTP server now; the outbox relay
// and consumers later) under one context. It is errgroup.WithContext rebuilt on the
// standard library, plus component names in the logs.
//
// Any component exiting, with or without an error, stops all of them. A service whose
// consumer has died but whose HTTP server still answers looks healthy while doing
// nothing; exiting lets the supervisor see the failure and restart it.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    *slog.Logger
	wg     sync.WaitGroup
	once   sync.Once
	err    error
}

// NewGroup returns a Group whose components stop when ctx is cancelled.
func NewGroup(ctx context.Context, log *slog.Logger) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{ctx: ctx, cancel: cancel, log: log}
}

// Go runs fn in its own goroutine. fn must return promptly once its context is done.
func (g *Group) Go(name string, fn func(ctx context.Context) error) {
	g.wg.Go(func() {
		defer g.cancel()
		if err := fn(g.ctx); err != nil {
			g.log.Error("component failed", "component", name, "err", err)
			g.once.Do(func() { g.err = fmt.Errorf("%s: %w", name, err) })
			return
		}
		g.log.Info("component stopped", "component", name)
	})
}

// Wait blocks until every component has returned, then reports the first failure.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}
