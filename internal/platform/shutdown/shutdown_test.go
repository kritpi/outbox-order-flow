package shutdown_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/kritpi/outbox-order-flow/internal/platform/shutdown"
)

var discard = slog.New(slog.DiscardHandler)

func TestGroupFailureStopsOtherComponents(t *testing.T) {
	g := shutdown.NewGroup(context.Background(), discard)

	waiterStopped := make(chan struct{})
	g.Go("waiter", func(ctx context.Context) error {
		<-ctx.Done()
		close(waiterStopped)
		return nil
	})
	boom := errors.New("boom")
	g.Go("failer", func(context.Context) error { return boom })

	if err := g.Wait(); !errors.Is(err, boom) {
		t.Fatalf("Wait = %v, want the failer's error", err)
	}
	select {
	case <-waiterStopped:
	default:
		t.Fatal("waiter still running after Wait returned")
	}
}

func TestGroupParentCancelStopsEveryComponent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := shutdown.NewGroup(ctx, discard)
	for _, name := range []string{"http", "relay"} {
		g.Go(name, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
	}

	cancel()
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait = %v, want nil for a clean shutdown", err)
	}
}

func TestSignalContextCancelsOnSIGTERM(t *testing.T) {
	ctx, stop := shutdown.SignalContext(context.Background(), discard)
	defer stop()

	// SignalContext registered its handler before returning, so this SIGTERM is
	// caught rather than killing the test binary.
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("context not cancelled by SIGTERM")
	}
}
