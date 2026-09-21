package httpserver_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kritpi/outbox-order-flow/internal/platform/httpserver"
)

// startServer serves handler on a free port and returns its base URL, a cancel func that
// requests shutdown, and a channel with Serve's result.
func startServer(t *testing.T, handler http.Handler, timeout time.Duration) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- httpserver.Serve(ctx, &http.Server{Handler: handler}, ln, timeout) }()
	return "http://" + ln.Addr().String(), cancel, done
}

func TestServeDrainsInFlightRequestBeforeReturning(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	url, cancel, done := startServer(t, handler, 5*time.Second)

	status := make(chan int, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			status <- -1
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()

	<-started
	cancel() // shutdown is requested while the request is still running

	select {
	case err := <-done:
		t.Fatalf("Serve returned (%v) before the in-flight request finished", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if got := <-status; got != http.StatusNoContent {
		t.Errorf("in-flight request status = %d, want 204", got)
	}
	if err := <-done; err != nil {
		t.Errorf("Serve = %v, want nil after a clean drain", err)
	}
}

func TestServeCutsOffRequestsThatOutliveTheTimeout(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
	})
	url, cancel, done := startServer(t, handler, 50*time.Millisecond)

	go func() {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
		}
	}()

	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Serve = %v, want a shutdown deadline error", err)
	}
}
