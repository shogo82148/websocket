package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type cleanupTrackingRWC struct {
	closed chan struct{}
	count  atomic.Int32
}

func newCleanupTrackingRWC() *cleanupTrackingRWC {
	return &cleanupTrackingRWC{closed: make(chan struct{})}
}

func (r *cleanupTrackingRWC) Read(p []byte) (int, error) {
	return 0, nil
}

func (r *cleanupTrackingRWC) Write(p []byte) (int, error) {
	return len(p), nil
}

func (r *cleanupTrackingRWC) Close() error {
	if r.count.Add(1) == 1 {
		close(r.closed)
	}
	return nil
}

func TestCloseStatus(t *testing.T) {
	ce := CloseError{Code: StatusNormalClosure, Reason: "normal closure"}
	tests := []struct {
		err      error
		expected StatusCode
	}{
		{nil, -1},
		{errors.New("some error"), -1},
		{ce, StatusNormalClosure},
		{fmt.Errorf("wrapped: %w", ce), StatusNormalClosure},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.err), func(t *testing.T) {
			got := CloseStatus(test.err)
			if got != test.expected {
				t.Errorf("CloseStatus(%v) = %d; want %d", test.err, got, test.expected)
			}
		})
	}
}

func TestConnCleanupClosesUnderlyingConnection(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	rwc := newCleanupTrackingRWC()
	func() {
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})
		_ = conn
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case <-rwc.closed:
			if got := rwc.count.Load(); got != 1 {
				t.Fatalf("underlying Close call count = %d; want 1", got)
			}
			return
		case <-ctx.Done():
			t.Fatal("test context canceled before cleanup completed")
		default:
		}

		if time.Now().After(deadline) {
			t.Fatal("cleanup did not close underlying connection before timeout")
		}

		runtime.GC()
		runtime.Gosched()
	}
}
