package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestPing(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	client := newConn(connConfig{
		rwc: clientSide, client: true,
		br: bufio.NewReader(clientSide), bw: bufio.NewWriter(clientSide),
	})
	server := newConn(connConfig{
		rwc: serverSide,
		br:  bufio.NewReader(serverSide), bw: bufio.NewWriter(serverSide),
	})
	t.Cleanup(func() {
		_ = client.CloseNow()
		_ = server.CloseNow()
	})

	client.CloseRead(t.Context())
	server.CloseRead(t.Context())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestPingContextCancellation(t *testing.T) {
	rwc := new(testReadWriteCloser)
	conn := newConn(connConfig{
		rwc: rwc,
		br:  bufio.NewReader(rwc), bw: bufio.NewWriter(rwc),
	})
	t.Cleanup(func() { _ = conn.CloseNow() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := conn.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping error = %v; want wrapping %v", err, context.Canceled)
	}
}

func TestPingCallbackCanSuppressPong(t *testing.T) {
	input := []byte{0x89, 0x04, 'p', 'i', 'n', 'g', 0x81, 0x00}
	rwc := new(testReadWriteCloser)
	_, _ = rwc.r.Write(input)
	called := false
	conn := newConn(connConfig{
		rwc: rwc, client: true,
		br: bufio.NewReader(rwc), bw: bufio.NewWriter(rwc),
		onPingReceived: func(_ context.Context, payload []byte) bool {
			called = bytes.Equal(payload, []byte("ping"))
			return false
		},
	})
	t.Cleanup(func() { _ = conn.CloseNow() })

	if _, _, err := conn.Reader(t.Context()); err != nil {
		t.Fatalf("Reader failed: %v", err)
	}
	if !called {
		t.Fatal("OnPingReceived was not called with the ping payload")
	}
	if rwc.w.Len() != 0 {
		t.Fatalf("wrote %v; want no pong frame", rwc.w.Bytes())
	}
}
