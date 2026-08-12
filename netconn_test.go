package websocket

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func newNetConnPair(t *testing.T, msgType MessageType) (net.Conn, *Conn) {
	t.Helper()

	clientSide, serverSide := net.Pipe()
	client := newConn(connConfig{
		rwc:    clientSide,
		client: true,
		br:     bufio.NewReader(clientSide),
		bw:     bufio.NewWriter(clientSide),
	})
	server := newConn(connConfig{
		rwc: serverSide,
		br:  bufio.NewReader(serverSide),
		bw:  bufio.NewWriter(serverSide),
	})
	t.Cleanup(func() {
		_ = client.CloseNow()
		_ = server.CloseNow()
	})

	return NetConn(t.Context(), client, msgType), server
}

func TestNetConnWrite(t *testing.T) {
	t.Parallel()

	nc, peer := newNetConnPair(t, MessageBinary)
	want := []byte("hello over websocket")

	type readResult struct {
		typ  MessageType
		data []byte
		err  error
	}
	result := make(chan readResult, 1)
	go func() {
		typ, data, err := peer.Read(t.Context())
		result <- readResult{typ: typ, data: data, err: err}
	}()

	n, err := nc.Write(want)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(want) {
		t.Fatalf("Write returned %d; want %d", n, len(want))
	}

	got := <-result
	if got.err != nil {
		t.Fatalf("peer Read failed: %v", got.err)
	}
	if got.typ != MessageBinary {
		t.Fatalf("message type = %v; want %v", got.typ, MessageBinary)
	}
	if string(got.data) != string(want) {
		t.Fatalf("message = %q; want %q", got.data, want)
	}
}

func TestNetConnReadAcrossMessages(t *testing.T) {
	t.Parallel()

	nc, peer := newNetConnPair(t, MessageText)
	writeErr := make(chan error, 1)
	go func() {
		for _, p := range [][]byte{nil, []byte("abc"), []byte("def")} {
			if err := peer.Write(t.Context(), MessageText, p); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	buf := make([]byte, 2)
	var got []byte
	for len(got) < len("abcdef") {
		n, err := nc.Read(buf)
		if err != nil {
			t.Fatalf("Read failed after %q: %v", got, err)
		}
		if n == 0 {
			t.Fatal("Read returned no data and no error")
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != "abcdef" {
		t.Fatalf("read data = %q; want %q", got, "abcdef")
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("peer Write failed: %v", err)
	}
}

func TestNetConnRejectsUnexpectedMessageType(t *testing.T) {
	t.Parallel()

	nc, peer := newNetConnPair(t, MessageBinary)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- peer.Write(t.Context(), MessageText, []byte("text"))
	}()

	_, err := nc.Read(make([]byte, 1))
	if err == nil || err.Error() != "unsupported message type" {
		t.Fatalf("Read error = %v; want unsupported message type", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("peer Write failed: %v", err)
	}
}

func TestNetConnTranslatesNormalCloseToEOF(t *testing.T) {
	t.Parallel()

	for name, code := range map[string]StatusCode{
		"normal closure": StatusNormalClosure,
		"going away":     StatusGoingAway,
	} {
		t.Run(name, func(t *testing.T) {
			nc, peer := newNetConnPair(t, MessageBinary)
			closeErr := make(chan error, 1)
			go func() { closeErr <- peer.Close(code, "bye") }()

			_, err := nc.Read(make([]byte, 1))
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Read error = %v; want %v", err, io.EOF)
			}
			if err := <-closeErr; err != nil {
				t.Fatalf("peer Close failed: %v", err)
			}
		})
	}
}

func TestNetConnContextCancellation(t *testing.T) {
	t.Parallel()

	clientSide, serverSide := net.Pipe()
	client := newConn(connConfig{
		rwc: clientSide, client: true,
		br: bufio.NewReader(clientSide), bw: bufio.NewWriter(clientSide),
	})
	t.Cleanup(func() {
		_ = client.CloseNow()
		_ = serverSide.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	nc := NetConn(ctx, client, MessageBinary)
	result := make(chan error, 1)
	go func() {
		_, err := nc.Read(make([]byte, 1))
		result <- err
	}()
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Read error = %v; want wrapping %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Read was not canceled")
	}
}

func TestNetConnAddresses(t *testing.T) {
	t.Parallel()

	nc, peer := newNetConnPair(t, MessageBinary)
	underlying := nc.(*netConn).c.rwc.(net.Conn)

	if got, want := nc.LocalAddr(), underlying.LocalAddr(); got != want {
		t.Fatalf("LocalAddr() = %v; want %v", got, want)
	}
	if got, want := nc.RemoteAddr(), underlying.RemoteAddr(); got != want {
		t.Fatalf("RemoteAddr() = %v; want %v", got, want)
	}
	_ = peer

	rwc := new(testReadWriteCloser)
	c := newConn(connConfig{
		rwc: rwc,
		br:  bufio.NewReader(rwc),
		bw:  bufio.NewWriter(rwc),
	})
	t.Cleanup(func() { _ = c.CloseNow() })
	unknown := NetConn(t.Context(), c, MessageBinary)
	for name, addr := range map[string]net.Addr{
		"LocalAddr":  unknown.LocalAddr(),
		"RemoteAddr": unknown.RemoteAddr(),
	} {
		if addr.Network() != "websocket" || addr.String() != "websocket/unknown-addr" {
			t.Errorf("%s() = (%q, %q); want (%q, %q)", name, addr.Network(), addr.String(), "websocket", "websocket/unknown-addr")
		}
	}
}
