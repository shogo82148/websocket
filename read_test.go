package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type blockedReadWriteCloser struct {
	closed chan struct{}
}

func (r *blockedReadWriteCloser) Read(p []byte) (int, error) {
	<-r.closed
	return 0, net.ErrClosed
}

func (r *blockedReadWriteCloser) Write(p []byte) (int, error) {
	<-r.closed
	return 0, net.ErrClosed
}

func (r *blockedReadWriteCloser) Close() error {
	close(r.closed)
	return nil
}

func newTestConnWithInput(t *testing.T, input []byte) *Conn {
	t.Helper()

	rwc := new(testReadWriteCloser)
	if _, err := rwc.Write(input); err != nil {
		t.Fatalf("failed to prepare test input: %v", err)
	}

	return newConn(connConfig{
		rwc: rwc,
		br:  bufio.NewReader(rwc),
		bw:  bufio.NewWriter(rwc),
	})
}

func TestConnReader(t *testing.T) {
	t.Parallel()

	t.Run("reads text message", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}

		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(data, []byte("hello")) {
			t.Fatalf("payload = %q; want %q", data, []byte("hello"))
		}
	})

	t.Run("reads binary message", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x82, 0x04, 0x01, 0x02, 0x03, 0x04}
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}
		if typ != MessageBinary {
			t.Fatalf("message type = %v; want %v", typ, MessageBinary)
		}

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, []byte{0x01, 0x02, 0x03, 0x04}) {
			t.Fatalf("payload = %v; want %v", got, []byte{0x01, 0x02, 0x03, 0x04})
		}
	})

	t.Run("returns EOF when no frame is available", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		conn := newTestConnWithInput(t, nil)

		_, _, err := conn.Reader(ctx)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Reader error = %v; want %v", err, io.EOF)
		}
	})

	t.Run("reader returns EOF on final chunk", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		conn := newTestConnWithInput(t, frame)

		_, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}

		buf := make([]byte, 2)
		n, err := r.Read(buf)
		if err != nil {
			t.Fatalf("first Read error = %v; want nil", err)
		}
		if n != 2 || string(buf[:n]) != "he" {
			t.Fatalf("first Read = (%d, %q); want (2, %q)", n, buf[:n], "he")
		}

		n, err = r.Read(buf)
		if err != nil {
			t.Fatalf("second Read error = %v; want nil", err)
		}
		if n != 2 || string(buf[:n]) != "ll" {
			t.Fatalf("second Read = (%d, %q); want (2, %q)", n, buf[:n], "ll")
		}

		n, err = r.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("third Read error = %v; want %v", err, io.EOF)
		}
		if n != 1 || string(buf[:n]) != "o" {
			t.Fatalf("third Read = (%d, %q); want (1, %q)", n, buf[:n], "o")
		}
	})

	t.Run("zero-byte final frame returns EOF immediately", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x00}
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}

		buf := make([]byte, 8)
		n, err := r.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read error = %v; want %v", err, io.EOF)
		}
		if n != 0 {
			t.Fatalf("Read n = %d; want 0", n)
		}
	})

	t.Run("reads masked payload across multiple reads", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x85, 0x01, 0x02, 0x03, 0x04, 0x69, 0x67, 0x6f, 0x68, 0x6e}
		conn := newTestConnWithInput(t, frame)
		_, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}

		buf2 := make([]byte, 2)
		n, err := r.Read(buf2)
		if err != nil {
			t.Fatalf("first Read error = %v; want nil", err)
		}
		if n != 2 || string(buf2[:n]) != "he" {
			t.Fatalf("first Read = (%d, %q); want (2, %q)", n, buf2[:n], "he")
		}

		n, err = r.Read(buf2)
		if err != nil {
			t.Fatalf("second Read error = %v; want nil", err)
		}
		if n != 2 || string(buf2[:n]) != "ll" {
			t.Fatalf("second Read = (%d, %q); want (2, %q)", n, buf2[:n], "ll")
		}

		n, err = r.Read(buf2)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("third Read error = %v; want %v", err, io.EOF)
		}
		if n != 1 || string(buf2[:n]) != "o" {
			t.Fatalf("third Read = (%d, %q); want (1, %q)", n, buf2[:n], "o")
		}
	})

	t.Run("timeout while reading frame header returns context error", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		rwc := &blockedReadWriteCloser{closed: make(chan struct{})}
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		_, _, err := conn.Reader(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Reader error = %v; want wrapping %v", err, context.DeadlineExceeded)
		}
	})
}
