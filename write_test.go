package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"
)

type testReadWriteCloser struct {
	bytes.Buffer
}

func (rw *testReadWriteCloser) Close() error {
	return nil
}

func TestConnWriter(t *testing.T) {
	t.Run("writes fragmented text message and final frame on close", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		w, err := conn.Writer(t.Context(), MessageText)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		if _, err := w.Write([]byte("hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		expected := []byte{0x01, 0x05, 'h', 'e', 'l', 'l', 'o', 0x80, 0x00}
		if got := rwc.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})

	t.Run("uses continuation frames after first write", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		w, err := conn.Writer(t.Context(), MessageBinary)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		if _, err := w.Write([]byte{0x01, 0x02}); err != nil {
			t.Fatalf("first Write failed: %v", err)
		}
		if _, err := w.Write([]byte{0x03}); err != nil {
			t.Fatalf("second Write failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		expected := []byte{0x02, 0x02, 0x01, 0x02, 0x00, 0x01, 0x03, 0x80, 0x00}
		if got := rwc.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})

	t.Run("rejects invalid message type", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		w, err := conn.Writer(t.Context(), MessageType(99))
		if err == nil {
			t.Fatal("Writer succeeded for invalid message type")
		}
		if w != nil {
			t.Fatal("Writer returned non-nil writer for invalid message type")
		}
	})

	t.Run("releases writer lock on close", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		first, err := conn.Writer(t.Context(), MessageText)
		if err != nil {
			t.Fatalf("first Writer failed: %v", err)
		}

		blockedCtx, cancel := context.WithCancel(context.Background())
		cancel()
		second, err := conn.Writer(blockedCtx, MessageText)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Writer with canceled context error = %v; want wrapping %v", err, context.Canceled)
		}
		if second != nil {
			t.Fatal("Writer returned non-nil writer while lock was held")
		}

		if err := first.Close(); err != nil {
			t.Fatalf("first Close failed: %v", err)
		}

		second, err = conn.Writer(t.Context(), MessageText)
		if err != nil {
			t.Fatalf("second Writer failed after close: %v", err)
		}
		if err := second.Close(); err != nil {
			t.Fatalf("second Close failed: %v", err)
		}
	})
}

func TestConnWrite(t *testing.T) {
	t.Run("writes a single final text frame", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		if err := conn.Write(t.Context(), MessageText, []byte("hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		expected := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		if got := rwc.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})

	t.Run("rejects invalid message type", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		err := conn.Write(t.Context(), MessageType(99), []byte("hello"))
		if err == nil {
			t.Fatal("Write succeeded for invalid message type")
		}
	})

	t.Run("respects writer lock context cancellation", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		w, err := conn.Writer(t.Context(), MessageText)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}

		blockedCtx, cancel := context.WithCancel(context.Background())
		cancel()
		err = conn.Write(blockedCtx, MessageBinary, []byte{0x01})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Write with canceled context error = %v; want wrapping %v", err, context.Canceled)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if err := conn.Write(t.Context(), MessageBinary, []byte{0x01, 0x02}); err != nil {
			t.Fatalf("Write failed after writer close: %v", err)
		}

		expected := []byte{0x81, 0x00, 0x82, 0x02, 0x01, 0x02}
		if got := rwc.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})
}
