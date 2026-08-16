package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type testReadWriteCloser struct {
	r, w       bytes.Buffer
	closeCount atomic.Int32
}

func (rw *testReadWriteCloser) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *testReadWriteCloser) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func (rw *testReadWriteCloser) Close() error {
	rw.closeCount.Add(1)
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
		if got := rwc.w.Bytes(); !bytes.Equal(got, expected) {
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
		if got := rwc.w.Bytes(); !bytes.Equal(got, expected) {
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

	t.Run("timeout while writing frame header returns context error", func(t *testing.T) {
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

		w, err := conn.Writer(ctx, MessageText)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}

		if _, err := w.Write([]byte("hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		if err := w.Close(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v; want wrapping %v", err, context.DeadlineExceeded)
		}
	})

	t.Run("writes a compressed message when the first write reaches the threshold", func(t *testing.T) {
		ctx := t.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			flateThreshold: 5,
		})

		w, err := conn.Writer(ctx, MessageText)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		if _, err := w.Write([]byte("hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if _, err := w.Write([]byte(" world")); err != nil {
			t.Fatalf("second Write failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if got := rwc.w.Bytes()[0] & 0x40; got == 0 {
			t.Fatal("first frame does not have RSV1 set")
		}

		peerRWC := new(testReadWriteCloser)
		peerRWC.r.Write(rwc.w.Bytes())
		peer := newConn(connConfig{
			rwc:    peerRWC,
			client: true,
			br:     bufio.NewReader(peerRWC),
			bw:     bufio.NewWriter(peerRWC),
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
		})
		_, got, err := peer.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if string(got) != "hello world" {
			t.Fatalf("Read returned %q; want %q", got, "hello world")
		}
	})

	t.Run("does not start compression after the first fragment", func(t *testing.T) {
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc:            rwc,
			br:             bufio.NewReader(rwc),
			bw:             bufio.NewWriter(rwc),
			copts:          new(compressionOptions),
			flateThreshold: 5,
		})

		w, err := conn.Writer(t.Context(), MessageText)
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		if _, err := w.Write([]byte("tiny")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if _, err := w.Write([]byte("this write is over the threshold")); err != nil {
			t.Fatalf("second Write failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if got := rwc.w.Bytes()[0] & 0x40; got != 0 {
			t.Fatal("first frame unexpectedly has RSV1 set")
		}
	})
}

func TestConnWrite(t *testing.T) {
	t.Parallel()

	t.Run("writes a single final text frame", func(t *testing.T) {
		t.Parallel()
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
		if got := rwc.w.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})

	t.Run("writes masked frames when client is true", func(t *testing.T) {
		t.Parallel()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc:    rwc,
			client: true, // client connections must mask frames
			br:     bufio.NewReader(rwc),
			bw:     bufio.NewWriter(rwc),
		})

		if err := conn.Write(t.Context(), MessageText, []byte("hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		got := rwc.w.Bytes()
		if got[1]&0x80 == 0 {
			t.Fatal("frame is not masked")
		}
		maskKey := got[2:6]
		payload := got[2+4:] // skip header and mask key
		if maskKey[0]^payload[0] != 'h' || maskKey[1]^payload[1] != 'e' || maskKey[2]^payload[2] != 'l' || maskKey[3]^payload[3] != 'l' || maskKey[0]^payload[4] != 'o' {
			t.Fatalf("payload is not masked correctly: got %v, mask key %v", payload, maskKey)
		}
	})

	t.Run("writes a large payload in a single frame", func(t *testing.T) {
		t.Parallel()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc:    rwc,
			client: true, // client connections must mask frames
			br:     bufio.NewReader(rwc),
			bw:     bufio.NewWriter(rwc),
		})

		payload := bytes.Repeat([]byte{'a'}, 64*1024) // 64KB payload
		if err := conn.Write(t.Context(), MessageText, payload); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		got := rwc.w.Bytes()
		if got[1]&0x80 == 0 {
			t.Fatal("frame is not masked")
		}
	})

	t.Run("rejects invalid message type", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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
		if got := rwc.w.Bytes(); !bytes.Equal(got, expected) {
			t.Fatalf("unexpected frame bytes: got %v, want %v", got, expected)
		}
	})

	t.Run("timeout while writing frame header returns context error", func(t *testing.T) {
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

		err := conn.Write(ctx, MessageText, []byte("hello"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Write error = %v; want wrapping %v", err, context.DeadlineExceeded)
		}
	})

	t.Run("writes a compressed text frame when compression is enabled", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			flateThreshold: 1,
		})

		if err := conn.Write(ctx, MessageText, []byte("Hello")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		rwc2 := new(testReadWriteCloser)
		if _, err := rwc2.r.Write(rwc.w.Bytes()); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}
		conn2 := newConn(connConfig{
			rwc:    rwc2,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc2),
			bw: bufio.NewWriter(rwc2),
		})

		typ, got, err := conn2.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("Read returned message type = %v; want %v", typ, MessageText)
		}
		if string(got) != "Hello" {
			t.Fatalf("Read returned message = %q; want %q", string(got), "Hello")
		}
	})
}

func BenchmarkConnWriter(b *testing.B) {
	b.Run("writes a text frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			w, err := conn.Writer(ctx, MessageText)
			if err != nil {
				b.Fatalf("Writer failed: %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})

	b.Run("writes a text frame from the client", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			br:     bufio.NewReader(rwc),
			bw:     bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			w, err := conn.Writer(ctx, MessageText)
			if err != nil {
				b.Fatalf("Writer failed: %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})

	b.Run("writes a binary frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			w, err := conn.Writer(ctx, MessageBinary)
			if err != nil {
				b.Fatalf("Writer failed: %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})

	b.Run("writes a compressed binary frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			flateThreshold: 1,
			br:             bufio.NewReader(rwc),
			bw:             bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			w, err := conn.Writer(ctx, MessageBinary)
			if err != nil {
				b.Fatalf("Writer failed: %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})
}

func BenchmarkConnWrite(b *testing.B) {
	b.Run("writes a text frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			if err := conn.Write(ctx, MessageText, payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("writes a text frame from the client", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc:    rwc,
			br:     bufio.NewReader(rwc),
			bw:     bufio.NewWriter(io.Discard),
			client: true,
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			if err := conn.Write(ctx, MessageText, payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("writes a binary frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			if err := conn.Write(ctx, MessageBinary, payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("writes a compressed binary frame from the server", func(b *testing.B) {
		ctx := b.Context()
		rwc := new(testReadWriteCloser)
		conn := newConn(connConfig{
			rwc: rwc,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			flateThreshold: 1,
			br:             bufio.NewReader(rwc),
			bw:             bufio.NewWriter(io.Discard),
		})
		payload := []byte("Hello, 世界🍺")
		b.ResetTimer()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			if err := conn.Write(ctx, MessageBinary, payload); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})
}
