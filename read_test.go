package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
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

func newTestConnWithInput(t testing.TB, input []byte) (*Conn, *testReadWriteCloser) {
	t.Helper()

	rwc := new(testReadWriteCloser)
	if _, err := rwc.r.Write(input); err != nil {
		t.Fatalf("failed to prepare test input: %v", err)
	}

	return newConn(connConfig{
		rwc:    rwc,
		client: true,
		br:     bufio.NewReader(rwc),
		bw:     bufio.NewWriter(rwc),
	}), rwc
}

func TestConnReader(t *testing.T) {
	t.Parallel()

	t.Run("reads text message", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		conn, _ := newTestConnWithInput(t, frame)

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
		conn, _ := newTestConnWithInput(t, frame)

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

		conn, _ := newTestConnWithInput(t, nil)

		_, _, err := conn.Reader(ctx)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Reader error = %v; want %v", err, io.EOF)
		}
	})

	t.Run("reader returns EOF on final chunk", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		conn, _ := newTestConnWithInput(t, frame)

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
		conn, _ := newTestConnWithInput(t, frame)

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
		conn, _ := newTestConnWithInput(t, frame)
		conn.client = false // disable masking for outgoing frames
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

func TestConnRead(t *testing.T) {
	t.Parallel()

	t.Run("reads text message", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
		conn, _ := newTestConnWithInput(t, frame)

		typ, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Reader failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}

		want := []byte("hello")
		if !bytes.Equal(data, want) {
			t.Fatalf("payload = %q; want %q", data, want)
		}
	})

	// RFC 7692 Section 7.2.3.1 A Message Compressed Using One Compressed DEFLATE Block
	t.Run("a message compressed using one compressed DEFLATE block", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.1 A Message Compressed Using One Compressed DEFLATE Block
	t.Run("the compressed message with fragmentation", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0x41, 0x03, 0xf2, 0x48, 0xcd, // first frame
			0x80, 0x04, 0xc9, 0xc9, 0x07, 0x00, // second frame
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.2. Sharing LZ77 Sliding Window
	t.Run("sharing LZ77 sliding window", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, // first frame
			0xc1, 0x05, 0xf2, 0x00, 0x11, 0x00, 0x00, // second frame
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: false,
				serverNoContextTakeover: false,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		// validate first frame
		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}

		// validate second frame
		typ, payload, err = conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want = []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.3. Using a DEFLATE Block with No Compression
	t.Run("a DEFLATE block with no compression", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0xc1, 0x0b, 0x00, 0x05, 0x00, 0xfa, 0xff, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00,
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.4. Using a DEFLATE Block with "BFINAL" Set to 1
	t.Run("DEFLATE blocks with BFINAL set to 1.", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0xc1, 0x08, 0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00,
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.5. Two DEFLATE Blocks in One Message
	t.Run("Two DEFLATE blocks in one message", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0xc1, 0x0d, 0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07, 0x00,
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte("Hello")
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	// RFC 7692 Section 7.2.3.6. Generating an Empty Fragment
	t.Run("an Empty Fragment", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{
			0xc1, 0x01, 0x00,
		}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			copts: &compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			br: bufio.NewReader(rwc),
			bw: bufio.NewWriter(rwc),
		})

		typ, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if typ != MessageText {
			t.Fatalf("message type = %v; want %v", typ, MessageText)
		}
		want := []byte{}
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload = %q; want %q", payload, want)
		}
	})

	for _, senderClient := range []bool{true, false} {
		name := "server messages"
		if senderClient {
			name = "client messages"
		}
		t.Run("context takeover for "+name, func(t *testing.T) {
			ctx := t.Context()
			copts := new(compressionOptions)
			senderRWC := new(testReadWriteCloser)
			sender := newConn(connConfig{
				rwc:            senderRWC,
				client:         senderClient,
				copts:          copts,
				flateThreshold: 1,
				br:             bufio.NewReader(senderRWC),
				bw:             bufio.NewWriter(senderRWC),
			})

			first := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog;"), 128)
			second := slices.Clone(first)
			second[len(second)-1] = '!'
			for _, message := range [][]byte{first, second} {
				w, err := sender.Writer(ctx, MessageBinary)
				if err != nil {
					t.Fatalf("Writer failed: %v", err)
				}
				if _, err := w.Write(message); err != nil {
					t.Fatalf("Write failed: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close failed: %v", err)
				}
			}

			receiverRWC := new(testReadWriteCloser)
			if _, err := receiverRWC.r.Write(senderRWC.w.Bytes()); err != nil {
				t.Fatalf("failed to prepare test input: %v", err)
			}
			receiver := newConn(connConfig{
				rwc:    receiverRWC,
				client: !senderClient,
				copts:  copts,
				br:     bufio.NewReader(receiverRWC),
				bw:     bufio.NewWriter(receiverRWC),
			})
			receiver.SetReadLimit(-1)

			for i, want := range [][]byte{first, second} {
				typ, got, err := receiver.Read(ctx)
				if err != nil {
					t.Fatalf("Read message %d failed: %v", i+1, err)
				}
				if typ != MessageBinary {
					t.Fatalf("message %d type = %v; want %v", i+1, typ, MessageBinary)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("message %d payload does not match", i+1)
				}
			}
		})
	}
}

func TestConnCloseRead(t *testing.T) {
	t.Run("returns a context canceled when reading stops", func(t *testing.T) {
		conn, _ := newTestConnWithInput(t, nil)
		ctx := conn.CloseRead(t.Context())

		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("CloseRead context was not canceled")
		}
	})

	t.Run("is idempotent", func(t *testing.T) {
		rwc := &blockedReadWriteCloser{closed: make(chan struct{})}
		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})

		ctx1 := conn.CloseRead(t.Context())
		ctx2 := conn.CloseRead(context.Background())
		if ctx1 != ctx2 {
			t.Fatal("CloseRead returned a different context on the second call")
		}

		if err := conn.CloseNow(); err != nil {
			t.Fatalf("CloseNow failed: %v", err)
		}
		select {
		case <-ctx1.Done():
		case <-time.After(time.Second):
			t.Fatal("CloseRead context was not canceled after closing the connection")
		}
	})
}

func TestLimitReader(t *testing.T) {
	t.Parallel()

	t.Run("returns ErrMessageTooBig when limit is exceeded", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		frame := []byte{0x81, 0x85, 0x01, 0x02, 0x03, 0x04, 0x69, 0x67, 0x6f, 0x68, 0x6e}
		rwc := new(testReadWriteCloser)
		if _, err := rwc.r.Write(frame); err != nil {
			t.Fatalf("failed to prepare test input: %v", err)
		}

		conn := newConn(connConfig{
			rwc: rwc,
			br:  bufio.NewReader(rwc),
			bw:  bufio.NewWriter(rwc),
		})
		conn.SetReadLimit(4)
		_, _, err := conn.Read(ctx)
		if !errors.Is(err, ErrMessageTooBig) {
			t.Fatalf("Read error = %v; want %v", err, ErrMessageTooBig)
		}

		want := []byte{0x88, 0x0c, 0x03, 0xf1, 'r', 'e', 'a', 'd', ' ', 'l', 'i', 'm', 'i', 't'}
		if got := rwc.w.Bytes(); !bytes.Equal(got, want) {
			t.Fatalf("written bytes = %x; want %x", got, want)
		}
	})
}
