package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

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

func buildFrameForTest(t *testing.T, fin bool, code opCode, payload []byte) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	bw := bufio.NewWriter(buf)
	header := frameHeader{
		fin:        fin,
		opCode:     code,
		mask:       false,
		payloadLen: int64(len(payload)),
	}
	if err := writeFrameHeader(bw, header); err != nil {
		t.Fatalf("writeFrameHeader failed: %v", err)
	}
	if _, err := bw.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("failed to flush payload: %v", err)
	}
	return buf.Bytes()
}

func TestConnReader(t *testing.T) {
	t.Run("reads text message", func(t *testing.T) {
		frame := buildFrameForTest(t, true, opText, []byte("hello"))
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(context.Background())
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
		payload := []byte{0x01, 0x02, 0x03, 0x04}
		frame := buildFrameForTest(t, true, opBinary, payload)
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(context.Background())
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
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload = %v; want %v", got, payload)
		}
	})

	t.Run("returns EOF when no frame is available", func(t *testing.T) {
		conn := newTestConnWithInput(t, nil)

		_, _, err := conn.Reader(context.Background())
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Reader error = %v; want %v", err, io.EOF)
		}
	})

	t.Run("reader returns EOF on final chunk", func(t *testing.T) {
		frame := buildFrameForTest(t, true, opText, []byte("hello"))
		conn := newTestConnWithInput(t, frame)

		_, r, err := conn.Reader(context.Background())
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
		frame := buildFrameForTest(t, true, opText, nil)
		conn := newTestConnWithInput(t, frame)

		typ, r, err := conn.Reader(context.Background())
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
}
