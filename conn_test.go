package websocket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
)

type nopReadWriteCloser struct {
	io.Reader
	io.Writer
}

func (nopReadWriteCloser) Close() error {
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

func TestConnReadMaskedTextFrame(t *testing.T) {
	t.Parallel()

	raw := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opText,
		mask:       true,
		maskKey:    0x01020304,
		payloadLen: int64(len("Hello")),
	}, []byte("Hello"))
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: io.Discard},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(io.Discard),
	})

	messageType, payload, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if messageType != MessageText {
		t.Fatalf("Read messageType = %v, want %v", messageType, MessageText)
	}
	if string(payload) != "Hello" {
		t.Fatalf("Read payload = %q, want %q", payload, "Hello")
	}
}

func TestConnReaderSkipsPing(t *testing.T) {
	t.Parallel()

	ping := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opPing,
		mask:       true,
		maskKey:    0x01020304,
		payloadLen: int64(len("ok")),
	}, []byte("ok"))
	text := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opText,
		mask:       true,
		maskKey:    0x05060708,
		payloadLen: int64(len("next")),
	}, []byte("next"))
	raw := append(ping, text...)
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: io.Discard},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(io.Discard),
	})

	messageType, reader, err := conn.Reader(context.Background())
	if err != nil {
		t.Fatalf("Reader failed: %v", err)
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if messageType != MessageText {
		t.Fatalf("Reader messageType = %v, want %v", messageType, MessageText)
	}
	if string(payload) != "next" {
		t.Fatalf("Reader payload = %q, want %q", payload, "next")
	}
}

func TestConnReadCloseFrame(t *testing.T) {
	t.Parallel()

	closePayload := make([]byte, 2+len("bye"))
	binary.BigEndian.PutUint16(closePayload[:2], uint16(StatusGoingAway))
	copy(closePayload[2:], "bye")
	raw := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opClose,
		mask:       true,
		maskKey:    0x01020304,
		payloadLen: int64(len(closePayload)),
	}, closePayload)
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: io.Discard},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(io.Discard),
	})

	_, _, err := conn.Read(context.Background())
	if err == nil {
		t.Fatal("Read error = nil, want CloseError")
	}
	closeErr, ok := errors.AsType[CloseError](err)
	if !ok {
		t.Fatalf("Read error = %T, want CloseError", err)
	}
	if closeErr.Code != StatusGoingAway {
		t.Fatalf("CloseError.Code = %v, want %v", closeErr.Code, StatusGoingAway)
	}
	if closeErr.Reason != "bye" {
		t.Fatalf("CloseError.Reason = %q, want %q", closeErr.Reason, "bye")
	}
}

func TestConnReadRejectsInvalidMasking(t *testing.T) {
	t.Parallel()

	raw := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opText,
		payloadLen: int64(len("bad")),
	}, []byte("bad"))
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: io.Discard},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(io.Discard),
	})

	_, _, err := conn.Read(context.Background())
	if err == nil || err.Error() != "websocket: invalid frame masking" {
		t.Fatalf("Read error = %v, want invalid frame masking", err)
	}
}

func encodeTestFrame(t *testing.T, header frameHeader, payload []byte) []byte {
	t.Helper()

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	var buf [8]byte
	if err := writeFrameHeader(writer, header, buf[:]); err != nil {
		t.Fatalf("writeFrameHeader failed: %v", err)
	}
	framePayload := append([]byte(nil), payload...)
	if header.mask {
		maskFramePayload(framePayload, header.maskKey)
	}
	if _, err := writer.Write(framePayload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	return raw.Bytes()
}
