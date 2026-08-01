package websocket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
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

func TestConnReaderRespondsToPing(t *testing.T) {
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
	var written bytes.Buffer
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: &written},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(&written),
	})

	messageType, payload, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if messageType != MessageText {
		t.Fatalf("Read messageType = %v, want %v", messageType, MessageText)
	}
	if string(payload) != "next" {
		t.Fatalf("Read payload = %q, want %q", payload, "next")
	}

	header, pongPayload, err := readFrame(bufio.NewReader(bytes.NewReader(written.Bytes())), make([]byte, 8))
	if err != nil {
		t.Fatalf("readFrame failed: %v", err)
	}
	if header.opCode != opPong {
		t.Fatalf("pong opcode = %v, want %v", header.opCode, opPong)
	}
	if header.mask {
		t.Fatal("server pong must not be masked")
	}
	if string(pongPayload) != "ok" {
		t.Fatalf("pong payload = %q, want %q", pongPayload, "ok")
	}
}

func TestConnReaderClientRespondsToPingWithMaskedPong(t *testing.T) {
	t.Parallel()

	ping := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opPing,
		payloadLen: int64(len("ok")),
	}, []byte("ok"))
	text := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opText,
		payloadLen: int64(len("next")),
	}, []byte("next"))
	raw := append(ping, text...)
	var written bytes.Buffer
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: &written},
		client: true,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(&written),
	})

	messageType, payload, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if messageType != MessageText {
		t.Fatalf("Read messageType = %v, want %v", messageType, MessageText)
	}
	if string(payload) != "next" {
		t.Fatalf("Read payload = %q, want %q", payload, "next")
	}

	header, pongPayload, err := readFrame(bufio.NewReader(bytes.NewReader(written.Bytes())), make([]byte, 8))
	if err != nil {
		t.Fatalf("readFrame failed: %v", err)
	}
	if header.opCode != opPong {
		t.Fatalf("pong opcode = %v, want %v", header.opCode, opPong)
	}
	if !header.mask {
		t.Fatal("client pong must be masked")
	}
	if string(pongPayload) != "ok" {
		t.Fatalf("pong payload = %q, want %q", pongPayload, "ok")
	}
}

func TestConnReaderCanSuppressAutomaticPong(t *testing.T) {
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
	var written bytes.Buffer
	called := false
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: &written},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(&written),
		onPingReceived: func(ctx context.Context, payload []byte) bool {
			called = true
			return false
		},
	})

	messageType, payload, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !called {
		t.Fatal("OnPingReceived was not called")
	}
	if messageType != MessageText {
		t.Fatalf("Read messageType = %v, want %v", messageType, MessageText)
	}
	if string(payload) != "next" {
		t.Fatalf("Read payload = %q, want %q", payload, "next")
	}
	if written.Len() != 0 {
		t.Fatalf("automatic pong was not suppressed: wrote %d bytes", written.Len())
	}
}

func TestConnReadFragmentedTextFrame(t *testing.T) {
	t.Parallel()

	first := encodeTestFrame(t, frameHeader{
		fin:        false,
		opCode:     opText,
		mask:       true,
		maskKey:    0x01020304,
		payloadLen: int64(len("Hel")),
	}, []byte("Hel"))
	ping := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opPing,
		mask:       true,
		maskKey:    0x05060708,
		payloadLen: int64(len("ok")),
	}, []byte("ok"))
	last := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opContinuation,
		mask:       true,
		maskKey:    0x11121314,
		payloadLen: int64(len("lo")),
	}, []byte("lo"))
	raw := append(first, ping...)
	raw = append(raw, last...)
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

func TestConnReadRejectsUnexpectedContinuation(t *testing.T) {
	t.Parallel()

	raw := encodeTestFrame(t, frameHeader{
		fin:        true,
		opCode:     opContinuation,
		mask:       true,
		maskKey:    0x01020304,
		payloadLen: int64(len("bad")),
	}, []byte("bad"))
	conn := newConn(connConfig{
		rwc:    nopReadWriteCloser{Reader: bytes.NewReader(raw), Writer: io.Discard},
		client: false,
		br:     bufio.NewReader(bytes.NewReader(raw)),
		bw:     bufio.NewWriter(io.Discard),
	})

	_, _, err := conn.Read(context.Background())
	if err == nil || err.Error() != "websocket: unexpected continuation frame" {
		t.Fatalf("Read error = %v, want unexpected continuation frame", err)
	}
}

func TestConnPing(t *testing.T) {
	t.Parallel()

	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	conn := newConn(connConfig{
		rwc:    local,
		client: false,
		br:     bufio.NewReader(local),
		bw:     bufio.NewWriter(local),
	})

	peerErrCh := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(remote)
		writer := bufio.NewWriter(remote)
		var buf [8]byte

		header, pingPayload, err := readFrame(reader, buf[:])
		if err != nil {
			peerErrCh <- err
			return
		}
		if header.opCode != opPing {
			peerErrCh <- fmt.Errorf("peer saw opcode %v, want %v", header.opCode, opPing)
			return
		}
		if header.mask {
			peerErrCh <- errors.New("server ping must not be masked")
			return
		}

		pong := encodeTestFrame(t, frameHeader{
			fin:        true,
			opCode:     opPong,
			mask:       true,
			maskKey:    0x01020304,
			payloadLen: int64(len(pingPayload)),
		}, pingPayload)
		if _, err := writer.Write(pong); err != nil {
			peerErrCh <- err
			return
		}

		text := encodeTestFrame(t, frameHeader{
			fin:        true,
			opCode:     opText,
			mask:       true,
			maskKey:    0x05060708,
			payloadLen: int64(len("next")),
		}, []byte("next"))
		if _, err := writer.Write(text); err != nil {
			peerErrCh <- err
			return
		}
		peerErrCh <- writer.Flush()
	}()

	readResultCh := make(chan struct {
		messageType MessageType
		payload     []byte
		err         error
	}, 1)
	go func() {
		messageType, payload, err := conn.Read(context.Background())
		readResultCh <- struct {
			messageType MessageType
			payload     []byte
			err         error
		}{messageType: messageType, payload: payload, err: err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	if err := <-peerErrCh; err != nil {
		t.Fatalf("peer failed: %v", err)
	}
	result := <-readResultCh
	if result.err != nil {
		t.Fatalf("Read failed: %v", result.err)
	}
	if result.messageType != MessageText {
		t.Fatalf("Read messageType = %v, want %v", result.messageType, MessageText)
	}
	if string(result.payload) != "next" {
		t.Fatalf("Read payload = %q, want %q", result.payload, "next")
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
