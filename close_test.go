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

func TestCloseError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		ce      CloseError
		want    []byte
		success bool
	}{
		{
			name: "normal closure",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: "normal closure",
			},
			want:    []byte{0x03, 0xE8, 'n', 'o', 'r', 'm', 'a', 'l', ' ', 'c', 'l', 'o', 's', 'u', 'r', 'e'},
			success: true,
		},
		{
			name: "reason too long",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: string(bytes.Repeat([]byte{'a'}, maxCloseReason+1)),
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
		{
			name: "invalid utf-8 reason",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: string([]byte{0xff}),
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
		{
			name: "invalid close code",
			ce: CloseError{
				Code:   statusReserved,
				Reason: "invalid close code",
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.ce.bytes()
			if (err == nil) != tc.success {
				t.Fatalf("CloseError.bytes() error = %v, want success = %v", err, tc.success)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("CloseError.bytes() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseClosePayload(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload []byte
		want    CloseError
		success bool
	}{
		{
			name:    "no payload",
			payload: []byte{},
			want: CloseError{
				Code:   StatusNoStatusRcvd,
				Reason: "",
			},
			success: true,
		},
		{
			name:    "valid payload",
			payload: []byte{0x03, 0xE8, 'n', 'o', 'r', 'm', 'a', 'l', ' ', 'c', 'l', 'o', 's', 'u', 'r', 'e'},
			want: CloseError{
				Code:   StatusNormalClosure,
				Reason: "normal closure",
			},
			success: true,
		},
		{
			name:    "payload too short",
			payload: []byte{0x03},
			want:    CloseError{},
			success: false,
		},
		{
			name:    "invalid utf-8 reason",
			payload: []byte{0x03, 0xE8, 0xff},
			want:    CloseError{},
			success: false,
		},
		{
			name:    "invalid close code",
			payload: []byte{0x00, 0x00, 'i', 'n', 'v', 'a', 'l', 'i', 'd'},
			want:    CloseError{},
			success: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseClosePayload(tc.payload)
			if (err == nil) != tc.success {
				t.Fatalf("parseClosePayload() error = %v, want success = %v", err, tc.success)
			}
			if got != tc.want {
				t.Errorf("parseClosePayload() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConnWaitCloseHandshake(t *testing.T) {
	t.Run("accepts a close frame", func(t *testing.T) {
		ctx := t.Context()
		conn, _ := newTestConnWithInput(t, []byte{0x88, 0x02, 0x03, 0xe8})

		err := conn.waitCloseHandshake(ctx)
		if ce, ok := errors.AsType[CloseError](err); !ok || ce.Code != StatusNormalClosure {
			t.Fatalf("waitCloseHandshake error = %v; want CloseError with StatusNormalClosure", err)
		}
	})

	t.Run("discards data messages before the close frame", func(t *testing.T) {
		ctx := t.Context()
		input := []byte{
			0x81, 0x05, 'h', 'e', 'l', 'l', 'o',
			0x82, 0x03, 0x01, 0x02, 0x03,
			0x88, 0x02, 0x03, 0xe8,
		}
		conn, _ := newTestConnWithInput(t, input)

		err := conn.waitCloseHandshake(ctx)
		if ce, ok := errors.AsType[CloseError](err); !ok || ce.Code != StatusNormalClosure {
			t.Fatalf("waitCloseHandshake error = %v; want CloseError with StatusNormalClosure", err)
		}
	})

	t.Run("returns an error when the transport closes before a close frame", func(t *testing.T) {
		ctx := t.Context()
		conn, _ := newTestConnWithInput(t, nil)

		err := conn.waitCloseHandshake(ctx)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("waitCloseHandshake error = %v; want %v", err, io.EOF)
		}
	})

	t.Run("honors context cancellation", func(t *testing.T) {
		rwc := &blockedReadWriteCloser{closed: make(chan struct{})}
		conn := newConn(connConfig{
			rwc:    rwc,
			client: true,
			br:     bufio.NewReader(rwc),
			bw:     bufio.NewWriter(rwc),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		err := conn.waitCloseHandshake(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitCloseHandshake error = %v; want wrapping %v", err, context.DeadlineExceeded)
		}
	})
}

func TestConnCloseHandshake(t *testing.T) {
	t.Run("returns an error when the peer responds with a different status", func(t *testing.T) {
		ctx := t.Context()
		conn, _ := newTestConnWithInput(t, []byte{0x88, 0x02, 0x03, 0xe9})

		err := conn.closeHandshake(ctx, StatusNormalClosure, "")
		ce, ok := errors.AsType[CloseError](err)
		if !ok {
			t.Fatalf("closeHandshake error = %v; want CloseError", err)
		}
		if ce.Code != StatusGoingAway {
			t.Fatalf("closeHandshake status = %v; want %v", ce.Code, StatusGoingAway)
		}
	})

	t.Run("sends close, receives close, and closes the transport", func(t *testing.T) {
		conn, rwc := newTestConnWithInput(t, []byte{0x88, 0x02, 0x03, 0xe8})

		if err := conn.Close(StatusNormalClosure, ""); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if got := rwc.closeCount.Load(); got != 1 {
			t.Fatalf("underlying Close call count = %d; want 1", got)
		}

		br := bufio.NewReader(bytes.NewReader(rwc.w.Bytes()))
		h, err := conn.readFrameHeader()
		if err != nil {
			t.Fatalf("failed to read sent close frame: %v", err)
		}
		if !h.fin || h.opCode != opClose || !h.mask {
			t.Fatalf("sent frame header = %+v; want final, masked close frame", h)
		}
		payload := make([]byte, h.payloadLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			t.Fatalf("failed to read sent close payload: %v", err)
		}
		maskFramePayload(payload, h.maskKey)
		ce, err := parseClosePayload(payload)
		if err != nil {
			t.Fatalf("sent close payload is invalid: %v", err)
		}
		if ce != (CloseError{Code: StatusNormalClosure}) {
			t.Fatalf("sent close = %+v; want normal closure without reason", ce)
		}
	})

	t.Run("responds when the peer initiates the handshake", func(t *testing.T) {
		conn, rwc := newTestConnWithInput(t, []byte{0x88, 0x05, 0x03, 0xe9, 'b', 'y', 'e'})

		_, _, err := conn.Reader(t.Context())
		var ce CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("Reader error = %v; want CloseError", err)
		}
		if ce != (CloseError{Code: StatusGoingAway, Reason: "bye"}) {
			t.Fatalf("received close = %+v; want going away with reason", ce)
		}

		br := bufio.NewReader(bytes.NewReader(rwc.w.Bytes()))
		h, err := conn.readFrameHeader()
		if err != nil {
			t.Fatalf("peer-initiated close produced no response frame: %v", err)
		}
		if h.opCode != opClose {
			t.Fatalf("response opcode = %v; want close", h.opCode)
		}
		payload := make([]byte, h.payloadLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			t.Fatalf("failed to read response close payload: %v", err)
		}
		if h.mask {
			maskFramePayload(payload, h.maskKey)
		}
		got, err := parseClosePayload(payload)
		if err != nil {
			t.Fatalf("response close payload is invalid: %v", err)
		}
		if got != ce {
			t.Fatalf("response close = %+v; want echo of %+v", got, ce)
		}
	})

	t.Run("works while CloseRead owns the reader", func(t *testing.T) {
		local, peer := net.Pipe()
		defer peer.Close()
		conn := newConn(connConfig{
			rwc:    local,
			client: true,
			br:     bufio.NewReader(local),
			bw:     bufio.NewWriter(local),
		})
		closeReadCtx := conn.CloseRead(context.Background())

		peerErr := make(chan error, 1)
		go func() {
			br := bufio.NewReader(peer)
			h, err := conn.readFrameHeader()
			if err != nil {
				peerErr <- err
				return
			}
			if h.opCode != opClose {
				peerErr <- errors.New("received frame is not a close frame")
				return
			}
			if _, err := io.CopyN(io.Discard, br, h.payloadLen); err != nil {
				peerErr <- err
				return
			}
			_, err = peer.Write([]byte{0x88, 0x02, 0x03, 0xe8})
			peerErr <- err
		}()

		if err := conn.Close(StatusNormalClosure, ""); err != nil {
			t.Fatalf("Close failed while CloseRead was active: %v", err)
		}
		if err := <-peerErr; err != nil {
			t.Fatalf("peer failed during close handshake: %v", err)
		}
		select {
		case <-closeReadCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("CloseRead context was not canceled after close handshake")
		}
	})
}
