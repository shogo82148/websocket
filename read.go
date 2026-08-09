package websocket

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
)

type messageReader struct {
	ctx         context.Context
	conn        *Conn
	flate       bool
	flateReader io.Reader
	flateBufio  *bufio.Reader
	flateTail   strings.Reader
	limitReader *limitReader
	dict        *slidingWindow

	fin        bool
	payloadLen int64
	mask       uint32
	closed     bool
}

func newMessageReader(conn *Conn) *messageReader {
	return &messageReader{
		conn: conn,
	}
}

func (r *messageReader) reset(ctx context.Context, h frameHeader) {
	r.ctx = ctx
	r.flate = h.rsv1
	r.closed = false
	r.setHeader(h)
}

func (r *messageReader) setHeader(h frameHeader) {
	r.fin = h.fin
	r.payloadLen = h.payloadLen
	r.mask = h.maskKey
}

func (r *messageReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, net.ErrClosed
	}
	if r.payloadLen <= 0 {
		if r.fin {
			r.close()
			return 0, io.EOF
		}

		// Read the next frame header.
		h, err := r.conn.readLoop(r.ctx)
		if err != nil {
			r.close()
			if cerr := r.conn.canceledRead(); cerr != nil {
				return 0, cerr
			}
			return 0, err
		}
		r.setHeader(h)
		r.payloadLen = h.payloadLen
	}

	if int64(len(p)) > r.payloadLen {
		p = p[:r.payloadLen]
	}
	n, err := r.conn.br.Read(p)
	r.payloadLen -= int64(n)
	if !r.conn.client {
		r.mask = maskFramePayload(p[:n], r.mask)
	}
	if err != nil {
		r.close()
		if cerr := r.conn.canceledRead(); cerr != nil {
			return 0, cerr
		}
		return n, err
	}

	if r.payloadLen == 0 && r.fin {
		r.close()
		return n, io.EOF
	}
	return n, nil
}

func (r *messageReader) close() error {
	if r.closed {
		return net.ErrClosed
	}
	r.closed = true
	r.conn.finishRead()
	r.conn.readerMu.unlock()
	return nil
}

// Reader reads from the connection until there is a WebSocket data message to be read.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error) {
	if err := c.readerMu.lock(ctx); err != nil {
		return 0, nil, err
	}

	if err := c.watchReadCancel(ctx); err != nil {
		c.readerMu.unlock()
		return 0, nil, err
	}

	h, err := c.readLoop(ctx)
	if err != nil {
		c.finishRead()
		c.readerMu.unlock()
		if cerr := c.canceledRead(); cerr != nil {
			return 0, nil, cerr
		}
		return 0, nil, err
	}
	r := c.msgReader
	r.reset(ctx, h)
	return MessageType(h.opCode), r, nil
}

// Read reads a single WebSocket message from the connection.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	typ, r, err := c.Reader(ctx)
	if err != nil {
		return 0, nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, nil, err
	}
	return typ, data, nil
}

// CloseRead starts a goroutine to read from the connection until it is closed
// or a data message is received.
//
// Once CloseRead is called you cannot read any messages from the connection.
// The returned context will be cancelled when the connection is closed.
//
// If a data message is received, the connection will be closed with StatusPolicyViolation.
//
// Call CloseRead when you do not expect to read any more messages.
// Since it actively reads from the connection, it will ensure that ping, pong and close
// frames are responded to. This means c.Ping and c.Close will still work as expected.
//
// This function is idempotent.
func (c *Conn) CloseRead(ctx context.Context) context.Context {
	// TODO: implement CloseRead
	return ctx
}

func (c *Conn) readLoop(ctx context.Context) (frameHeader, error) {
	for {
		h, err := readFrameHeader(c.br)
		if err != nil {
			return frameHeader{}, err
		}

		// TODO: verify the frame header

		switch h.opCode {
		case opClose, opPing, opPong:
			if err := c.handleControlFrame(ctx, h); err != nil {
				return frameHeader{}, err
			}
		case opContinuation, opText, opBinary:
			return h, nil
		default:
			c.writeClose(ctx, StatusProtocolError, "received unknown opcode")
			return frameHeader{}, fmt.Errorf("websocket: received unknown opcode: %d", h.opCode)
		}
	}
}

func (c *Conn) handleControlFrame(ctx context.Context, h frameHeader) error {
	// validate control frame
	if h.payloadLen < 0 || h.payloadLen > maxControlPayload {
		c.writeClose(ctx, StatusProtocolError, "control frame payload length is invalid")
		return fmt.Errorf("websocket: control frame payload length is invalid: %d", h.payloadLen)
	}
	if !h.fin {
		c.writeClose(ctx, StatusProtocolError, "control frame is fragmented")
		return errors.New("websocket: control frame is fragmented")
	}

	buf := make([]byte, h.payloadLen)
	if _, err := io.ReadFull(c.br, buf); err != nil {
		return err
	}
	if h.mask {
		maskFramePayload(buf, h.maskKey)
	}

	switch h.opCode {
	case opClose:
		ce, err := parseClosePayload(buf)
		if err != nil {
			c.writeClose(ctx, StatusProtocolError, "received invalid close payload")
			return err
		}
		return ce
	case opPing:
		return c.writeFrame(ctx, true, opPong, buf)
	case opPong:
	default:
		c.writeClose(ctx, StatusProtocolError, "received unknown opcode")
		return fmt.Errorf("websocket: received unknown opcode: %d", h.opCode)
	}
	return nil
}

var ErrMessageTooBig = errors.New("websocket: message too big")

type limitReader struct {
	ctx   context.Context
	c     *Conn
	r     io.Reader
	limit atomic.Int64
	n     int64
}

func newLimitReader(c *Conn, limit int64) *limitReader {
	lr := &limitReader{
		c: c,
	}
	lr.limit.Store(limit)
	return lr
}

func (lr *limitReader) reset(ctx context.Context, r io.Reader) {
	lr.ctx = ctx
	lr.n = lr.limit.Load()
	lr.r = r
}

func (lr *limitReader) Read(p []byte) (int, error) {
	if lr.n < 0 {
		// no limit
		return lr.r.Read(p)
	}

	if lr.n == 0 {
		lr.c.writeClose(lr.ctx, StatusMessageTooBig, "read limit")
		return 0, ErrMessageTooBig
	}

	if int64(len(p)) > lr.n {
		p = p[:lr.n]
	}
	n, err := lr.r.Read(p)
	lr.n -= int64(n)
	if lr.n < 0 {
		lr.n = 0
	}
	return n, err
}
