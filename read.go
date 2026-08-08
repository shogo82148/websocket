package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type messageReader struct {
	ctx        context.Context
	conn       *Conn
	fin        bool
	payloadLen int64
	mask       uint32
}

func (r *messageReader) Read(p []byte) (int, error) {
	if r.payloadLen <= 0 {
		if r.fin {
			r.close()
			return 0, io.EOF
		}

		// Read the next frame header.
		h, err := r.conn.readLoop(r.ctx)
		if err != nil {
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

func (r *messageReader) setHeader(h frameHeader) {
	r.fin = h.fin
	r.payloadLen = h.payloadLen
	r.mask = h.maskKey
}

func (r *messageReader) close() error {
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
		c.readerMu.unlock()
		if cerr := c.canceledRead(); cerr != nil {
			return 0, nil, cerr
		}
		return 0, nil, err
	}
	r := &messageReader{
		ctx:  ctx,
		conn: c,
	}
	r.setHeader(h)
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
