package websocket

import (
	"context"
	"io"
)

type messageReader struct {
	conn       *Conn
	fin        bool
	payloadLen int64
}

func (r *messageReader) Read(p []byte) (int, error) {
	if r.payloadLen <= 0 {
		h, err := r.conn.readLoop()
		if err != nil {
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
	if err != nil {
		return n, err
	}
	if r.payloadLen == 0 && r.fin {
		return n, io.EOF
	}
	return n, nil
}

func (r *messageReader) setHeader(h frameHeader) {
	r.fin = h.fin
	r.payloadLen = h.payloadLen
}

// Reader reads from the connection until there is a WebSocket data message to be read.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error) {
	h, err := c.readLoop()
	if err != nil {
		return 0, nil, err
	}
	r := &messageReader{
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

func (c *Conn) readLoop() (frameHeader, error) {
	for {
		h, err := readFrameHeader(c.br)
		if err != nil {
			return frameHeader{}, err
		}

		// TODO: verify the frame header

		switch h.opCode {
		case opClose, opPing, opPong:
			// TODO: handle control frames
		case opText, opBinary:
			return h, nil
		case opContinuation:
			// TODO: error handling
		default:
			// TODO: handle invalid opcode
		}
	}
}
