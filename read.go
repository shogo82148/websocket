package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
)

type messageReader struct {
	ctx  context.Context
	conn *Conn

	fin        bool
	mask       bool
	payloadLen int64
	maskKey    uint32
	closed     bool
}

func newMessageReader(conn *Conn) *messageReader {
	return &messageReader{
		conn: conn,
	}
}

func (r *messageReader) reset(ctx context.Context, h frameHeader) {
	r.ctx = ctx
	r.closed = false
	r.setHeader(h)
}

func (r *messageReader) setHeader(h frameHeader) {
	r.fin = h.fin
	r.payloadLen = h.payloadLen
	r.mask = h.mask
	r.maskKey = h.maskKey
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
	if r.mask {
		r.maskKey = maskFramePayload(p[:n], r.maskKey)
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

	c.msgReader.reset(ctx, h)
	r := io.Reader(c.msgReader)
	if flate := h.rsv1; flate {
		c.flateReader.reset(r, c.flateReadContextTakeover())
		r = c.flateReader
	}
	lr := c.limitReader
	lr.reset(ctx, r)
	return MessageType(h.opCode), lr, nil
}

func (c *Conn) flateReadContextTakeover() bool {
	if c.client {
		return !c.copts.serverNoContextTakeover
	}
	return !c.copts.clientNoContextTakeover
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

// SetReadLimit sets the max number of bytes to read for a single message.
// It applies to the Reader and Read methods.
//
// By default, the connection has a message read limit of 32768 bytes.
//
// When the limit is hit, reads return an error wrapping ErrMessageTooBig and the connection is closed with StatusMessageTooBig.
//
// Set to -1 to disable.
func (c *Conn) SetReadLimit(limit int64) {
	c.limitReader.limit.Store(limit)
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
	c.closeReadOnce.Do(func() {
		var cancel context.CancelFunc
		c.closeReadCtx, cancel = context.WithCancel(ctx)

		go func() {
			defer cancel()
			defer c.close()

			_, _, err := c.Reader(c.closeReadCtx)
			if err == nil {
				_ = c.Close(StatusPolicyViolation, "unexpected data message")
			}
		}()
	})
	return c.closeReadCtx
}

func (c *Conn) readLoop(ctx context.Context) (frameHeader, error) {
	for {
		h, err := readFrameHeader(c.br)
		if err != nil {
			return frameHeader{}, err
		}

		// verify the frame header
		if !c.client && !h.mask {
			return frameHeader{}, errors.New("websocket: received unmasked frame from client")
		}

		switch h.opCode {
		case opClose, opPing, opPong:
			if err := c.handleControlFrame(ctx, h); err != nil {
				return frameHeader{}, err
			}
		case opContinuation, opText, opBinary:
			return h, nil
		default:
			c.abnormalClosure(ctx, StatusProtocolError, "received unknown opcode")
			return frameHeader{}, fmt.Errorf("websocket: received unknown opcode: %d", h.opCode)
		}
	}
}

func (c *Conn) handleControlFrame(ctx context.Context, h frameHeader) error {
	// validate control frame
	if h.payloadLen < 0 || h.payloadLen > maxControlPayload {
		c.abnormalClosure(ctx, StatusProtocolError, "control frame payload length is invalid")
		return fmt.Errorf("websocket: control frame payload length is invalid: %d", h.payloadLen)
	}
	if !h.fin {
		c.abnormalClosure(ctx, StatusProtocolError, "control frame is fragmented")
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
			c.abnormalClosure(ctx, StatusProtocolError, "received invalid close payload")
			return err
		}
		c.closeReceived.Store(&ce)
		if err := c.writeClose(ctx, ce.Code, ce.Reason); err != nil {
			return err
		}
		return ce
	case opPing:
		if c.onPingReceived != nil && !c.onPingReceived(ctx, buf) {
			return nil
		}
		return c.writeFrame(ctx, true, false, opPong, buf)
	case opPong:
		if c.onPongReceived != nil {
			c.onPongReceived(ctx, buf)
		}
		c.handlePong(buf)
	default:
		c.abnormalClosure(ctx, StatusProtocolError, "received unknown opcode")
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
	if lr.n >= 0 {
		lr.n++ // add 1 to detect limit exceeded
	}
	lr.r = r
}

func (lr *limitReader) Read(p []byte) (int, error) {
	if lr.n < 0 {
		// no limit
		return lr.r.Read(p)
	}

	if int64(len(p)) > lr.n {
		p = p[:lr.n]
	}
	n, err := lr.r.Read(p)
	lr.n -= int64(n)
	if lr.n < 0 {
		lr.n = 0
	}
	if lr.n == 0 {
		lr.c.abnormalClosure(lr.ctx, StatusMessageTooBig, "read limit")
		return 0, ErrMessageTooBig
	}
	return n, err
}

type flateReader struct {
	flateReader io.Reader
	flateTail   strings.Reader
	dict        slidingWindow
	takeover    bool
}

func (fr *flateReader) reset(r io.Reader, takeover bool) {
	fr.flateTail.Reset(deflateMessageTail)
	r = io.MultiReader(r, &fr.flateTail)
	fr.takeover = takeover
	if takeover {
		fr.dict.init(32 * 1024)
		fr.flateReader = getFlateReader(r, fr.dict.buf)
		return
	}
	fr.flateReader = getFlateReader(r, nil)
}

func (fr *flateReader) Read(p []byte) (int, error) {
	if fr.flateReader == nil {
		return 0, net.ErrClosed
	}
	n, err := fr.flateReader.Read(p)
	if fr.takeover {
		fr.dict.write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		fr.close()
	}
	return n, err
}

func (fr *flateReader) close() {
	if fr.flateReader != nil {
		putFlateReader(fr.flateReader)
		fr.flateReader = nil
	}
}
