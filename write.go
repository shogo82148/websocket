package websocket

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

type messageWriter struct {
	ctx    context.Context
	conn   *Conn
	opCode opCode
	closed bool
}

func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	err := w.conn.writeFrame(w.ctx, false, w.opCode, p)
	if err != nil {
		return 0, err
	}
	w.opCode = opContinuation
	return len(p), nil
}

func (w *messageWriter) Close() error {
	if w.closed {
		return io.ErrClosedPipe
	}
	w.closed = true
	err := w.conn.writeFrame(w.ctx, true, w.opCode, nil)
	w.conn.writerMu.unlock()
	if err != nil {
		return err
	}
	return nil
}

// Writer returns a writer bounded by the context that will write a WebSocket message of type dataType to the connection.
//
// You must close the writer once you have written the entire message.
//
// Only one writer can be open at a time, multiple calls will block until the previous writer is closed.
func (c *Conn) Writer(ctx context.Context, messageType MessageType) (io.WriteCloser, error) {
	var opCode opCode
	switch messageType {
	case MessageText:
		opCode = opText
	case MessageBinary:
		opCode = opBinary
	default:
		return nil, fmt.Errorf("websocket: invalid message type: %s", messageType)
	}

	if err := c.writerMu.lock(ctx); err != nil {
		return nil, err
	}

	return &messageWriter{
		ctx:    ctx,
		conn:   c,
		opCode: opCode,
	}, nil
}

// Write writes a message to the connection.
func (c *Conn) Write(ctx context.Context, messageType MessageType, data []byte) error {
	var opCode opCode
	switch messageType {
	case MessageText:
		opCode = opText
	case MessageBinary:
		opCode = opBinary
	default:
		return fmt.Errorf("websocket: invalid message type: %s", messageType)
	}

	// Acquire the writer lock to ensure that only one writer is active at a time.
	if err := c.writerMu.lock(ctx); err != nil {
		return err
	}
	defer c.writerMu.unlock()

	if err := c.writeFrame(ctx, true, opCode, data); err != nil {
		return err
	}
	return nil
}

func (c *Conn) writeFrame(ctx context.Context, fin bool, opCode opCode, data []byte) error {
	if err := c.writeFrameMu.lock(ctx); err != nil {
		return err
	}
	defer c.writeFrameMu.unlock()

	// watch for context cancellation and close the connection if the context is canceled.
	if err := c.watchWriteCancel(ctx); err != nil {
		return err
	}
	defer c.finishWrite()

	h := frameHeader{
		fin:        fin,
		opCode:     opCode,
		mask:       c.client,
		payloadLen: int64(len(data)),
	}

	framePayload := data
	if h.mask {
		var maskKey [4]byte
		rand.Read(maskKey[:])
		h.maskKey = binary.BigEndian.Uint32(maskKey[:])
		framePayload = append([]byte(nil), data...)
		maskFramePayload(framePayload, h.maskKey)
	}

	if err := writeFrameHeader(c.bw, h); err != nil {
		if cerr := c.canceledWrite(); cerr != nil {
			return cerr
		}
		return err
	}
	if _, err := c.bw.Write(framePayload); err != nil {
		if cerr := c.canceledWrite(); cerr != nil {
			return cerr
		}
		return err
	}
	if fin {
		if err := c.bw.Flush(); err != nil {
			if cerr := c.canceledWrite(); cerr != nil {
				return cerr
			}
			return err
		}
	}
	return nil
}
