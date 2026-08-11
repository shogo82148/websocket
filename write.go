package websocket

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type messageWriter struct {
	ctx    context.Context
	conn   *Conn
	opCode opCode
	closed bool
}

func newMessageWriter(conn *Conn) *messageWriter {
	return &messageWriter{
		conn: conn,
	}
}

func (w *messageWriter) reset(ctx context.Context, opCode opCode) {
	w.ctx = ctx
	w.opCode = opCode
	w.closed = false
}

func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	err := w.conn.writeFrame(w.ctx, false, false, w.opCode, p)
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
	err := w.conn.writeFrame(w.ctx, true, false, w.opCode, nil)
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

	w := c.msgWriter
	w.reset(ctx, opCode)
	return w, nil
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

	if c.flate() && len(data) >= c.flateThreshold {
		return c.writeCompressedFrame(ctx, opCode, data)
	}

	return c.writeFrame(ctx, true, false, opCode, data)
}

// writeCompressedFrame writes a compressed frame to the connection.
func (c *Conn) writeCompressedFrame(ctx context.Context, opCode opCode, data []byte) error {
	buf := new(bytes.Buffer)
	flateWriter, err := flate.NewWriter(buf, flate.DefaultCompression)
	if err != nil {
		return err
	}
	if _, err := flateWriter.Write(data); err != nil {
		return err
	}
	if err := flateWriter.Flush(); err != nil {
		return err
	}

	compressed := buf.Bytes()
	compressed = bytes.TrimSuffix(compressed, deflateMessageTailBytes)
	return c.writeFrame(ctx, true, true, opCode, compressed)
}

// writeFrame writes a frame to the connection.
func (c *Conn) writeFrame(ctx context.Context, fin, flate bool, opCode opCode, data []byte) error {
	if err := c.writeFrameMu.lock(ctx); err != nil {
		return err
	}
	defer c.writeFrameMu.unlock()

	// watch for context cancellation and close the connection if the context is canceled.
	if err := c.watchWriteCancel(ctx); err != nil {
		return err
	}
	defer c.finishWrite()

	if flate && (opCode != opText && opCode != opBinary) {
		return errors.New("websocket: cannot compress non-text/binary frame")
	}

	h := frameHeader{
		fin:        fin,
		rsv1:       flate,
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
