package websocket

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"unicode/utf8"
)

var writeBufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func getWriteBuf() *bytes.Buffer {
	buf := writeBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putWriteBuf(buf *bytes.Buffer) {
	writeBufPool.Put(buf)
}

type messageWriter struct {
	ctx    context.Context
	conn   *Conn
	opCode opCode
	closed bool
	flate  bool

	trimWriter  *trimLastFourBytesWriter
	flateWriter *flate.Writer
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
	w.flate = false
	if w.trimWriter != nil {
		w.trimWriter.reset()
	}
}

func (w *messageWriter) ensureFlate() error {
	if w.trimWriter == nil {
		w.trimWriter = &trimLastFourBytesWriter{
			w: writerFunc(w.write),
		}
	}
	if w.flateWriter == nil {
		flateWriter, err := flate.NewWriter(w.trimWriter, flate.DefaultCompression)
		if err != nil {
			return err
		}
		w.flateWriter = flateWriter
	}
	w.flate = true
	return nil
}

func (w *messageWriter) flateContextTakeover() bool {
	if w.conn.client {
		return !w.conn.copts.clientNoContextTakeover
	}
	return !w.conn.copts.serverNoContextTakeover
}

func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}

	// Compression can only be selected for the first fragment because RSV1 is
	// only valid on the first frame of a compressed message.
	if w.conn.flate() && w.opCode != opContinuation && len(p) >= w.conn.flateThreshold {
		if err := w.ensureFlate(); err != nil {
			return 0, err
		}
	}
	if w.flate {
		return w.flateWriter.Write(p)
	}
	return w.write(p)
}

func (w *messageWriter) write(p []byte) (int, error) {
	if err := w.conn.writeFrame(w.ctx, false, w.flate, w.opCode, p); err != nil {
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

	if w.flate {
		if err := w.flateWriter.Flush(); err != nil {
			w.conn.writerMu.unlock()
			return err
		}
	}
	err := w.conn.writeFrame(w.ctx, true, w.flate, w.opCode, nil)
	if w.flate && !w.flateContextTakeover() {
		w.flateWriter = nil
	}
	w.conn.writerMu.unlock()
	if err != nil {
		return err
	}
	return nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
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

	c.msgWriter.reset(ctx, opCode)
	w := io.WriteCloser(c.msgWriter)
	if opCode == opText && !c.skipValidateUTF8Write {
		c.utf8Writer.reset(ctx, w)
		w = c.utf8Writer
	}
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

	if opCode == opText && !c.skipValidateUTF8Write {
		if !utf8.Valid(data) {
			c.abnormalClosure(ctx, StatusInvalidFramePayloadData, "invalid UTF-8")
			return CloseError{
				Code:   StatusInvalidFramePayloadData,
				Reason: "invalid UTF-8",
			}
		}
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
	buf := getWriteBuf()
	defer putWriteBuf(buf)

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

	h := frameHeader{
		fin:        fin,
		rsv1:       flate && (opCode == opText || opCode == opBinary),
		opCode:     opCode,
		mask:       c.client,
		payloadLen: int64(len(data)),
	}

	if h.mask {
		// generate mask key
		buf := c.writeBuf[:]
		rand.Read(buf[:4])
		h.maskKey = binary.BigEndian.Uint32(buf[:4])
	}

	// Write the frame header.
	if err := c.writeFrameHeader(h); err != nil {
		if cerr := c.canceledWrite(); cerr != nil {
			return cerr
		}
		return err
	}

	// Write the frame payload.
	if h.mask {
		maskKey := h.maskKey
		for len(data) > 0 {
			// fill the available buffer.
			buf := c.bw.AvailableBuffer()
			l := min(cap(buf), len(data))
			buf = append(buf, data[:l]...)
			data = data[l:]

			// Mask the payload in place and write it to the buffer.
			maskKey = maskFramePayload(buf, maskKey)
			if _, err := c.bw.Write(buf); err != nil {
				if cerr := c.canceledWrite(); cerr != nil {
					return cerr
				}
				return err
			}

			if c.bw.Available() == 0 {
				if err := c.bw.Flush(); err != nil {
					if cerr := c.canceledWrite(); cerr != nil {
						return cerr
					}
					return err
				}
			}
		}
	} else {
		// Write the payload directly if not masked.
		if _, err := c.bw.Write(data); err != nil {
			if cerr := c.canceledWrite(); cerr != nil {
				return cerr
			}
			return err
		}
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
