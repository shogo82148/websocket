package websocket

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

type messageWriter struct {
	ctx         context.Context
	conn        *Conn
	messageType MessageType

	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

// Writer returns a writer bounded by the context that will write a WebSocket message of type dataType to the connection.
//
// You must close the writer once you have written the entire message.
//
// Only one writer can be open at a time, multiple calls will block until the previous writer is closed.
func (c *Conn) Writer(ctx context.Context, messageType MessageType) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validMessageType(messageType) {
		return nil, errors.New("websocket: invalid message type")
	}

	select {
	case c.writerSem <- struct{}{}:
		return &messageWriter{
			ctx:         ctx,
			conn:        c,
			messageType: messageType,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *messageWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.buf.Write(p)
}

func (w *messageWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	if err := w.ctx.Err(); err != nil {
		w.closed = true
		w.mu.Unlock()
		<-w.conn.writerSem
		return err
	}
	data := append([]byte(nil), w.buf.Bytes()...)
	w.closed = true
	w.mu.Unlock()
	defer func() {
		<-w.conn.writerSem
	}()

	var opCode opCode
	switch w.messageType {
	case MessageText:
		opCode = opText
	case MessageBinary:
		opCode = opBinary
	default:
		return errors.New("websocket: invalid message type")
	}
	return w.conn.writeFrame(opCode, data)
}

func (c *Conn) writeFrame(opCode opCode, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	header := frameHeader{
		fin:        true,
		opCode:     opCode,
		mask:       c.client,
		payloadLen: int64(len(data)),
	}
	framePayload := data
	if header.mask {
		var maskKey [4]byte
		if _, err := rand.Read(maskKey[:]); err != nil {
			return err
		}
		header.maskKey = binary.BigEndian.Uint32(maskKey[:])
		framePayload = append([]byte(nil), data...)
		maskFramePayload(framePayload, header.maskKey)
	}

	var buf [8]byte
	if err := writeFrameHeader(c.bw, header, buf[:]); err != nil {
		return err
	}
	if _, err := c.bw.Write(framePayload); err != nil {
		return err
	}
	return c.bw.Flush()
}

// Write writes a message to the connection.
func (c *Conn) Write(ctx context.Context, messageType MessageType, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !validMessageType(messageType) {
		return errors.New("websocket: invalid message type")
	}

	var opCode opCode
	if messageType == MessageText {
		opCode = opText
	} else {
		opCode = opBinary
	}
	return c.writeFrame(opCode, data)
}

func validMessageType(messageType MessageType) bool {
	return messageType == MessageText || messageType == MessageBinary
}
