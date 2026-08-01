package websocket

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

// Writer returns a writer bounded by the context that will write a WebSocket message of type dataType to the connection.
//
// You must close the writer once you have written the entire message.
//
// Only one writer can be open at a time, multiple calls will block until the previous writer is closed.
func (c *Conn) Writer(ctx context.Context, messageType MessageType) (io.WriteCloser, error) {
	return nil, errors.New("not implemented")
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
		return errors.New("websocket: invalid message type")
	}
	return c.writeFrame(ctx, opCode, data)
}

func (c *Conn) writeFrame(ctx context.Context, opCode opCode, data []byte) error {
	if err := c.writeFrameMu.lock(ctx); err != nil {
		return err
	}
	defer c.writeFrameMu.unlock()

	h := frameHeader{
		fin:        true,
		opCode:     opCode,
		mask:       c.client,
		payloadLen: int64(len(data)),
	}

	framePayload := data
	if h.mask {
		var maskKey [4]byte
		rand.Read(maskKey[:])
		h.maskKey = binary.NativeEndian.Uint32(maskKey[:])
		framePayload = append([]byte(nil), data...)
		maskFramePayload(framePayload, h.maskKey)
	}

	if err := writeFrameHeader(c.bw, h); err != nil {
		return err
	}
	if _, err := c.bw.Write(framePayload); err != nil {
		return err
	}
	return c.bw.Flush()
}
