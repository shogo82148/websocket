package websocket

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

type messageWriter struct {
	ctx         context.Context
	conn        *Conn
	messageType MessageType
}

// Writer returns a writer bounded by the context that will write a WebSocket message of type dataType to the connection.
//
// You must close the writer once you have written the entire message.
//
// Only one writer can be open at a time, multiple calls will block until the previous writer is closed.
func (c *Conn) Writer(ctx context.Context, messageType MessageType) (io.WriteCloser, error) {
	return nil, errors.New("not implemented")
}

func (c *Conn) writeFrame(opCode opCode, data []byte) error {
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

	var opCode opCode
	switch messageType {
	case MessageText:
		opCode = opText
	case MessageBinary:
		opCode = opBinary
	default:
		return errors.New("websocket: invalid message type")
	}
	return c.writeFrame(opCode, data)
}
