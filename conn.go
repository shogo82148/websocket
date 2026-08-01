package websocket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MessageType represents the type of a WebSocket message.
type MessageType int

const (
	// MessageText is for UTF-8 encoded text messages like JSON.
	MessageText MessageType = 1

	// MessageBinary is for binary messages like protobufs.
	MessageBinary MessageType = 2
)

type Conn struct {
	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer

	onPingReceived func(ctx context.Context, payload []byte) bool
	onPongReceived func(ctx context.Context, payload []byte)
}

type connConfig struct {
	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer

	onPingReceived func(ctx context.Context, payload []byte) bool
	onPongReceived func(ctx context.Context, payload []byte)
}

// StatusCode represents a WebSocket status code.
type StatusCode int

const (
	StatusNormalClosure   StatusCode = 1000
	StatusGoingAway       StatusCode = 1001
	StatusProtocolError   StatusCode = 1002
	StatusUnsupportedData StatusCode = 1003

	// 1004 is reserved.

	StatusNoStatusReceived        StatusCode = 1005
	StatusAbnormalClosure         StatusCode = 1006
	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalError           StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014
	StatusTLSHandshake            StatusCode = 1015
)

// CloseError is returned when the connection is closed with a status and reason.
type CloseError struct {
	Code   StatusCode
	Reason string
}

func (err CloseError) Error() string {
	return fmt.Sprintf("websocket: close %d (%s)", err.Code, err.Reason)
}

// CloseStatus returns the status code from the given error if it is a CloseError.
//
// -1 will be returned if the passed error is nil or not a CloseError.
func CloseStatus(err error) StatusCode {
	if ce, ok := errors.AsType[CloseError](err); ok {
		return ce.Code
	}
	return -1
}

func newConn(cfg connConfig) *Conn {
	return &Conn{
		rwc:    cfg.rwc,
		client: cfg.client,
		br:     cfg.br,
		bw:     cfg.bw,
		onPingReceived: cfg.onPingReceived,
		onPongReceived: cfg.onPongReceived,
	}
}

// Ping sends a ping to the peer and waits for a pong.
func (c *Conn) Ping(ctx context.Context) error {
	return errors.New("not implemented")
}

// Reader reads from the connection until there is a WebSocket data message to be read.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error) {
	var buf [8]byte
	var messagePayload bytes.Buffer
	var messageType MessageType
	fragmented := false

	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}

		header, framePayload, err := readFrame(c.br, buf[:])
		if err != nil {
			return 0, nil, err
		}
		if err := c.validateFrameHeader(header, fragmented); err != nil {
			return 0, nil, err
		}

		switch header.opCode {
		case opText:
			messageType = MessageText
			if header.fin {
				return messageType, bytes.NewReader(framePayload), nil
			}
			fragmented = true
			messagePayload.Reset()
			if _, err := messagePayload.Write(framePayload); err != nil {
				return 0, nil, err
			}
		case opBinary:
			messageType = MessageBinary
			if header.fin {
				return messageType, bytes.NewReader(framePayload), nil
			}
			fragmented = true
			messagePayload.Reset()
			if _, err := messagePayload.Write(framePayload); err != nil {
				return 0, nil, err
			}
		case opContinuation:
			if _, err := messagePayload.Write(framePayload); err != nil {
				return 0, nil, err
			}
			if header.fin {
				fragmented = false
				return messageType, bytes.NewReader(messagePayload.Bytes()), nil
			}
		case opPing:
			sendPong := true
			if c.onPingReceived != nil {
				sendPong = c.onPingReceived(ctx, append([]byte(nil), framePayload...))
			}
			if sendPong {
				if err := c.writeFrame(opPong, framePayload); err != nil {
					return 0, nil, err
				}
			}
			continue
		case opPong:
			if c.onPongReceived != nil {
				c.onPongReceived(ctx, append([]byte(nil), framePayload...))
			}
			continue
		case opClose:
			return 0, nil, parseClosePayload(framePayload)
		}
	}
}

// Read reads a single WebSocket message from the connection.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	messageType, reader, err := c.Reader(ctx)
	if err != nil {
		return 0, nil, err
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		return 0, nil, err
	}
	return messageType, payload, nil
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
	// TODO: implement SetReadLimit
}

// Subprotocol returns the negotiated subprotocol.
// An empty string means the default protocol.
func (c *Conn) Subprotocol() string {
	return ""
}

func (c *Conn) Close(code StatusCode, reason string) error {
	return errors.New("not implemented")
}

func (c *Conn) CloseNow() error {
	return errors.New("not implemented")
}

func (c *Conn) CloseRead(ctx context.Context) context.Context {
	// TODO: implement CloseRead
	return ctx
}

func (c *Conn) validateFrameHeader(header frameHeader, fragmented bool) error {
	if header.rsv1 || header.rsv2 || header.rsv3 {
		return errors.New("websocket: unsupported reserved bits")
	}
	if header.mask != !c.client {
		return errors.New("websocket: invalid frame masking")
	}

	switch header.opCode {
	case opText, opBinary:
		if fragmented {
			return errors.New("websocket: unexpected data frame in fragmented message")
		}
	case opClose, opPing, opPong:
		if !header.fin {
			return errors.New("websocket: fragmented control frames are not allowed")
		}
		if header.payloadLen > 125 {
			return errors.New("websocket: control frame payload too large")
		}
	case opContinuation:
		if !fragmented {
			return errors.New("websocket: unexpected continuation frame")
		}
	default:
		return errors.New("websocket: unknown opcode")
	}

	return nil
}

func parseClosePayload(payload []byte) error {
	if len(payload) == 0 {
		return CloseError{Code: StatusNoStatusReceived}
	}
	if len(payload) == 1 {
		return errors.New("websocket: invalid close payload")
	}

	return CloseError{
		Code:   StatusCode(binary.BigEndian.Uint16(payload[:2])),
		Reason: string(payload[2:]),
	}
}
