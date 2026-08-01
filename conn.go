package websocket

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

var ErrMessageTooBig = errors.New("websocket: message too big")

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
	subprotocol string

	readLimit int64

	onPingReceived func(ctx context.Context, payload []byte) bool
	onPongReceived func(ctx context.Context, payload []byte)

	writeMu sync.Mutex
	writerSem chan struct{}
	pongMu  sync.Mutex
	pongAck *pongAck

	closeReadMu  sync.Mutex
	closeReadCtx context.Context
}

type pongAck struct {
	payload []byte
	ch      chan []byte
}

type connConfig struct {
	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer
	subprotocol string

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
		rwc:            cfg.rwc,
		client:         cfg.client,
		br:             cfg.br,
		bw:             cfg.bw,
		subprotocol:    cfg.subprotocol,
		readLimit:      32768,
		writerSem:      make(chan struct{}, 1),
		onPingReceived: cfg.onPingReceived,
		onPongReceived: cfg.onPongReceived,
	}
}

// Ping sends a ping to the peer and waits for a pong.
func (c *Conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	payload := make([]byte, 8)
	if _, err := rand.Read(payload); err != nil {
		return err
	}
	ack := &pongAck{
		payload: append([]byte(nil), payload...),
		ch:      make(chan []byte, 1),
	}

	c.pongMu.Lock()
	if c.pongAck != nil {
		c.pongMu.Unlock()
		return errors.New("websocket: concurrent ping not supported")
	}
	c.pongAck = ack
	c.pongMu.Unlock()
	defer c.clearPongAck(ack)

	if err := c.writeFrame(opPing, payload); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pongPayload := <-ack.ch:
			if bytes.Equal(pongPayload, payload) {
				return nil
			}
		}
	}
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
			if err := c.checkReadLimit(int64(len(framePayload))); err != nil {
				return 0, nil, err
			}
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
			if err := c.checkReadLimit(int64(len(framePayload))); err != nil {
				return 0, nil, err
			}
			if header.fin {
				return messageType, bytes.NewReader(framePayload), nil
			}
			fragmented = true
			messagePayload.Reset()
			if _, err := messagePayload.Write(framePayload); err != nil {
				return 0, nil, err
			}
		case opContinuation:
			if err := c.checkReadLimit(int64(messagePayload.Len()) + int64(len(framePayload))); err != nil {
				return 0, nil, err
			}
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
			c.ackPong(framePayload)
			continue
		case opClose:
			return 0, nil, c.handleCloseFrame(framePayload)
		}
	}
}

func (c *Conn) ackPong(payload []byte) {
	c.pongMu.Lock()
	ack := c.pongAck
	c.pongMu.Unlock()
	if ack == nil {
		return
	}

	pongPayload := append([]byte(nil), payload...)
	select {
	case ack.ch <- pongPayload:
	default:
	}
}

func (c *Conn) clearPongAck(ack *pongAck) {
	c.pongMu.Lock()
	defer c.pongMu.Unlock()
	if c.pongAck == ack {
		c.pongAck = nil
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
	c.readLimit = limit
}

// Subprotocol returns the negotiated subprotocol.
// An empty string means the default protocol.
func (c *Conn) Subprotocol() string {
	return c.subprotocol
}

func (c *Conn) Close(code StatusCode, reason string) error {
	payload, err := closePayload(code, reason)
	if err != nil {
		return err
	}
	if err := c.writeFrame(opClose, payload); err != nil {
		return err
	}
	return c.rwc.Close()
}

func (c *Conn) CloseNow() error {
	return c.rwc.Close()
}

func (c *Conn) CloseRead(ctx context.Context) context.Context {
	c.closeReadMu.Lock()
	if c.closeReadCtx != nil {
		closeReadCtx := c.closeReadCtx
		c.closeReadMu.Unlock()
		return closeReadCtx
	}
	closeReadCtx, cancel := context.WithCancel(ctx)
	c.closeReadCtx = closeReadCtx
	c.closeReadMu.Unlock()

	go func() {
		defer cancel()
		for {
			_, reader, err := c.Reader(closeReadCtx)
			if err != nil {
				return
			}
			if _, err := io.Copy(io.Discard, reader); err != nil {
				return
			}
		}
	}()

	return closeReadCtx
}

func (c *Conn) handleCloseFrame(payload []byte) error {
	err := parseClosePayload(payload)
	responsePayload := payload
	if err != nil && CloseStatus(err) == -1 {
		protocolPayload, protocolErr := closePayload(StatusProtocolError, "")
		if protocolErr == nil {
			responsePayload = protocolPayload
		}
	}

	writeErr := c.writeFrame(opClose, responsePayload)
	closeErr := c.rwc.Close()
	if err != nil {
		return err
	}
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (c *Conn) checkReadLimit(messageSize int64) error {
	if c.readLimit >= 0 && messageSize > c.readLimit {
		if err := c.Close(StatusMessageTooBig, ""); err != nil {
			return errors.Join(ErrMessageTooBig, err)
		}
		return ErrMessageTooBig
	}
	return nil
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

func closePayload(code StatusCode, reason string) ([]byte, error) {
	if code == StatusNoStatusReceived {
		if reason != "" {
			return nil, errors.New("websocket: close reason requires a status code")
		}
		return nil, nil
	}
	if !validCloseCode(code) {
		return nil, fmt.Errorf("websocket: invalid close code: %d", code)
	}
	if len(reason) > 123 {
		return nil, errors.New("websocket: close reason too long")
	}

	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[:2], uint16(code))
	copy(payload[2:], reason)
	return payload, nil
}

func validCloseCode(code StatusCode) bool {
	if code >= 1000 && code <= 1015 {
		switch code {
		case StatusNoStatusReceived, StatusAbnormalClosure, StatusTLSHandshake:
			return false
		default:
			return true
		}
	}
	return code >= 3000 && code <= 4999
}
