package websocket

import (
	"bufio"
	"context"
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
	rwc          io.ReadWriteCloser
	client       bool
	br           *bufio.Reader
	bw           *bufio.Writer
	writeFrameMu *mutex
}

type connConfig struct {
	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer
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
	}
}

// Ping sends a ping to the peer and waits for a pong.
func (c *Conn) Ping(ctx context.Context) error {
	return errors.New("not implemented")
}

// Reader reads from the connection until there is a WebSocket data message to be read.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error) {
	return 0, nil, errors.New("not implemented")
}

// Read reads a single WebSocket message from the connection.
// It will handle ping, pong and close frames as appropriate.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	return 0, nil, errors.New("not implemented")
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

// mutex is a mutex that can be locked and unlocked with a context.Context.
type mutex struct {
	_  noCopy
	ch chan struct{}
}

func (m *mutex) lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("websocket: failed to acquire lock: %w", ctx.Err())
	case m.ch <- struct{}{}:
		return nil
	}
}

func (m *mutex) unlock() {
	select {
	case <-m.ch:
	}
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
