package websocket

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// MessageType represents the type of a WebSocket message.
type MessageType int

const (
	// MessageText is for UTF-8 encoded text messages like JSON.
	MessageText MessageType = 1

	// MessageBinary is for binary messages like protobufs.
	MessageBinary MessageType = 2
)

func (t MessageType) String() string {
	switch t {
	case MessageText:
		return "text"
	case MessageBinary:
		return "binary"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

type Conn struct {
	_ noCopy

	rwc          io.ReadWriteCloser
	client       bool
	br           *bufio.Reader
	bw           *bufio.Writer
	writeFrameMu *mutex
	writerMu     *mutex

	// closing TCP connection state
	closing atomic.Bool
	closeMu sync.Mutex
	closed  chan struct{}
}

type connConfig struct {
	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer
}

func newConn(cfg connConfig) *Conn {
	closed := make(chan struct{})
	conn := &Conn{
		rwc:          cfg.rwc,
		client:       cfg.client,
		br:           cfg.br,
		bw:           cfg.bw,
		writeFrameMu: newMutex(closed),
		writerMu:     newMutex(closed),
		closed:       closed,
	}
	return conn
}

// Ping sends a ping to the peer and waits for a pong.
func (c *Conn) Ping(ctx context.Context) error {
	return errors.New("not implemented")
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

// mutex is a mutex that can be locked and unlocked with a context.Context.
type mutex struct {
	_      noCopy
	ch     chan struct{}
	closed <-chan struct{}
}

func newMutex(closed <-chan struct{}) *mutex {
	return &mutex{
		ch:     make(chan struct{}, 1),
		closed: closed,
	}
}

func (m *mutex) lock(ctx context.Context) error {
	select {
	case <-m.closed:
		return net.ErrClosed
	case <-ctx.Done():
		return fmt.Errorf("websocket: failed to acquire lock: %w", ctx.Err())
	case m.ch <- struct{}{}:
		// To make sure the connection is certainly alive.
		select {
		case <-m.closed:
			<-m.ch // unlock
			return net.ErrClosed
		default:
			return nil
		}
	}
}

func (m *mutex) unlock() {
	<-m.ch
}

// noCopy may be embedded into structs which must not be copied after the first use.
// ref. https://shogo82148.github.io/blog/2018/05/16/macopy-is-struct/
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
