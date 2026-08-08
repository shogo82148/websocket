package websocket

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
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
	*conn
}

type conn struct {
	_ noCopy

	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer

	// for synchronizing reads
	readerMu *mutex

	// for synchronizing writes
	writerMu     *mutex
	writeFrameMu *mutex

	// for handling context cancellation
	watcher     chan<- context.Context
	finished    chan<- struct{}
	canceledMu  sync.Mutex
	canceledErr error

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
	c := &Conn{
		conn: &conn{
			rwc:          cfg.rwc,
			client:       cfg.client,
			br:           cfg.br,
			bw:           cfg.bw,
			readerMu:     newMutex(closed),
			writerMu:     newMutex(closed),
			writeFrameMu: newMutex(closed),
			closed:       closed,
		},
	}
	runtime.AddCleanup(c, func(c *conn) {
		c.close()
	}, c.conn)
	c.startWatcher()
	return c
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

func (c *conn) startWatcher() {
	watcher := make(chan context.Context, 1)
	c.watcher = watcher

	finished := make(chan struct{})
	c.finished = finished

	closed := c.closed

	// watcher goroutine
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-closed:
				// connection closed, exit goroutine
				return
			}

			// wait for context cancellation
			select {
			case <-ctx.Done():
				c.cancel(ctx.Err())
			case <-finished:
			case <-closed:
				// connection closed, exit goroutine
				return
			}
		}
	}()
}

// watchCancel watches the context for cancellation and cancels the connection if the context is canceled.
func (c *conn) watchCancel(ctx context.Context) error {
	// check if the connection is already closed
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.watcher <- ctx
	return nil
}

func (c *conn) finish() {
	select {
	case c.finished <- struct{}{}:
	case <-c.closed:
	}
}

// cancel cancels the connection and unblocks all goroutines interacting with the connection.
func (c *conn) cancel(err error) {
	c.canceledMu.Lock()
	c.canceledErr = err
	c.canceledMu.Unlock()

	c.close()
}

// canceled returns the error that caused the connection to be canceled, or nil if the connection was not canceled.
func (c *conn) canceled() error {
	c.canceledMu.Lock()
	defer c.canceledMu.Unlock()
	return c.canceledErr
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
