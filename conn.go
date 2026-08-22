package websocket

import (
	"bufio"
	"context"
	"crypto/rand"
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
	_ noCopy
	*conn

	// subprotocol is the subprotocol negotiated during the handshake.
	subprotocol string

	// skipValidateUTF8Read is true if the connection should skip UTF-8 validation for text messages when reading.
	skipValidateUTF8Read bool

	// skipValidateUTF8Write is true if the connection should skip UTF-8 validation for text messages when writing.
	skipValidateUTF8Write bool

	// for handling compression
	copts          *compressionOptions
	flateThreshold int
	flateLevel     int

	// for synchronizing reads
	readerMu      *mutex
	msgReader     *messageReader
	flateReader   *flateReader
	limitReader   *limitReader
	utf8Reader    *utf8Reader
	closeReadOnce sync.Once
	closeReadCtx  context.Context

	// for synchronizing writes
	writerMu     *mutex
	writeFrameMu *mutex
	msgWriter    *messageWriter
	utf8Writer   *utf8Writer

	// for handling ping and pong control frames
	onPingReceived func(context.Context, []byte) bool
	onPongReceived func(context.Context, []byte)
	pingMu         sync.Mutex
	pings          map[string]chan struct{}
}

type conn struct {
	_ noCopy

	rwc    io.ReadWriteCloser
	client bool
	br     *bufio.Reader
	bw     *bufio.Writer

	// for handling context cancellation
	readWatcher      chan<- context.Context
	readFinished     chan<- struct{}
	readBuf          [8]byte
	readCanceledMu   sync.Mutex
	readCanceledErr  error
	writeWatcher     chan<- context.Context
	writeFinished    chan<- struct{}
	writeBuf         [8]byte
	writeCanceledMu  sync.Mutex
	writeCanceledErr error

	// closing TCP connection state
	closing       atomic.Bool
	closeSent     atomic.Bool
	closeReceived atomic.Pointer[CloseError]
	closeMu       sync.Mutex
	closed        chan struct{}
}

type connConfig struct {
	rwc                   io.ReadWriteCloser
	client                bool
	subprotocol           string
	skipValidateUTF8Read  bool
	skipValidateUTF8Write bool
	onPingReceived        func(context.Context, []byte) bool
	onPongReceived        func(context.Context, []byte)

	br *bufio.Reader
	bw *bufio.Writer
}

func newConn(cfg connConfig) *Conn {
	closed := make(chan struct{})
	c := &Conn{
		conn: &conn{
			rwc:    cfg.rwc,
			client: cfg.client,

			br: cfg.br,
			bw: cfg.bw,

			closed: closed,
		},
		subprotocol:           cfg.subprotocol,
		skipValidateUTF8Read:  cfg.skipValidateUTF8Read,
		skipValidateUTF8Write: cfg.skipValidateUTF8Write,
		onPingReceived:        cfg.onPingReceived,
		onPongReceived:        cfg.onPongReceived,
		pings:                 make(map[string]chan struct{}),

		readerMu:     newMutex(closed),
		writerMu:     newMutex(closed),
		writeFrameMu: newMutex(closed),
	}

	c.msgReader = newMessageReader(c)
	c.limitReader = newLimitReader(c, 32*1024) // default read limit is 32KiB
	c.utf8Reader = &utf8Reader{conn: c}
	c.msgWriter = newMessageWriter(c)
	c.utf8Writer = &utf8Writer{conn: c}

	runtime.AddCleanup(c, func(c *conn) {
		_ = c.close()
	}, c.conn)
	c.startReadWatcher()
	c.startWriteWatcher()
	return c
}

func (c *Conn) initCompression(copts *compressionOptions, flateThreshold, flateLevel int) {
	c.copts = copts
	c.flateThreshold = flateThreshold
	c.flateLevel = flateLevel

	if c.flate() {
		c.flateReader = new(flateReader)
	}

	if c.flate() && c.flateThreshold == 0 {
		var flateContextTakeover bool
		if c.client {
			flateContextTakeover = !c.copts.clientNoContextTakeover
		} else {
			flateContextTakeover = !c.copts.serverNoContextTakeover
		}

		if flateContextTakeover {
			c.flateThreshold = 128
		} else {
			c.flateThreshold = 512
		}
	}
}

// flate returns true if the connection is using permessage-deflate compression.
func (c *Conn) flate() bool {
	return c.copts != nil
}

// Ping sends a ping to the peer and waits for a pong.
func (c *Conn) Ping(ctx context.Context) error {
	var payload [8]byte
	var pong chan struct{}
	var key string

	c.pingMu.Lock()
	for {
		if _, err := rand.Read(payload[:]); err != nil {
			c.pingMu.Unlock()
			return fmt.Errorf("websocket: failed to generate ping payload: %w", err)
		}
		key = string(payload[:])
		if _, exists := c.pings[key]; !exists {
			pong = make(chan struct{})
			c.pings[key] = pong
			break
		}
	}
	c.pingMu.Unlock()

	defer func() {
		c.pingMu.Lock()
		delete(c.pings, key)
		c.pingMu.Unlock()
	}()

	if err := c.writeFrame(ctx, true, false, opPing, payload[:]); err != nil {
		return err
	}

	select {
	case <-pong:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return net.ErrClosed
	}
}

func (c *Conn) handlePong(payload []byte) {
	c.pingMu.Lock()
	pong := c.pings[string(payload)]
	if pong != nil {
		delete(c.pings, string(payload))
		close(pong)
	}
	c.pingMu.Unlock()
}

// Subprotocol returns the negotiated subprotocol.
// An empty string means the default protocol.
func (c *Conn) Subprotocol() string {
	return c.subprotocol
}

func (c *conn) startReadWatcher() {
	watcher := make(chan context.Context, 1)
	c.readWatcher = watcher

	finished := make(chan struct{})
	c.readFinished = finished

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
				c.cancelRead(ctx.Err())
			case <-finished:
			case <-closed:
				// connection closed, exit goroutine
				return
			}
		}
	}()
}

// watchReadCancel watches the context for cancellation and cancels the connection if the context is canceled.
func (c *conn) watchReadCancel(ctx context.Context) error {
	// check if the connection is already closed
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.readWatcher <- ctx
	return nil
}

func (c *conn) finishRead() {
	select {
	case c.readFinished <- struct{}{}:
	case <-c.closed:
	}
}

// cancelRead cancels the connection and unblocks all goroutines interacting with the connection.
func (c *conn) cancelRead(err error) {
	c.readCanceledMu.Lock()
	c.readCanceledErr = err
	c.readCanceledMu.Unlock()

	c.close()
}

// canceledRead returns the error that caused the connection to be canceledRead, or nil if the connection was not canceledRead.
func (c *conn) canceledRead() error {
	c.readCanceledMu.Lock()
	defer c.readCanceledMu.Unlock()
	return c.readCanceledErr
}

func (c *conn) startWriteWatcher() {
	watcher := make(chan context.Context, 1)
	c.writeWatcher = watcher

	finished := make(chan struct{})
	c.writeFinished = finished

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
				c.cancelWrite(ctx.Err())
			case <-finished:
			case <-closed:
				// connection closed, exit goroutine
				return
			}
		}
	}()
}

// watchWriteCancel watches the context for cancellation and cancels the connection if the context is canceled.
func (c *conn) watchWriteCancel(ctx context.Context) error {
	// check if the connection is already closed
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.writeWatcher <- ctx
	return nil
}

func (c *conn) finishWrite() {
	select {
	case c.writeFinished <- struct{}{}:
	case <-c.closed:
	}
}

// cancelWrite cancels the connection and unblocks all goroutines interacting with the connection.
func (c *conn) cancelWrite(err error) {
	c.writeCanceledMu.Lock()
	c.writeCanceledErr = err
	c.writeCanceledMu.Unlock()

	c.close()
}

// canceledWrite returns the error that caused the connection to be canceledWrite, or nil if the connection was not canceledWrite.
func (c *conn) canceledWrite() error {
	c.writeCanceledMu.Lock()
	defer c.writeCanceledMu.Unlock()
	return c.writeCanceledErr
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
		case <-ctx.Done():
			<-m.ch // unlock
			return fmt.Errorf("websocket: failed to acquire lock: %w", ctx.Err())
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
