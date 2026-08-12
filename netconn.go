package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// NetConn converts a *websocket.Conn into a net.Conn.
//
// It's for tunneling arbitrary protocols over WebSockets.
// Few users of the library will need this but it's tricky to implement
// correctly and so provided in the library.
//
// Every Write to the net.Conn will correspond to a message write of
// the given type on *websocket.Conn.
//
// The passed ctx bounds the lifetime of the net.Conn. If cancelled,
// all reads and writes on the net.Conn will be cancelled.
//
// If a message is read that is not of the correct type, the connection
// will be closed with StatusUnsupportedData and an error will be returned.
//
// Close will close the *websocket.Conn with StatusNormalClosure.
//
// When a deadline is hit and there is an active read or write goroutine, the
// connection will be closed. This is different from most net.Conn implementations
// where only the reading/writing goroutines are interrupted but the connection
// is kept alive.
//
// The Addr methods will return the real addresses for connections obtained
// from websocket.Accept. But for connections obtained from websocket.Dial, a mock net.Addr
// will be returned that gives "websocket" for Network() and "websocket/unknown-addr" for
// String(). This is because websocket.Dial only exposes a io.ReadWriteCloser instead of the
// full net.Conn to us.
//
// A received StatusNormalClosure or StatusGoingAway close frame will be translated to
// io.EOF when reading.
//
// Furthermore, the ReadLimit is set to -1 to disable it.
func NetConn(ctx context.Context, c *Conn, msgType MessageType) net.Conn {
	c.SetReadLimit(-1)

	nc := &netConn{
		c:       c,
		msgType: msgType,
	}
	nc.readDeadline.init(ctx, &nc.readMu)
	nc.writeDeadline.init(ctx, &nc.writeMu)
	return nc
}

type netConn struct {
	c       *Conn
	msgType MessageType

	readMu       sync.Mutex
	readDeadline netConnDeadline
	reader       io.Reader

	writeMu       sync.Mutex
	writeDeadline netConnDeadline
}

var _ net.Conn = (*netConn)(nil)

func (nc *netConn) Write(p []byte) (int, error) {
	nc.writeMu.Lock()
	defer nc.writeMu.Unlock()

	if nc.writeDeadline.expired.Load() {
		return 0, fmt.Errorf("websocket: failed to write: %w", context.DeadlineExceeded)
	}

	err := nc.c.Write(nc.writeDeadline.ctx, nc.msgType, p)
	if err != nil {
		if nc.writeDeadline.expired.Load() {
			return 0, fmt.Errorf("websocket: failed to write: %w", context.DeadlineExceeded)
		}
		return 0, err
	}
	return len(p), nil
}

func (nc *netConn) Read(p []byte) (int, error) {
	nc.readMu.Lock()
	defer nc.readMu.Unlock()

	for {
		n, err := nc.read(p)
		if err != nil {
			if nc.readDeadline.expired.Load() {
				return n, fmt.Errorf("websocket: failed to read: %w", context.DeadlineExceeded)
			}
			return n, err
		}
		if n <= 0 {
			continue
		}
		return n, nil
	}
}

func (nc *netConn) read(p []byte) (int, error) {
	if nc.readDeadline.expired.Load() {
		return 0, fmt.Errorf("websocket: failed to read: %w", context.DeadlineExceeded)
	}

	if nc.reader == nil {
		typ, r, err := nc.c.Reader(nc.readDeadline.ctx)
		if err != nil {
			if ce, ok := errors.AsType[CloseError](err); ok {
				switch ce.Code {
				case StatusNormalClosure, StatusGoingAway:
					return 0, io.EOF
				}
			}
			return 0, err
		}
		if typ != nc.msgType {
			nc.c.Close(StatusUnsupportedData, "unsupported message type")
			return 0, errors.New("unsupported message type")
		}
		nc.reader = r
	}
	n, err := nc.reader.Read(p)
	if errors.Is(err, io.EOF) {
		nc.reader = nil
		err = nil
	}
	return n, err
}

func (nc *netConn) Close() error {
	nc.readDeadline.close()
	nc.writeDeadline.close()
	return nc.c.Close(StatusNormalClosure, "normal closure")
}

type netConnDeadline struct {
	mu       sync.Mutex
	deadline time.Time
	timer    *time.Timer
	opMu     *sync.Mutex

	ctx     context.Context
	cancel  context.CancelFunc
	expired atomic.Bool
}

func (d *netConnDeadline) init(ctx context.Context, opMu *sync.Mutex) {
	d.opMu = opMu
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.timer = time.AfterFunc(time.Hour, d.fire)
	d.timer.Stop()
}

func (d *netConnDeadline) fire() {
	d.mu.Lock()
	if d.deadline.IsZero() {
		d.mu.Unlock()
		return
	}
	if until := time.Until(d.deadline); until > 0 {
		d.timer.Reset(until)
		d.mu.Unlock()
		return
	}
	d.expired.Store(true)
	d.mu.Unlock()

	if !d.opMu.TryLock() {
		// Conn can only interrupt an in-flight operation by closing the
		// underlying connection through context cancellation.
		d.cancel()
		return
	}
	d.opMu.Unlock()
}

func (d *netConnDeadline) set(t time.Time) {
	d.mu.Lock()
	d.deadline = t
	d.expired.Store(false)
	d.timer.Stop()
	if t.IsZero() {
		d.mu.Unlock()
		return
	}
	until := time.Until(t)
	if until > 0 {
		d.timer.Reset(until)
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()
	d.fire()
}

func (d *netConnDeadline) close() {
	d.mu.Lock()
	d.deadline = time.Time{}
	d.timer.Stop()
	d.mu.Unlock()
	d.cancel()
}

type websocketAddr struct{}

func (a *websocketAddr) Network() string {
	return "websocket"
}

func (a *websocketAddr) String() string {
	return "websocket/unknown-addr"
}

func (nc *netConn) LocalAddr() net.Addr {
	if nc, ok := nc.c.rwc.(net.Conn); ok {
		return nc.LocalAddr()
	}
	return &websocketAddr{}
}

func (nc *netConn) RemoteAddr() net.Addr {
	if nc, ok := nc.c.rwc.(net.Conn); ok {
		return nc.RemoteAddr()
	}
	return &websocketAddr{}
}

func (nc *netConn) SetDeadline(t time.Time) error {
	nc.SetReadDeadline(t)
	nc.SetWriteDeadline(t)
	return nil
}

func (nc *netConn) SetReadDeadline(t time.Time) error {
	nc.readDeadline.set(t)
	return nil
}

func (nc *netConn) SetWriteDeadline(t time.Time) error {
	nc.writeDeadline.set(t)
	return nil
}
