package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// StatusCode represents a WebSocket status code.
type StatusCode int

const (
	StatusNormalClosure   StatusCode = 1000
	StatusGoingAway       StatusCode = 1001
	StatusProtocolError   StatusCode = 1002
	StatusUnsupportedData StatusCode = 1003

	// 1004 is reserved.
	statusReserved StatusCode = 1004

	// StatusNoStatusRcvd cannot be sent in a close message.
	// It is reserved for when a close message is received without
	// a status code.
	StatusNoStatusRcvd StatusCode = 1005

	// StatusAbnormalClosure is exported for use only with Wasm.
	// In non Wasm Go, the returned error will indicate whether the
	// connection was closed abnormally.
	StatusAbnormalClosure StatusCode = 1006

	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalError           StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014

	// StatusTLSHandshake is only exported for use with Wasm.
	// In non Wasm Go, the returned error will indicate whether there was
	// a TLS handshake failure.
	StatusTLSHandshake StatusCode = 1015
)

// CloseError is returned when the connection is closed with a status and reason.
type CloseError struct {
	Code   StatusCode
	Reason string
}

func (err CloseError) Error() string {
	return fmt.Sprintf("websocket: close %d (%s)", err.Code, err.Reason)
}

const maxCloseReason = maxControlPayload - 2

// bytes returns the byte representation of the CloseError, which can be sent as a close frame payload.
func (err CloseError) bytes() ([]byte, error) {
	if len(err.Reason) > maxCloseReason {
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(StatusInternalError))
		return b, fmt.Errorf("websocket: close reason too long: %d bytes (max %d)", len(err.Reason), maxCloseReason)
	}

	if !validWireCloseCode(err.Code) {
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(StatusInternalError))
		return b, fmt.Errorf("websocket: invalid close code: %d", err.Code)
	}

	b := make([]byte, 2+len(err.Reason))
	binary.BigEndian.PutUint16(b, uint16(err.Code))
	copy(b[2:], err.Reason)
	return b, nil
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

func validWireCloseCode(code StatusCode) bool {
	switch code {
	case statusReserved, StatusNoStatusRcvd, StatusAbnormalClosure, StatusTLSHandshake:
		return false
	}
	if code >= StatusNormalClosure && code <= StatusBadGateway {
		return true
	}
	if code >= 3000 && code <= 4999 {
		return true
	}
	return false
}

// Close performs the WebSocket close handshake with the given status code and reason.
//
// It will write a WebSocket close frame with a timeout of 5s and then wait 5s for
// the peer to send a close frame.
// All data messages received from the peer during the close handshake will be discarded.
//
// The connection can only be closed once. Additional calls to Close
// are no-ops.
//
// The maximum length of reason must be 123 bytes. Avoid sending a dynamic reason.
//
// Close will unblock all goroutines interacting with the connection once
// complete.
func (c *Conn) Close(code StatusCode, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.closeHandshake(ctx, code, reason); err != nil {
		return err
	}
	return nil
}

// CloseNow closes the WebSocket connection without attempting a close handshake.
// Use when you do not want the overhead of the close handshake.
func (c *Conn) CloseNow() error {
	return errors.New("not implemented")
}

func (c *Conn) closeHandshake(ctx context.Context, code StatusCode, reason string) error {
	if err := c.writeClose(ctx, code, reason); err != nil {
		return err
	}
	if err := c.waitCloseHandshake(ctx); err != nil {
		return err
	}
	return nil
}

// writeClose writes a close frame to the connection.
func (c *Conn) writeClose(ctx context.Context, code StatusCode, reason string) error {
	ce := CloseError{Code: code, Reason: reason}

	var p []byte
	var err error
	if ce.Code != StatusNoStatusRcvd {
		p, err = ce.bytes()
		if err != nil {
			return err
		}
	}

	err = c.writeFrame(ctx, true, opClose, p)
	// If the connection closed as we're writing we ignore the error as we might
	// have written the close frame, the peer responded and then someone else read it
	// and closed the connection.
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return err
}

// waitCloseHandshake waits for a close frame from the peer and returns the status code and reason.
func (c *Conn) waitCloseHandshake(ctx context.Context) error {
	for {
		_, r, err := c.Reader(ctx)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, r)
		if err != nil {
			return err
		}
	}
}
