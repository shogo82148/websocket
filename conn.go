package websocket

import (
	"errors"
	"fmt"
)

type Conn struct{}

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

func (c *Conn) Close(code StatusCode, reason string) error {
	return errors.New("not implemented")
}
