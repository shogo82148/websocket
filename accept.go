package websocket

import (
	"context"
	"errors"
	"net/http"
)

type CompressionMode int

const (
	// CompressionDisabled disables the negotiation of the permessage-deflate extension.
	CompressionDisabled CompressionMode = iota

	CompressionContextTakeover

	CompressionNoContextTakeover
)

type AcceptOptions struct {
	// Subprotocols lists the WebSocket subprotocols that Accept will negotiate with the client.
	Subprotocols []string

	// InsecureSkipVerify is used to disable Accept's origin verification behavior.
	InsecureSkipVerify bool

	// OriginPatterns lists the host patterns for authorized origins.
	OriginPatterns []string

	// CompressionMode controls the compression mode.
	// Defaults to CompressionDisabled.
	CompressionMode CompressionMode

	// CompressionThreshold controls the minimum size of a message before compression is applied.
	//
	// Defaults to 512 bytes for CompressionNoContextTakeover and 128 bytes
	// for CompressionContextTakeover.
	CompressionThreshold int

	// OnPingReceived is an optional callback invoked synchronously when a ping frame is received.
	// If it returns true, the default pong response will be sent automatically.
	OnPingReceived func(ctx context.Context, payload []byte) bool

	// OnPongReceived is an optional callback invoked synchronously when a pong frame is received.
	OnPongReceived func(ctx context.Context, payload []byte)
}

func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	return nil, errors.New("not implemented")
}
