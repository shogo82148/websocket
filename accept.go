package websocket

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
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

type rwUnwrap interface {
	Unwrap() http.ResponseWriter
}

// hijacker returns the Hijacker interface of the http.ResponseWriter.
// It looks for the Hijacker interface in a manner similar to http.ResponseController.
// If it is not found, it returns (nil, false).
//
// Since there is no way to know in advance whether the Hijacker interface is implemented,
// we implement it ourselves.
func hijacker(w http.ResponseWriter) (http.Hijacker, bool) {
	for {
		switch hijacker := w.(type) {
		case http.Hijacker:
			return hijacker, true
		case rwUnwrap:
			w = hijacker.Unwrap()
		default:
			return nil, false
		}
	}
}

var errHijackerNotSupported = errors.New("websocket: hijacker is not supported")

func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	// TODO: validate method, headers, and version
	// TODO: validate origin

	hijacker, ok := hijacker(w)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
		return nil, errHijackerNotSupported
	}

	// Upgrade to WebSocket
	key := r.Header.Get("Sec-WebSocket-Key")
	h := w.Header()
	h.Set("Upgrade", "websocket")
	h.Set("Connection", "Upgrade")
	h.Set("Sec-WebSocket-Accept", acceptHeader(key))
	w.WriteHeader(http.StatusSwitchingProtocols)

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	return &Conn{
		conn: conn,
		rw:   rw,
	}, nil
}

var websocketGUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

func acceptHeader(key string) string {
	hash := sha1.New()
	buf := []byte(key)
	hash.Write(buf)
	hash.Write(websocketGUID)
	return base64.StdEncoding.EncodeToString(hash.Sum(buf[:0]))
}
