package websocket

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
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

var errUpgradeHeaderNotWebSocket = errors.New("websocket: Upgrade header is not websocket")
var errConnectionHeaderNotUpgrade = errors.New("websocket: Connection header is not Upgrade")
var errHijackerNotSupported = errors.New("websocket: hijacker is not supported")

func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	// validate the request
	if !r.ProtoAtLeast(1, 1) {
		http.Error(w, http.StatusText(http.StatusUpgradeRequired), http.StatusUpgradeRequired)
		return nil, fmt.Errorf("websocket: HTTP version not supported: %s", r.Proto)
	}
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return nil, fmt.Errorf("websocket: method not allowed: %s", r.Method)
	}
	if !headerContainsTokenIgnoreCase(r.Header, "Upgrade", "websocket") {
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Upgrade", "websocket")
		http.Error(w, http.StatusText(http.StatusUpgradeRequired), http.StatusUpgradeRequired)
		return nil, errUpgradeHeaderNotWebSocket
	}
	if !headerContainsTokenIgnoreCase(r.Header, "Connection", "Upgrade") {
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Upgrade", "websocket")
		http.Error(w, http.StatusText(http.StatusUpgradeRequired), http.StatusUpgradeRequired)
		return nil, errConnectionHeaderNotUpgrade
	}
	if version := r.Header.Get("Sec-WebSocket-Version"); version != "13" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, fmt.Errorf("websocket: unsupported version: %s", version)
	}
	key, err := getWebSocketKey(r)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, err
	}

	// TODO: validate origin

	hijacker, ok := hijacker(w)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
		return nil, errHijackerNotSupported
	}

	// Upgrade to WebSocket
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

func headerContainsTokenIgnoreCase(h http.Header, key, token string) bool {
	for t := range headerTokens(h, key) {
		if strings.EqualFold(t, token) {
			return true
		}
	}
	return false
}

func headerTokens(h http.Header, key string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, v := range h.Values(key) {
			for token := range strings.SplitSeq(v, ",") {
				token := strings.TrimSpace(token)
				if !yield(token) {
					return
				}
			}
		}
	}
}

func getWebSocketKey(r *http.Request) (string, error) {
	keys := r.Header.Values("Sec-WebSocket-Key")
	if len(keys) == 0 {
		return "", errors.New("websocket: missing Sec-WebSocket-Key header")
	}
	if len(keys) > 1 {
		return "", errors.New("websocket: multiple Sec-WebSocket-Key headers")
	}
	key := strings.TrimSpace(keys[0])
	data, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return "", fmt.Errorf("websocket: invalid Sec-WebSocket-Key: %v", err)
	}
	if len(data) != 16 {
		return "", fmt.Errorf("websocket: invalid Sec-WebSocket-Key length: %d", len(data))
	}
	return key, nil
}

var websocketGUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

func acceptHeader(key string) string {
	hash := sha1.New()
	buf := []byte(key)
	hash.Write(buf)
	hash.Write(websocketGUID)
	return base64.StdEncoding.EncodeToString(hash.Sum(buf[:0]))
}
