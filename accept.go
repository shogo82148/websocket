package websocket

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"
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

func (opts *AcceptOptions) cloneWithDefaults() *AcceptOptions {
	var o AcceptOptions
	if opts != nil {
		o = *opts
	}
	return &o
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
	if versions := r.Header.Values("Sec-Websocket-Version"); len(versions) != 1 || versions[0] != "13" {
		version := strings.Join(versions, ", ")
		w.Header().Set("Sec-Websocket-Version", "13")
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, fmt.Errorf("websocket: unsupported version: %q", version)
	}
	key, err := getWebSocketKey(r)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, err
	}

	opts = opts.cloneWithDefaults()

	// TODO: validate origin

	hijacker, ok := hijacker(w)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
		return nil, errHijackerNotSupported
	}

	h := w.Header()

	// negotiate extensions
	copts, ok := selectDeflate(websocketExtensions(r.Header), opts.CompressionMode)
	if ok {
		h.Set("Sec-Websocket-Extensions", copts.String())
	}

	// Upgrade to WebSocket
	h.Set("Upgrade", "websocket")
	h.Set("Connection", "Upgrade")
	h.Set("Sec-Websocket-Accept", acceptHeader(key))
	w.WriteHeader(http.StatusSwitchingProtocols)

	conn, brw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	// https://github.com/golang/go/issues/32314
	b, _ := brw.Reader.Peek(brw.Reader.Buffered())
	brw.Reader.Reset(io.MultiReader(bytes.NewReader(b), conn))

	return newConn(connConfig{
		rwc:    conn,
		client: false,
		br:     brw.Reader,
		bw:     brw.Writer,
	}), nil
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

type websocketExtension struct {
	name   string
	params []string
}

func websocketExtensions(h http.Header) iter.Seq[websocketExtension] {
	return func(yield func(websocketExtension) bool) {
		for extStr := range headerTokens(h, "Sec-Websocket-Extensions") {
			if extStr == "" {
				continue
			}

			vals := strings.Split(extStr, ";")
			for i := range vals {
				vals[i] = strings.TrimSpace(vals[i])
			}
			ext := websocketExtension{
				name:   vals[0],
				params: vals[1:],
			}
			if !yield(ext) {
				return
			}
		}
	}
}

func selectDeflate(selectDeflate iter.Seq[websocketExtension], mode CompressionMode) (*compressionOptions, bool) {
	switch mode {
	case CompressionDisabled:
		return nil, false
	case CompressionContextTakeover:
	case CompressionNoContextTakeover:
	default:
		// This should never happen, but if it does, we will treat it as CompressionDisabled.
		return nil, false
	}

	for ext := range selectDeflate {
		switch ext.name {
		case "permessage-deflate":
			copts, ok := acceptDeflate(ext, mode)
			if ok {
				return copts, true
			}
		}
	}
	return nil, false
}

func acceptDeflate(ext websocketExtension, mode CompressionMode) (*compressionOptions, bool) {
	copts := mode.opts()
	for _, p := range ext.params {
		switch p {
		case "client_no_context_takeover":
			copts.clientNoContextTakeover = true
			continue
		case "server_no_context_takeover":
			copts.serverNoContextTakeover = true
			continue
		case "client_max_window_bits", "server_max_window_bits":
			continue
		}
		if strings.HasPrefix(p, "client_max_window_bits=") {
			// We can't adjust the deflate window, but decoding with a larger window is acceptable.
			continue
		}
		return nil, false
	}
	return copts, true
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
