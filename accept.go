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
	"net/url"
	"slices"
	"strconv"
	"strings"
)

type AcceptOptions struct {
	// Subprotocols lists the WebSocket subprotocols that Accept will negotiate with the client.
	Subprotocols []string

	// InsecureSkipVerify is used to disable Accept's origin verification behavior.
	InsecureSkipVerify bool

	// OriginPatterns lists the host patterns for authorized origins.
	// The request host is always authorized.
	// Use this to enable cross origin WebSockets.
	//
	// i.e JavaScript running on https://example.com wants to access a WebSocket server at https://chat.example.com.
	// In such a case, https://example.com is the origin and https://chat.example.com is the request host.
	// One would set this field to []string{"https://example.com"} to authorize https://example.com to connect.
	//
	// The wildcard pattern (*) matches a single arbitrary subdomain.
	// For example, https://*.example.com matches https://foo.example.com,
	// but does not match https://bar.foo.example.com.
	OriginPatterns []string

	// CompressionMode controls the compression mode.
	// Defaults to CompressionDisabled.
	CompressionMode CompressionMode

	// CompressionThreshold controls the minimum size of a message before compression is applied.
	//
	// Defaults to 512 bytes for CompressionNoContextTakeover and 128 bytes
	// for CompressionContextTakeover.
	CompressionThreshold int

	// CompressionLevel controls the compression level for the flate.Writer.
	// Defaults to flate.NoCompression.
	CompressionLevel int

	// OnPingReceived is an optional callback invoked synchronously when a ping frame is received.
	// If it returns true, the default pong response will be sent automatically.
	OnPingReceived func(ctx context.Context, payload []byte) bool

	// OnPongReceived is an optional callback invoked synchronously when a pong frame is received.
	OnPongReceived func(ctx context.Context, payload []byte)

	// SkipValidateUTF8Read disables UTF-8 validation for text messages when reading.
	// This is useful for performance reasons if you know
	// that the text messages are valid UTF-8.
	//
	// Defaults to false.
	SkipValidateUTF8Read bool

	// SkipValidateUTF8Write disables UTF-8 validation for text messages when writing.
	// This is useful for performance reasons if you know
	// that the text messages are valid UTF-8.
	//
	// Defaults to false.
	SkipValidateUTF8Write bool
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
	if !opts.InsecureSkipVerify {
		err := validateOrigin(r, opts.OriginPatterns)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return nil, err
		}
	}

	hijacker, ok := hijacker(w)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
		return nil, errHijackerNotSupported
	}

	h := w.Header()

	// negotiate the subprotocol. The client's order expresses its preference.
	subprotocol := selectSubprotocol(r.Header, opts.Subprotocols)
	if subprotocol != "" {
		h.Set("Sec-Websocket-Protocol", subprotocol)
	}

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
		rwc:                   conn,
		client:                false,
		subprotocol:           subprotocol,
		skipValidateUTF8Read:  opts.SkipValidateUTF8Read,
		skipValidateUTF8Write: opts.SkipValidateUTF8Write,
		copts:                 copts,
		flateThreshold:        opts.CompressionThreshold,
		flateLevel:            opts.CompressionLevel,
		onPingReceived:        opts.OnPingReceived,
		onPongReceived:        opts.OnPongReceived,

		br: brw.Reader,
		bw: brw.Writer,
	}), nil
}

func selectSubprotocol(h http.Header, supported []string) string {
	for offered := range headerTokens(h, "Sec-Websocket-Protocol") {
		if slices.Contains(supported, offered) {
			return offered
		}
	}
	return ""
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

func parseInt(param string) (int, error) {
	_, value, ok := strings.Cut(param, "=")
	if !ok {
		return 0, fmt.Errorf("websocket: parameter value is missing: %q", param)
	}

	// Remove quotes if present
	if len(value) > 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}

	// Validate that the value is a valid integer string
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("websocket: invalid parameter value: %q", param)
		}
	}

	// leading zeros are not allowed
	if len(value) > 1 && value[0] == '0' {
		return 0, fmt.Errorf("websocket: invalid parameter value: %q", param)
	}

	i, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("websocket: invalid parameter value: %q", param)
	}
	return i, nil
}

func acceptDeflate(ext websocketExtension, mode CompressionMode) (*compressionOptions, bool) {
	seenClientNoContextTakeover := false
	seenServerNoContextTakeover := false
	seenClientMaxWindowBits := false
	seenServerMaxWindowBits := false
	copts := mode.opts()
	for _, p := range ext.params {
		switch {
		case p == "client_no_context_takeover":
			if seenClientNoContextTakeover {
				return nil, false
			}
			seenClientNoContextTakeover = true
			copts.clientNoContextTakeover = true

		case p == "server_no_context_takeover":
			if seenServerNoContextTakeover {
				return nil, false
			}
			seenServerNoContextTakeover = true
			copts.serverNoContextTakeover = true

		case p == "client_max_window_bits":
			if seenClientMaxWindowBits {
				return nil, false
			}
			seenClientMaxWindowBits = true

		case strings.HasPrefix(p, "client_max_window_bits="):
			if seenClientMaxWindowBits {
				return nil, false
			}
			seenClientMaxWindowBits = true

			// We can't adjust the deflate window, but decoding with a larger window is acceptable.
			val, err := parseInt(p)
			if err != nil || val < 8 || val > 15 {
				return nil, false
			}

		case strings.HasPrefix(p, "server_max_window_bits="):
			if seenServerMaxWindowBits {
				return nil, false
			}
			seenServerMaxWindowBits = true

			// We can't adjust the deflate window, the largest window is only acceptable.
			val, err := parseInt(p)
			if err != nil || val != 15 {
				return nil, false
			}

		default:
			// unknown parameter, reject the extension.
			return nil, false
		}
	}
	return copts, true
}

// originType represents the origin of a WebSocket connection.
type originType struct {
	scheme string
	host   string
	port   int
}

func parseOrigin(s string) (originType, error) {
	var origin originType
	u, err := url.Parse(s)
	if err != nil {
		return originType{}, fmt.Errorf("websocket: failed to parse origin: %w", err)
	}

	switch {
	case strings.EqualFold(u.Scheme, "http"):
		origin.scheme = "http"
		origin.port = 80 // default port for http
	case strings.EqualFold(u.Scheme, "https"):
		origin.scheme = "https"
		origin.port = 443 // default port for https
	default:
		return originType{}, fmt.Errorf("websocket: unsupported origin scheme: %q", u.Scheme)
	}

	// host is case-insensitive, so we convert it to lowercase for comparison.
	origin.host = strings.ToLower(u.Hostname())

	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return originType{}, fmt.Errorf("websocket: invalid origin port: %w", err)
		}
		origin.port = p
	}

	return origin, nil
}

func match(origin, allowed originType) bool {
	if origin.scheme != allowed.scheme {
		return false
	}
	if origin.port != allowed.port {
		return false
	}

	// handle wildcard domain patterns
	for strings.HasPrefix(allowed.host, "*.") {
		_, after, ok := strings.Cut(origin.host, ".")
		if !ok {
			return false
		}
		origin.host = after
		allowed.host = allowed.host[2:] // remove "*."
	}
	return origin.host == allowed.host
}

func validateOrigin(req *http.Request, allowed []string) error {
	if origins := req.Header.Values("Origin"); len(origins) == 0 {
		// The communication is allowed because it is not from a browser.
		return nil
	}

	origin := req.Header.Get("Origin")
	o, err := parseOrigin(origin)
	if err != nil {
		return fmt.Errorf("websocket: failed to parse origin: %w", err)
	}

	if strings.EqualFold(o.host, req.Host) {
		return nil
	}

	for _, a := range allowed {
		parsedAllowed, err := parseOrigin(a)
		if err != nil {
			return fmt.Errorf("websocket: failed to parse allowed origin: %w", err)
		}
		if match(o, parsedAllowed) {
			return nil
		}
	}
	return fmt.Errorf("websocket: origin not allowed: %q", origin)
}

func getWebSocketKey(r *http.Request) (string, error) {
	keys := r.Header.Values("Sec-Websocket-Key")
	if len(keys) == 0 {
		return "", errors.New("websocket: missing Sec-Websocket-Key header")
	}
	if len(keys) > 1 {
		return "", errors.New("websocket: multiple Sec-Websocket-Key headers")
	}
	key := strings.TrimSpace(keys[0])
	data, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return "", fmt.Errorf("websocket: invalid Sec-Websocket-Key: %v", err)
	}
	if len(data) != 16 {
		return "", fmt.Errorf("websocket: invalid Sec-Websocket-Key length: %d", len(data))
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
