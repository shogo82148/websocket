package websocket

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

type DialOptions struct {
	// HTTPClient is used for the connection.
	// Its Transport must return writable bodies for WebSocket handshakes.
	HTTPClient *http.Client

	// HTTPHeader specifies the HTTP headers included in the handshake request.
	HTTPHeader http.Header

	// Host optionally overrides the Host HTTP header to send. If empty, the value
	// of URL.Host will be used.
	Host string

	// Subprotocols lists the WebSocket subprotocols to negotiate with the server.
	Subprotocols []string

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

func (opts *DialOptions) cloneWithDefaults(ctx context.Context) (context.Context, context.CancelFunc, *DialOptions) {
	var cancel context.CancelFunc

	var o DialOptions
	if opts != nil {
		o = *opts
	}
	if o.HTTPClient == nil {
		o.HTTPClient = http.DefaultClient
	}
	if timeout := o.HTTPClient.Timeout; timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		newClient := *o.HTTPClient
		newClient.Timeout = 0
		o.HTTPClient = &newClient
	}
	if o.HTTPHeader == nil {
		o.HTTPHeader = make(http.Header)
	}

	// Wrap the HTTPClient to handle redirects and change the scheme from ws/wss to http/https.
	newClient := *o.HTTPClient
	oldCheckRedirect := o.HTTPClient.CheckRedirect
	newClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		switch req.URL.Scheme {
		case "ws":
			req.URL.Scheme = "http"
		case "wss":
			req.URL.Scheme = "https"
		}
		if oldCheckRedirect != nil {
			return oldCheckRedirect(req, via)
		}
		return nil
	}

	// disable HTTP/2 for the HTTPClient because it does not support WebSocket.
	transport := o.HTTPClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if t, ok := transport.(*http.Transport); ok {
		newTransport := t.Clone()
		newTransport.Protocols = new(http.Protocols)
		newTransport.Protocols.SetHTTP1(true)
		newClient.Transport = newTransport
	}

	o.HTTPClient = &newClient
	return ctx, cancel, &o
}

// Dial performs a WebSocket handshake on u.
func Dial(ctx context.Context, u string, opts *DialOptions) (*Conn, *http.Response, error) {
	var cancel context.CancelFunc
	ctx, cancel, opts = opts.cloneWithDefaults(ctx)
	if cancel != nil {
		defer cancel()
	}

	// generate a random Sec-Websocket-Key
	var buf [16]byte
	rand.Read(buf[:])
	secWebSocketKey := base64.StdEncoding.EncodeToString(buf[:])

	var copts *compressionOptions
	switch opts.CompressionMode {
	case CompressionDisabled:
		// no compression
	case CompressionNoContextTakeover, CompressionContextTakeover:
		copts = opts.CompressionMode.opts()
	default:
		return nil, nil, fmt.Errorf("websocket: unsupported compression mode: %v", opts.CompressionMode)
	}

	resp, err := handshakeRequest(ctx, u, secWebSocketKey, opts, copts)
	if err != nil {
		return nil, nil, err
	}

	copts, err = verifyServerResponse(resp, secWebSocketKey, opts, copts)
	if err != nil {
		return nil, readResponseBody(resp), err
	}
	subprotocol := resp.Header.Get("Sec-Websocket-Protocol")

	rwc, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		return nil, readResponseBody(resp), fmt.Errorf("websocket: response body is not a ReadWriteCloser: %T", resp.Body)
	}

	conn := newConn(connConfig{
		rwc:                   rwc,
		client:                true,
		subprotocol:           subprotocol,
		skipValidateUTF8Read:  opts.SkipValidateUTF8Read,
		skipValidateUTF8Write: opts.SkipValidateUTF8Write,
		onPingReceived:        opts.OnPingReceived,
		onPongReceived:        opts.OnPongReceived,
		br:                    bufio.NewReader(rwc),
		bw:                    bufio.NewWriter(rwc),
	})
	conn.initCompression(copts, opts.CompressionThreshold, opts.CompressionLevel)

	return conn, resp, nil
}

// readResponseBody reads a bit of the body for easier debugging.
// It returns a new response with the body replaced by a ReadCloser that reads the buffered data first.
func readResponseBody(resp *http.Response) *http.Response {
	respBody := resp.Body
	timer := time.AfterFunc(time.Second*3, func() {
		respBody.Close()
	})
	defer timer.Stop()

	r := io.LimitReader(respBody, 1024)
	buf, _ := io.ReadAll(r)
	respBody.Close()
	resp.Body = io.NopCloser(bytes.NewReader(buf))
	return resp
}

func handshakeRequest(ctx context.Context, u, secWebSocketKey string, opts *DialOptions, copts *compressionOptions) (*http.Response, error) {
	// parse the URL and change the scheme from ws/wss to http/https
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("websocket: failed to parse URL: %w", err)
	}
	switch parsed.Scheme {
	case "ws":
		parsed.Scheme = "http"
	case "wss":
		parsed.Scheme = "https"
	case "http", "https":
		// do nothing
	default:
		return nil, fmt.Errorf("websocket: unsupported scheme: %s", parsed.Scheme)
	}

	// create the HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("websocket: failed to create request: %w", err)
	}
	maps.Copy(req.Header, opts.HTTPHeader)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-Websocket-Version", "13")
	req.Header.Set("Sec-Websocket-Key", secWebSocketKey)
	if opts.Host != "" {
		req.Host = opts.Host
	}
	if len(opts.Subprotocols) > 0 {
		req.Header.Set("Sec-Websocket-Protocol", strings.Join(opts.Subprotocols, ", "))
	}
	if copts != nil {
		req.Header.Set("Sec-Websocket-Extensions", copts.String())
	}

	// send the HTTP request
	resp, err := opts.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("websocket: failed to send request: %w", err)
	}
	return resp, nil
}

func verifyServerResponse(resp *http.Response, secWebSocketKey string, opts *DialOptions, copts *compressionOptions) (*compressionOptions, error) {
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("websocket: unexpected status code: %d", resp.StatusCode)
	}
	if !headerContainsTokenIgnoreCase(resp.Header, "Upgrade", "websocket") {
		return nil, errUpgradeHeaderNotWebSocket
	}
	if !headerContainsTokenIgnoreCase(resp.Header, "Connection", "Upgrade") {
		return nil, errConnectionHeaderNotUpgrade
	}
	expectedAccept := acceptHeader(secWebSocketKey)
	if got := resp.Header.Get("Sec-Websocket-Accept"); got != expectedAccept {
		return nil, fmt.Errorf("websocket: Sec-Websocket-Accept mismatch: got %q, want %q", got, expectedAccept)
	}

	var subprotocols []string
	if opts != nil {
		subprotocols = opts.Subprotocols
	}
	if err := verifySubprotocol(subprotocols, resp); err != nil {
		return nil, err
	}

	return verifyServerExtensions(copts, resp.Header)
}

func verifySubprotocol(subprotocols []string, resp *http.Response) error {
	protocols := resp.Header.Values("Sec-Websocket-Protocol")
	if len(protocols) == 0 {
		// No subprotocol was negotiated, which is valid if the client did not request any.
		return nil
	}
	if len(protocols) > 1 {
		return fmt.Errorf("websocket: multiple Sec-Websocket-Protocol headers: %v", protocols)
	}

	proto := protocols[0]
	if slices.Contains(subprotocols, proto) {
		return nil
	}

	return fmt.Errorf("websocket: server selected unsupported subprotocol: %q", proto)
}

func verifyServerExtensions(copts *compressionOptions, h http.Header) (*compressionOptions, error) {
	exts := slices.Collect(websocketExtensions(h))

	if len(exts) == 0 {
		// No extensions were negotiated.
		return nil, nil
	}

	ext := exts[0]
	if ext.name != "permessage-deflate" || len(exts) > 1 || copts == nil {
		return nil, fmt.Errorf("websocket: unsupported extensions from server: %+v", exts)
	}

	// clone copts to avoid modifying the original options
	tmp := *copts
	copts = &tmp

	requestedServerNoContextTakeover := copts.serverNoContextTakeover
	seenClientNoContextTakeover := false
	seenServerNoContextTakeover := false
	seenServerMaxWindowBits := false
	for _, p := range ext.params {
		switch {
		case p == "client_no_context_takeover":
			if seenClientNoContextTakeover {
				return nil, errors.New("websocket: duplicate client_no_context_takeover parameter from server")
			}
			seenClientNoContextTakeover = true
			copts.clientNoContextTakeover = true

		case p == "server_no_context_takeover":
			if seenServerNoContextTakeover {
				return nil, errors.New("websocket: duplicate server_no_context_takeover parameter from server")
			}
			seenServerNoContextTakeover = true
			copts.serverNoContextTakeover = true

		case strings.HasPrefix(p, "server_max_window_bits="):
			// We can't adjust the deflate window, but decoding with a larger window is acceptable.
			if seenServerMaxWindowBits {
				return nil, errors.New("websocket: duplicate server_max_window_bits parameter from server")
			}
			seenServerMaxWindowBits = true
			val, err := parseInt(p)
			if err != nil || val < 8 || val > 15 {
				return nil, fmt.Errorf("websocket: invalid server_max_window_bits parameter from server: %q", p)
			}

		default:
			return nil, fmt.Errorf("websocket: unsupported permessage-deflate parameter from server: %q", p)
		}
	}
	if requestedServerNoContextTakeover && !seenServerNoContextTakeover {
		return nil, errors.New("websocket: server did not accept server_no_context_takeover")
	}
	return copts, nil
}
