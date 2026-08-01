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

	// OnPingReceived is an optional callback invoked synchronously when a ping frame is received.
	OnPingReceived func(ctx context.Context, payload []byte) bool

	// OnPongReceived is an optional callback invoked synchronously when a pong frame is received.
	OnPongReceived func(ctx context.Context, payload []byte)
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

	// generate a random Sec-WebSocket-Key
	var buf [16]byte
	rand.Read(buf[:])
	secWebSocketKey := base64.StdEncoding.EncodeToString(buf[:])

	resp, err := handshakeRequest(ctx, u, secWebSocketKey, opts)
	if err != nil {
		return nil, nil, err
	}

	if err := verifyServerResponse(resp, secWebSocketKey, opts); err != nil {
		return nil, readResponseBody(resp), err
	}

	rwc, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		return nil, readResponseBody(resp), errors.New("websocket: response body is not a ReadWriteCloser")
	}

	return newConn(connConfig{
		rwc:    rwc,
		client: true,
		br:     bufio.NewReader(rwc),
		bw:     bufio.NewWriter(rwc),
		subprotocol: resp.Header.Get("Sec-WebSocket-Protocol"),
		onPingReceived: opts.OnPingReceived,
		onPongReceived: opts.OnPongReceived,
	}), resp, nil
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

func handshakeRequest(ctx context.Context, u, secWebSocketKey string, opts *DialOptions) (*http.Response, error) {
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
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", secWebSocketKey)
	if opts.Host != "" {
		req.Host = opts.Host
	}
	if len(opts.Subprotocols) > 0 {
		req.Header.Set("Sec-WebSocket-Protocol", strings.Join(opts.Subprotocols, ", "))
	}

	// send the HTTP request
	resp, err := opts.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("websocket: failed to send request: %w", err)
	}
	return resp, nil
}

func verifyServerResponse(resp *http.Response, secWebSocketKey string, opts *DialOptions) error {
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("websocket: unexpected status code: %d", resp.StatusCode)
	}
	if !headerContainsTokenIgnoreCase(resp.Header, "Upgrade", "websocket") {
		return errors.New("websocket: Upgrade header is not websocket")
	}
	if !headerContainsTokenIgnoreCase(resp.Header, "Connection", "Upgrade") {
		return errors.New("websocket: Connection header is not Upgrade")
	}
	expectedAccept := acceptHeader(secWebSocketKey)
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != expectedAccept {
		return fmt.Errorf("websocket: Sec-WebSocket-Accept mismatch: got %q, want %q", got, expectedAccept)
	}
	if err := verifySubprotocol(resp.Header.Get("Sec-WebSocket-Protocol"), opts.Subprotocols); err != nil {
		return err
	}
	return nil
}

func verifySubprotocol(got string, requested []string) error {
	if got == "" {
		return nil
	}
	for _, protocol := range requested {
		if got == protocol {
			return nil
		}
	}
	return fmt.Errorf("websocket: unexpected subprotocol: %q", got)
}
