package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDial(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Scheme != "http" {
				t.Fatalf("request URL scheme = %q; want %q", req.URL.Scheme, "http")
			}
			if req.Host != "override.example.com" {
				t.Fatalf("request Host = %q; want %q", req.Host, "override.example.com")
			}
			if req.Header.Get("X-Test") != "ok" {
				t.Fatalf("X-Test header = %q; want %q", req.Header.Get("X-Test"), "ok")
			}
			if req.Header.Get("Upgrade") != "websocket" {
				t.Fatalf("Upgrade header = %q; want %q", req.Header.Get("Upgrade"), "websocket")
			}
			if req.Header.Get("Connection") != "Upgrade" {
				t.Fatalf("Connection header = %q; want %q", req.Header.Get("Connection"), "Upgrade")
			}
			if req.Header.Get("Sec-Websocket-Version") != "13" {
				t.Fatalf("Sec-Websocket-Version = %q; want %q", req.Header.Get("Sec-Websocket-Version"), "13")
			}
			if req.Header.Get("Sec-Websocket-Protocol") != "chat, superchat" {
				t.Fatalf("Sec-Websocket-Protocol = %q; want %q", req.Header.Get("Sec-Websocket-Protocol"), "chat, superchat")
			}

			key := req.Header.Get("Sec-Websocket-Key")
			if key == "" {
				t.Fatal("Sec-Websocket-Key is empty")
			}

			h := make(http.Header)
			h.Set("Upgrade", "websocket")
			h.Set("Connection", "Upgrade")
			h.Set("Sec-Websocket-Accept", acceptHeader(key))
			h.Set("Sec-Websocket-Protocol", "superchat")
			return &http.Response{
				StatusCode: http.StatusSwitchingProtocols,
				Header:     h,
				Body:       new(testReadWriteCloser),
				Request:    req,
			}, nil
		})

		opts := &DialOptions{
			HTTPClient: &http.Client{Transport: transport},
			HTTPHeader: http.Header{"X-Test": []string{"ok"}},
			Host:       "override.example.com",
			Subprotocols: []string{
				"chat",
				"superchat",
			},
		}

		conn, resp, err := Dial(t.Context(), "ws://example.com/ws", opts)
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		if conn == nil {
			t.Fatal("Dial returned nil conn")
		}
		if resp == nil {
			t.Fatal("Dial returned nil response")
		}
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status code = %d; want %d", resp.StatusCode, http.StatusSwitchingProtocols)
		}
		if got := conn.Subprotocol(); got != "superchat" {
			t.Fatalf("Conn.Subprotocol() = %q; want %q", got, "superchat")
		}

		if err := conn.CloseNow(); err != nil {
			t.Fatalf("CloseNow failed: %v", err)
		}
	})

	t.Run("unsupported scheme", func(t *testing.T) {
		t.Parallel()

		_, _, err := Dial(t.Context(), "ftp://example.com", nil)
		if err == nil {
			t.Fatal("Dial succeeded for unsupported scheme")
		}
		if !strings.Contains(err.Error(), "websocket: unsupported scheme") {
			t.Fatalf("error = %q; want to contain %q", err, "websocket: unsupported scheme")
		}
	})

	t.Run("unexpected status code returns response body for debugging", func(t *testing.T) {
		t.Parallel()

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(strings.Repeat("a", 2048))),
				Request:    req,
			}, nil
		})

		opts := &DialOptions{HTTPClient: &http.Client{Transport: transport}}
		conn, resp, err := Dial(t.Context(), "ws://example.com/ws", opts)
		if err == nil {
			t.Fatal("Dial succeeded for unexpected status code")
		}
		if conn != nil {
			t.Fatal("Dial returned non-nil conn on failure")
		}
		if resp == nil {
			t.Fatal("Dial returned nil response on failure")
		}
		if !strings.Contains(err.Error(), "websocket: unexpected status code") {
			t.Fatalf("error = %q; want to contain %q", err, "websocket: unexpected status code")
		}

		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("failed to read response body: %v", readErr)
		}
		if len(body) != 1024 {
			t.Fatalf("debug body length = %d; want %d", len(body), 1024)
		}
	})

	t.Run("response body must be read write closer", func(t *testing.T) {
		t.Parallel()

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			key := req.Header.Get("Sec-Websocket-Key")
			h := make(http.Header)
			h.Set("Upgrade", "websocket")
			h.Set("Connection", "Upgrade")
			h.Set("Sec-Websocket-Accept", acceptHeader(key))

			return &http.Response{
				StatusCode: http.StatusSwitchingProtocols,
				Header:     h,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		})

		opts := &DialOptions{HTTPClient: &http.Client{Transport: transport}}
		conn, resp, err := Dial(t.Context(), "ws://example.com/ws", opts)
		if err == nil {
			t.Fatal("Dial succeeded with non-ReadWriteCloser body")
		}
		if conn != nil {
			t.Fatal("Dial returned non-nil conn on failure")
		}
		if resp == nil {
			t.Fatal("Dial returned nil response")
		}
		if !strings.Contains(err.Error(), "not a ReadWriteCloser") {
			t.Fatalf("error = %q; want to contain %q", err, "not a ReadWriteCloser")
		}
	})
}

func TestVerifyServerResponse(t *testing.T) {
	t.Parallel()

	const key = "dGhlIHNhbXBsZSBub25jZQ==" // betterleaks:allow

	validResponse := func() *http.Response {
		h := make(http.Header)
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Accept", acceptHeader(key))
		return &http.Response{StatusCode: http.StatusSwitchingProtocols, Header: h}
	}

	verifyCloseHandshake := func(t testing.TB, rwc *testReadWriteCloser) {
		t.Helper()
		data := rwc.w.Bytes()
		if len(data) < 2 {
			t.Fatalf("close handshake not sent, data = %q", data)
		}
		if data[0] != 0x88 {
			t.Fatalf("close handshake opcode = %x; want 0x88", data[0])
		}
		if data[1]&0x80 == 0 {
			t.Fatal("close handshake not masked")
		}
		maskKey := binary.BigEndian.Uint32(data[2:6])
		maskFramePayload(data[6:], maskKey)
		if data[6] != 0x03 || data[7] != 0xea {
			t.Fatalf("close handshake payload = %x; want 03ea", data[6:8])
		}
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		conn, rwc := newTestConnWithInput(t, nil)
		if _, err := verifyServerResponse(conn, resp, key, &DialOptions{}, nil); err != nil {
			t.Fatalf("verifyServerResponse failed: %v", err)
		}
		if rwc.w.Len() != 0 {
			t.Fatalf("unexpected data written to connection: %q", rwc.w.String())
		}
	})

	t.Run("invalid accept header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Accept", "invalid")
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, &DialOptions{}, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with invalid Sec-Websocket-Accept")
		}
		if !strings.Contains(err.Error(), "Sec-Websocket-Accept mismatch") {
			t.Fatalf("error = %q; want to contain %q", err, "Sec-Websocket-Accept mismatch")
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("missing upgrade header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Del("Upgrade")
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, &DialOptions{}, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with missing Upgrade header")
		}
		if !errors.Is(err, errUpgradeHeaderNotWebSocket) {
			t.Fatalf("error = %q; want to be %v", err, errUpgradeHeaderNotWebSocket)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("missing connection header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Del("Connection")
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, &DialOptions{}, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with missing Connection header")
		}
		if !errors.Is(err, errConnectionHeaderNotUpgrade) {
			t.Fatalf("error = %q; want to be %v", err, errConnectionHeaderNotUpgrade)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("unsupported subprotocol", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Protocol", "video")
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, &DialOptions{Subprotocols: []string{"chat"}}, nil)
		if err == nil || !strings.Contains(err.Error(), "unsupported subprotocol") {
			t.Fatalf("error = %v; want unsupported subprotocol error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("multiple selected subprotocols", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Protocol", "chat, superchat")
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, &DialOptions{Subprotocols: []string{"chat", "superchat"}}, nil)
		if err == nil || !strings.Contains(err.Error(), "unsupported subprotocol") {
			t.Fatalf("error = %v; want unsupported subprotocol error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("permessage-deflate negotiation", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; client_no_context_takeover; server_no_context_takeover; server_max_window_bits=15")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err != nil {
			t.Fatalf("verifyServerResponse failed: %v", err)
		}
		if copts == nil {
			t.Fatal("verifyServerResponse returned nil compression options")
		}
		if !copts.clientNoContextTakeover {
			t.Fatal("clientNoContextTakeover not set in compression options")
		}
		if !copts.serverNoContextTakeover {
			t.Fatal("serverNoContextTakeover not set in compression options")
		}
		if rwc.w.Len() != 0 {
			t.Fatalf("unexpected data written to connection: %q", rwc.w.String())
		}
	})

	t.Run("duplicate client_no_context_takeover parameter", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; client_no_context_takeover; client_no_context_takeover")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "duplicate client_no_context_takeover") {
			t.Fatalf("error = %v; want duplicate client_no_context_takeover error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("duplicate server_no_context_takeover parameter", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_no_context_takeover; server_no_context_takeover")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "duplicate server_no_context_takeover") {
			t.Fatalf("error = %v; want duplicate server_no_context_takeover error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("duplicated server_max_window_bits parameter", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=15; server_max_window_bits=10")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "duplicate server_max_window_bits") {
			t.Fatalf("error = %v; want duplicate server_max_window_bits error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("server_max_window_bits parameter too small", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=7")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "invalid server_max_window_bits") {
			t.Fatalf("error = %v; want invalid server_max_window_bits error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("server_max_window_bits parameter too large", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=16")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "invalid server_max_window_bits") {
			t.Fatalf("error = %v; want invalid server_max_window_bits error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("server_max_window_bits parameter not number", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=invalid")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "invalid server_max_window_bits") {
			t.Fatalf("error = %v; want invalid server_max_window_bits error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("server_max_window_bits parameter with leading zero", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=08")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "invalid server_max_window_bits") {
			t.Fatalf("error = %v; want invalid server_max_window_bits error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("unsupported permessage-deflate parameter", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate; unsupported_param")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "unsupported permessage-deflate parameter") {
			t.Fatalf("error = %v; want unsupported permessage-deflate parameter error", err)
		}
		verifyCloseHandshake(t, rwc)
	})

	t.Run("drops server_no_context_takeover", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-Websocket-Extensions", "permessage-deflate")
		opts := &DialOptions{CompressionMode: CompressionNoContextTakeover}
		copts := opts.CompressionMode.opts()
		conn, rwc := newTestConnWithInput(t, nil)
		_, err := verifyServerResponse(conn, resp, key, opts, copts)
		if err == nil || !strings.Contains(err.Error(), "server did not accept server_no_context_takeover") {
			t.Fatalf("error = %v; want server did not accept server_no_context_takeover error", err)
		}
		verifyCloseHandshake(t, rwc)
	})
}

func TestHandshakeRequest(t *testing.T) {
	t.Parallel()

	t.Run("converts wss to https and sends expected headers", func(t *testing.T) {
		t.Parallel()

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Scheme != "https" {
				t.Fatalf("request URL scheme = %q; want %q", req.URL.Scheme, "https")
			}
			if req.Header.Get("Upgrade") != "websocket" {
				t.Fatalf("Upgrade header = %q; want %q", req.Header.Get("Upgrade"), "websocket")
			}
			if req.Header.Get("Connection") != "Upgrade" {
				t.Fatalf("Connection header = %q; want %q", req.Header.Get("Connection"), "Upgrade")
			}
			if req.Header.Get("Sec-Websocket-Key") != "fixed-key" {
				t.Fatalf("Sec-Websocket-Key = %q; want %q", req.Header.Get("Sec-Websocket-Key"), "fixed-key")
			}

			return &http.Response{
				StatusCode: http.StatusSwitchingProtocols,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		})

		opts := &DialOptions{HTTPClient: &http.Client{Transport: transport}}
		resp, err := handshakeRequest(context.Background(), "wss://example.com/ws", "fixed-key", opts, nil)
		if err != nil {
			t.Fatalf("handshakeRequest failed: %v", err)
		}
		if resp == nil {
			t.Fatal("handshakeRequest returned nil response")
		}
	})
}
