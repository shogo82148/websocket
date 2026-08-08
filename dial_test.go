package websocket

import (
	"context"
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
			if req.Header.Get("Sec-WebSocket-Version") != "13" {
				t.Fatalf("Sec-WebSocket-Version = %q; want %q", req.Header.Get("Sec-WebSocket-Version"), "13")
			}
			if req.Header.Get("Sec-WebSocket-Protocol") != "chat, superchat" {
				t.Fatalf("Sec-WebSocket-Protocol = %q; want %q", req.Header.Get("Sec-WebSocket-Protocol"), "chat, superchat")
			}

			key := req.Header.Get("Sec-WebSocket-Key")
			if key == "" {
				t.Fatal("Sec-WebSocket-Key is empty")
			}

			h := make(http.Header)
			h.Set("Upgrade", "websocket")
			h.Set("Connection", "Upgrade")
			h.Set("Sec-WebSocket-Accept", acceptHeader(key))
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
			key := req.Header.Get("Sec-WebSocket-Key")
			h := make(http.Header)
			h.Set("Upgrade", "websocket")
			h.Set("Connection", "Upgrade")
			h.Set("Sec-WebSocket-Accept", acceptHeader(key))

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
		h.Set("Sec-WebSocket-Accept", acceptHeader(key))
		return &http.Response{StatusCode: http.StatusSwitchingProtocols, Header: h}
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		if err := verifyServerResponse(resp, key, nil); err != nil {
			t.Fatalf("verifyServerResponse failed: %v", err)
		}
	})

	t.Run("invalid accept header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Set("Sec-WebSocket-Accept", "invalid")
		err := verifyServerResponse(resp, key, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with invalid Sec-WebSocket-Accept")
		}
		if !strings.Contains(err.Error(), "Sec-WebSocket-Accept mismatch") {
			t.Fatalf("error = %q; want to contain %q", err, "Sec-WebSocket-Accept mismatch")
		}
	})

	t.Run("missing upgrade header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Del("Upgrade")
		err := verifyServerResponse(resp, key, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with missing Upgrade header")
		}
		if !errors.Is(err, errUpgradeHeaderNotWebSocket) {
			t.Fatalf("error = %q; want to be %v", err, errUpgradeHeaderNotWebSocket)
		}
	})

	t.Run("missing connection header", func(t *testing.T) {
		t.Parallel()

		resp := validResponse()
		resp.Header.Del("Connection")
		err := verifyServerResponse(resp, key, nil)
		if err == nil {
			t.Fatal("verifyServerResponse succeeded with missing Connection header")
		}
		if !errors.Is(err, errConnectionHeaderNotUpgrade) {
			t.Fatalf("error = %q; want to be %v", err, errConnectionHeaderNotUpgrade)
		}
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
			if req.Header.Get("Sec-WebSocket-Key") != "fixed-key" {
				t.Fatalf("Sec-WebSocket-Key = %q; want %q", req.Header.Get("Sec-WebSocket-Key"), "fixed-key")
			}

			return &http.Response{
				StatusCode: http.StatusSwitchingProtocols,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		})

		opts := &DialOptions{HTTPClient: &http.Client{Transport: transport}}
		resp, err := handshakeRequest(context.Background(), "wss://example.com/ws", "fixed-key", opts)
		if err != nil {
			t.Fatalf("handshakeRequest failed: %v", err)
		}
		if resp == nil {
			t.Fatal("handshakeRequest returned nil response")
		}
	})
}
