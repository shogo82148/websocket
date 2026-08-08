package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAccept(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, nil)
			if err != nil {
				t.Errorf("Accept failed: %v", err)
				return
			}
			conn.CloseNow()
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusSwitchingProtocols)
		}
		if resp.Header.Get("Upgrade") != "websocket" {
			t.Errorf("unexpected Upgrade header: got %q, want %q", resp.Header.Get("Upgrade"), "websocket")
		}
		if resp.Header.Get("Connection") != "Upgrade" {
			t.Errorf("unexpected Connection header: got %q, want %q", resp.Header.Get("Connection"), "Upgrade")
		}
		if resp.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
			t.Errorf("unexpected Sec-WebSocket-Accept header: got %q, want %q", resp.Header.Get("Sec-WebSocket-Accept"), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
		}
	})

	t.Run("invalid method", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid method")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
		}
	})

	t.Run("missing upgrade header", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing upgrade header")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		// h.Set("Upgrade", "websocket") // omit to simulate missing header
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusUpgradeRequired)
		}
		if resp.Header.Get("Upgrade") != "websocket" {
			t.Errorf("unexpected Upgrade header: got %q, want %q", resp.Header.Get("Upgrade"), "websocket")
		}
		if resp.Header.Get("Connection") != "Upgrade" {
			t.Errorf("unexpected Connection header: got %q, want %q", resp.Header.Get("Connection"), "Upgrade")
		}
	})

	t.Run("missing connection header", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing connection header")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		// h.Set("Connection", "Upgrade") // omit to simulate missing header
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusUpgradeRequired)
		}
		if resp.Header.Get("Upgrade") != "websocket" {
			t.Errorf("unexpected Upgrade header: got %q, want %q", resp.Header.Get("Upgrade"), "websocket")
		}
		if resp.Header.Get("Connection") != "Upgrade" {
			t.Errorf("unexpected Connection header: got %q, want %q", resp.Header.Get("Connection"), "Upgrade")
		}
	})

	t.Run("invalid websocket version", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid websocket version")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "12")                   // invalid version
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
		if resp.Header.Get("Sec-WebSocket-Version") != "13" {
			t.Errorf("unexpected Sec-WebSocket-Version header: got %q, want %q", resp.Header.Get("Sec-WebSocket-Version"), "13")
		}
	})

	t.Run("missing websocket key", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing websocket key")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		// h.Set("Sec-WebSocket-Key", "") // omit to simulate missing key
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("multiple websocket keys", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for multiple websocket keys")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("invalid base64 websocket keys", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid base64 websocket keys")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Add("Sec-WebSocket-Key", "!!invalid-base64!!") // invalid base64
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("short websocket keys", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for short websocket keys")
			}
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Add("Sec-WebSocket-Key", "c2hvcnQ=") // short key
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("negotiate extensions", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, &AcceptOptions{
				CompressionMode: CompressionContextTakeover,
			})
			if err != nil {
				t.Errorf("Accept failed: %v", err)
				return
			}
			conn.CloseNow()
		}))
		defer ts.Close()

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")
		h.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.Header.Get("Sec-WebSocket-Extensions") != "permessage-deflate" {
			t.Errorf("unexpected Sec-WebSocket-Extensions header: got %q, want %q", resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")
		}
	})
}

func BenchmarkAcceptHeader(b *testing.B) {
	for b.Loop() {
		acceptHeader("dGhlIHNhbXBsZSBub25jZQ==")
	}
}
