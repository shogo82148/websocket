package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAccept(t *testing.T) {
	t.Parallel()

	t.Run("accept", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, nil)
			if err != nil {
				t.Errorf("Accept failed: %v", err)
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
}

func BenchmarkAcceptHeader(b *testing.B) {
	for b.Loop() {
		acceptHeader("dGhlIHNhbXBsZSBub25jZQ==")
	}
}
