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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, nil)
			if err != nil {
				t.Errorf("Accept failed: %v", err)
				return
			}
			conn.CloseNow()
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow

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
		if resp.Header.Get("Sec-Websocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
			t.Errorf("unexpected Sec-Websocket-Accept header: got %q, want %q", resp.Header.Get("Sec-Websocket-Accept"), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
		}
	})

	t.Run("negotiates subprotocol", func(t *testing.T) {
		t.Parallel()

		selected := make(chan string, 1)
		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, &AcceptOptions{Subprotocols: []string{"superchat", "chat"}})
			if err != nil {
				t.Errorf("Accept failed: %v", err)
				return
			}
			selected <- conn.Subprotocol()
			conn.CloseNow()
		}))

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Sec-Websocket-Version", "13")
		req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		req.Header.Set("Sec-Websocket-Protocol", "unknown, chat, superchat")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("Sec-Websocket-Protocol"); got != "chat" {
			t.Fatalf("Sec-Websocket-Protocol = %q; want %q", got, "chat")
		}
		if got := <-selected; got != "chat" {
			t.Fatalf("Conn.Subprotocol() = %q; want %q", got, "chat")
		}
	})

	t.Run("invalid method", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid method")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing upgrade header")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		// h.Set("Upgrade", "websocket") // omit to simulate missing header
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing connection header")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		// h.Set("Connection", "Upgrade") // omit to simulate missing header
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid websocket version")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "12")                   // invalid version
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Set("Origin", "http://example.com")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
		if resp.Header.Get("Sec-Websocket-Version") != "13" {
			t.Errorf("unexpected Sec-Websocket-Version header: got %q, want %q", resp.Header.Get("Sec-Websocket-Version"), "13")
		}
	})

	t.Run("missing websocket key", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for missing websocket key")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		// h.Set("Sec-Websocket-Key", "") // omit to simulate missing key
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for multiple websocket keys")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Add("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
		h.Add("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for invalid base64 websocket keys")
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Add("Sec-Websocket-Key", "!!invalid-base64!!") // invalid base64
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Errorf("Accept should have failed for short websocket keys")
			}
		}))
		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Add("Sec-Websocket-Key", "c2hvcnQ=") // short key
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

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, &AcceptOptions{
				CompressionMode: CompressionContextTakeover,
			})
			if err != nil {
				t.Errorf("Accept failed: %v", err)
				return
			}
			conn.CloseNow()
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow

		// unknown extension.
		h.Add("Sec-Websocket-Extensions", "permessage-foo")

		// client_max_window_bits is too small.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; client_max_window_bits=7")

		// unsupported server_max_window_bits value.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=8")

		// duplicate client_no_context_takeover parameter.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; client_no_context_takeover; client_no_context_takeover")

		// duplicate server_no_context_takeover parameter.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; server_no_context_takeover; server_no_context_takeover")

		// duplicate client_max_window_bits parameter.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; client_max_window_bits=15; client_max_window_bits=15")

		// duplicate server_max_window_bits parameter.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; server_max_window_bits=15; server_max_window_bits=15")

		// unknown parameter.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; foo_bar")

		// valid extension, it will be accepted.
		h.Add("Sec-Websocket-Extensions", "permessage-deflate; client_max_window_bits=15")

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.Header.Get("Sec-Websocket-Extensions") != "permessage-deflate" {
			t.Errorf("unexpected Sec-Websocket-Extensions header: got %q, want %q", resp.Header.Get("Sec-Websocket-Extensions"), "permessage-deflate")
		}
	})

	t.Run("origin mismatch", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := Accept(w, r, nil)
			if err == nil {
				t.Error("Accept should have failed for origin mismatch")
				return
			}
		}))

		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("http.NewRequestWithContext failed: %v", err)
		}
		h := req.Header
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Origin", "http://foo.example.com")
		h.Set("Sec-Websocket-Version", "13")
		h.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") // betterleaks:allow

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("http.Client.Do failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("unexpected status code: got %d, want %d", resp.StatusCode, http.StatusForbidden)
		}
	})
}

func BenchmarkAcceptHeader(b *testing.B) {
	for b.Loop() {
		acceptHeader("dGhlIHNhbXBsZSBub25jZQ==")
	}
}

func TestParseInt(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		param   string
		want    int
		wantErr bool
	}{
		{"max_window_bits=15", 15, false},
		{"max_window_bits=8", 8, false},
		{`max_window_bits="15"`, 15, false},
		{"max_window_bits=-1", 0, true},
		{"max_window_bits=abc", 0, true},
		{"max_window_bits=08", 0, true},
		{"max_window_bits=", 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.param, func(t *testing.T) {
			got, err := parseInt(tc.param)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseInt(%q) error = %v, wantErr = %v", tc.param, err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("parseInt(%q) = %d, want %d", tc.param, got, tc.want)
			}
		})
	}
}
func TestValidateOrigin(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		origin   string
		host     string
		patterns []string
		success  bool
	}{
		{
			name:    "none",
			host:    "example.com",
			success: true,
		},
		{
			name:    "invalid origin",
			origin:  "$#)(*)$#@*$(#@*$)#@*%)#(@*%)#(@%#@$#@$#$#@$#@}{}{}",
			host:    "example.com",
			success: false,
		},
		{
			name:    "invalid port number",
			origin:  "http://foo.example.com:invalidport",
			host:    "example.com",
			success: false,
		},
		{
			name:    "host mismatch",
			origin:  "http://example.com",
			host:    "example1.com",
			success: false,
		},
		{
			name:    "host match",
			origin:  "http://example.com",
			host:    "example.com",
			success: true,
		},
		{
			name:    "host is case-insensitive",
			origin:  "https://examplE.com",
			host:    "example.com",
			success: true,
		},
		{
			name:   "origin patterns",
			origin: "https://two.examplE.com",
			host:   "example.com",
			patterns: []string{
				"https://*.example.com",
				"https://bar.com",
			},
			success: true,
		},
		{
			name:   "scheme mismatch",
			origin: "https://two.example.com",
			host:   "example.com",
			patterns: []string{
				"http://*.example.com",
			},
			success: false,
		},
		{
			name:   "origin patterns with scheme and port",
			origin: "https://foo.example.com:8443",
			host:   "example.com",
			patterns: []string{
				"https://foo.example.com:8443",
			},
			success: true,
		},
		{
			name:   "port mismatch",
			origin: "https://foo.example.com:8443",
			host:   "example.com",
			patterns: []string{
				"https://foo.example.com:443",
			},
			success: false,
		},
		{
			name:   "default port matches",
			origin: "https://foo.example.com",
			host:   "example.com",
			patterns: []string{
				"https://foo.example.com:443",
			},
			success: true,
		},
		{
			name:   "wildcard pattern does not match multiple subdomains",
			origin: "https://bar.foo.example.com",
			host:   "example.com",
			patterns: []string{
				"https://*.example.com",
			},
			success: false,
		},
		{
			name:   "wildcard pattern does not match root domain",
			origin: "https://example.org",
			host:   "example.com",
			patterns: []string{
				"https://*.example.org",
			},
			success: false,
		},
		{
			name:   "invalid pattern",
			origin: "https://foo.example.com",
			host:   "example.com",
			patterns: []string{
				"$#)(*)$#@*$(#@*$)#@*%)#(@*%)#(@%#@$#@$#$#@$#@}{}{}",
			},
			success: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			r := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://"+tc.host, nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}

			err := validateOrigin(r, tc.patterns)
			if (err == nil) != tc.success {
				t.Fatalf("validateOrigin() error = %v, want success = %v", err, tc.success)
			}
		})
	}
}
