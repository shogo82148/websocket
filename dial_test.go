package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDialSubprotocol(t *testing.T) {
	t.Parallel()

	serverSubprotocolCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := Accept(w, r, &AcceptOptions{Subprotocols: []string{"chat", "superchat"}})
		if err != nil {
			t.Errorf("Accept failed: %v", err)
			return
		}
		serverSubprotocolCh <- conn.Subprotocol()
		conn.CloseNow()
	}))
	defer ts.Close()

	conn, resp, err := Dial(t.Context(), ts.URL, &DialOptions{Subprotocols: []string{"binary", "chat"}})
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer resp.Body.Close()
	defer conn.CloseNow()

	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != "chat" {
		t.Fatalf("unexpected Sec-WebSocket-Protocol header: got %q, want %q", got, "chat")
	}
	if got := conn.Subprotocol(); got != "chat" {
		t.Fatalf("conn.Subprotocol() = %q, want %q", got, "chat")
	}
	if got := <-serverSubprotocolCh; got != "chat" {
		t.Fatalf("server conn.Subprotocol() = %q, want %q", got, "chat")
	}
}

func TestVerifySubprotocol(t *testing.T) {
	t.Parallel()

	if err := verifySubprotocol("", []string{"chat"}); err != nil {
		t.Fatalf("verifySubprotocol empty = %v, want nil", err)
	}
	if err := verifySubprotocol("chat", []string{"binary", "chat"}); err != nil {
		t.Fatalf("verifySubprotocol requested = %v, want nil", err)
	}
	if err := verifySubprotocol("superchat", []string{"binary", "chat"}); err == nil {
		t.Fatal("verifySubprotocol unexpected protocol error = nil, want error")
	}
}
