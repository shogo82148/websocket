package websocket

import (
	"bytes"
	"testing"
)

func TestCompressionModeString(t *testing.T) {
	tests := []struct {
		mode     CompressionMode
		expected string
	}{
		{CompressionDisabled, "disabled"},
		{CompressionNoContextTakeover, "no_context_takeover"},
		{CompressionContextTakeover, "context_takeover"},
		{CompressionMode(42), "unknown(42)"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.mode.String()
			if got != test.expected {
				t.Errorf("CompressionMode.String() = %q; want %q", got, test.expected)
			}
		})
	}
}

func TestCompressionOptionsString(t *testing.T) {
	tests := []struct {
		opts     *compressionOptions
		expected string
	}{
		{
			&compressionOptions{
				clientNoContextTakeover: false,
				serverNoContextTakeover: false,
			},
			"permessage-deflate",
		},
		{
			&compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: false,
			},
			"permessage-deflate; client_no_context_takeover",
		},
		{
			&compressionOptions{
				clientNoContextTakeover: false,
				serverNoContextTakeover: true,
			},
			"permessage-deflate; server_no_context_takeover",
		},
		{
			&compressionOptions{
				clientNoContextTakeover: true,
				serverNoContextTakeover: true,
			},
			"permessage-deflate; client_no_context_takeover; server_no_context_takeover",
		},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.opts.String()
			if got != test.expected {
				t.Errorf("compressionOptions.String() = %q; want %q", got, test.expected)
			}
		})
	}
}

func BenchmarkCompressionOptionsString(b *testing.B) {
	opts := &compressionOptions{
		clientNoContextTakeover: true,
		serverNoContextTakeover: true,
	}
	b.ResetTimer()
	for b.Loop() {
		_ = opts.String()
	}
}

func TestSlidingWindowInit(t *testing.T) {
	t.Run("default size", func(t *testing.T) {
		var sw slidingWindow
		sw.init(0)

		if len(sw.buf) != 0 {
			t.Fatalf("len(sw.buf) = %d; want 0", len(sw.buf))
		}
		if cap(sw.buf) != 32*1024 {
			t.Fatalf("cap(sw.buf) = %d; want %d", cap(sw.buf), 32*1024)
		}
		sw.close()
	})

	t.Run("custom size", func(t *testing.T) {
		var sw slidingWindow
		sw.init(1024)

		if len(sw.buf) != 0 {
			t.Fatalf("len(sw.buf) = %d; want 0", len(sw.buf))
		}
		if cap(sw.buf) != 1024 {
			t.Fatalf("cap(sw.buf) = %d; want %d", cap(sw.buf), 1024)
		}
		sw.close()
	})

	t.Run("no reinit when already initialized", func(t *testing.T) {
		sw := slidingWindow{buf: make([]byte, 0, 64)}
		sw.init(1024)
		if cap(sw.buf) != 64 {
			t.Fatalf("cap(sw.buf) = %d; want %d", cap(sw.buf), 64)
		}
	})
}

func TestSlidingWindowWrite(t *testing.T) {
	t.Run("append without overflow", func(t *testing.T) {
		sw := slidingWindow{buf: make([]byte, 0, 8)}
		sw.write([]byte("abc"))
		sw.write([]byte("de"))

		if got, want := sw.buf, []byte("abcde"); !bytes.Equal(got, want) {
			t.Fatalf("sw.buf = %q; want %q", got, want)
		}
	})

	t.Run("discard oldest when overflow", func(t *testing.T) {
		sw := slidingWindow{buf: make([]byte, 0, 8)}
		sw.write([]byte("abcdef"))
		sw.write([]byte("ghij"))

		if got, want := sw.buf, []byte("cdefghij"); !bytes.Equal(got, want) {
			t.Fatalf("sw.buf = %q; want %q", got, want)
		}
	})

	t.Run("keep tail when single write exceeds capacity", func(t *testing.T) {
		sw := slidingWindow{buf: make([]byte, 0, 8)}
		sw.write([]byte("abcdefghijklmnopqrstuvwxyz"))

		if got, want := sw.buf, []byte("stuvwxyz"); !bytes.Equal(got, want) {
			t.Fatalf("sw.buf = %q; want %q", got, want)
		}
	})
}

func TestSlidingWindowClose(t *testing.T) {
	var sw slidingWindow
	sw.init(16)
	sw.write([]byte("hello world"))

	sw.close()

	if len(sw.buf) != 0 {
		t.Fatalf("len(sw.buf) = %d; want 0", len(sw.buf))
	}
	if cap(sw.buf) != 16 {
		t.Fatalf("cap(sw.buf) = %d; want %d", cap(sw.buf), 16)
	}
}
