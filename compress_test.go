package websocket

import "testing"

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
		opts.String()
	}
}
