package websocket

import (
	"fmt"
	"strings"
)

// CompressionMode represents the modes available to the permessage-deflate extension.
// See https://tools.ietf.org/html/rfc7692
//
// Works in all modern browsers except Safari which does not implement the permessage-deflate extension.
//
// Compression is only used if the peer supports the mode selected.
type CompressionMode int

const (
	// CompressionDisabled disables the negotiation of the permessage-deflate extension.
	//
	// This is the default. Do not enable compression without benchmarking for your particular use case first.
	CompressionDisabled CompressionMode = iota

	// CompressionContextTakeover compresses each message greater than 128 bytes reusing the 32 KB sliding window from
	// previous messages. i.e compression context across messages is preserved.
	//
	// As most WebSocket protocols are text based and repetitive, this compression mode can be very efficient.
	//
	// The memory overhead is a fixed 32 KB sliding window, a fixed 1.2 MB flate.Writer and a sync.Pool of 40 KB flate.Reader's
	// that are used when reading and then returned.
	//
	// Thus, it uses more memory than CompressionNoContextTakeover but compresses more efficiently.
	//
	// If the peer does not support CompressionContextTakeover then we will fall back to CompressionNoContextTakeover.
	CompressionContextTakeover

	// CompressionNoContextTakeover compresses each message greater than 512 bytes. Each message is compressed with
	// a new 1.2 MB flate.Writer pulled from a sync.Pool. Each message is read with a 40 KB flate.Reader pulled from
	// a sync.Pool.
	//
	// This means less efficient compression as the sliding window from previous messages will not be used but the
	// memory overhead will be lower as there will be no fixed cost for the flate.Writer nor the 32 KB sliding window.
	// Especially if the connections are long lived and seldom written to.
	//
	// Thus, it uses less memory than CompressionContextTakeover but compresses less efficiently.
	//
	// If the peer does not support CompressionNoContextTakeover then we will fall back to CompressionDisabled.
	CompressionNoContextTakeover
)

func (mode CompressionMode) String() string {
	switch mode {
	case CompressionDisabled:
		return "disabled"
	case CompressionContextTakeover:
		return "context_takeover"
	case CompressionNoContextTakeover:
		return "no_context_takeover"
	default:
		return fmt.Sprintf("unknown(%d)", int(mode))
	}
}

func (mode CompressionMode) opts() *compressionOptions {
	return &compressionOptions{
		clientNoContextTakeover: mode == CompressionNoContextTakeover,
		serverNoContextTakeover: mode == CompressionNoContextTakeover,
	}
}

type compressionOptions struct {
	clientNoContextTakeover bool
	serverNoContextTakeover bool
}

func (copts *compressionOptions) String() string {
	l := len("permessage-deflate")
	if copts.clientNoContextTakeover {
		l += len("; client_no_context_takeover")
	}
	if copts.serverNoContextTakeover {
		l += len("; server_no_context_takeover")
	}

	var b strings.Builder
	b.Grow(l)
	b.WriteString("permessage-deflate")
	if copts.clientNoContextTakeover {
		b.WriteString("; client_no_context_takeover")
	}
	if copts.serverNoContextTakeover {
		b.WriteString("; server_no_context_takeover")
	}
	return b.String()
}
