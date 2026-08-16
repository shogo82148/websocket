package websocket

import (
	"compress/flate"
	"fmt"
	"io"
	"strings"
	"sync"
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

// These bytes are required to get flate.Reader to return.
// They are removed when sending to avoid the overhead as
// WebSocket framing tells when the message has ended but then
// we need to add them back otherwise flate.Reader keeps
// trying to read more bytes.
const deflateMessageTail = "\x00\x00\xff\xff" + // WebSocket Synchronized Padding
	"\x03\x00" // End-of-stream marker (BFINAL=1)
var deflateMessageTailBytes = []byte{0x00, 0x00, 0xff, 0xff}

// trimLastFourBytesWriter holds back the permessage-deflate tail emitted by a
// sync flush. RFC 7692 requires that tail to be omitted from the wire.
type trimLastFourBytesWriter struct {
	w    io.Writer
	tail []byte
}

func (tw *trimLastFourBytesWriter) reset() {
	tw.tail = tw.tail[:0]
}

func (tw *trimLastFourBytesWriter) Write(p []byte) (int, error) {
	const tailLen = 4

	if tw.tail == nil {
		tw.tail = make([]byte, 0, tailLen)
	}

	extra := len(tw.tail) + len(p) - tailLen
	if extra <= 0 {
		tw.tail = append(tw.tail, p...)
		return len(p), nil
	}

	fromTail := min(extra, len(tw.tail))
	if fromTail > 0 {
		if _, err := tw.w.Write(tw.tail[:fromTail]); err != nil {
			return 0, err
		}
		copy(tw.tail, tw.tail[fromTail:])
		tw.tail = tw.tail[:len(tw.tail)-fromTail]
	}

	if len(p) <= tailLen {
		tw.tail = append(tw.tail, p...)
		return len(p), nil
	}

	tw.tail = append(tw.tail, p[len(p)-tailLen:]...)
	n, err := tw.w.Write(p[:len(p)-tailLen])
	return n + tailLen, err
}

var flateReaderPool sync.Pool

func getFlateReader(r io.Reader, dict []byte) io.Reader {
	fr, ok := flateReaderPool.Get().(io.Reader)
	if !ok {
		return flate.NewReaderDict(r, dict)
	}
	fr.(flate.Resetter).Reset(r, dict)
	return fr
}

func putFlateReader(fr io.Reader) {
	flateReaderPool.Put(fr)
}

var flateWriterPool = [11]sync.Pool{}

func getFlateWriter(w io.Writer, level int) (*flate.Writer, error) {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		return nil, fmt.Errorf("flate: invalid compression level: %d", level)
	}
	fw, ok := flateWriterPool[level+2].Get().(*flate.Writer)
	if !ok {
		return flate.NewWriter(w, level)
	}
	fw.Reset(w)
	return fw, nil
}

func putFlateWriter(fw *flate.Writer, level int) {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		return
	}
	flateWriterPool[level+2].Put(fw)
}

type slidingWindow struct {
	buf []byte
}

var (
	swPoolMu sync.RWMutex
	swPool   = map[int]*sync.Pool{}
)

func slidingWindowPool(size int) *sync.Pool {
	swPoolMu.RLock()
	p, ok := swPool[size]
	swPoolMu.RUnlock()
	if ok {
		return p
	}

	swPoolMu.Lock()
	defer swPoolMu.Unlock()

	p, ok = swPool[size]
	if ok {
		return p
	}

	p = &sync.Pool{
		New: func() any {
			return &slidingWindow{
				buf: make([]byte, 0, size),
			}
		},
	}
	swPool[size] = p
	return p
}

func (sw *slidingWindow) init(size int) {
	if sw.buf != nil {
		return
	}
	if size <= 0 {
		size = 32 * 1024
	}

	p := slidingWindowPool(size)
	sw2 := p.Get().(*slidingWindow)
	*sw = *sw2
}

func (sw *slidingWindow) close() {
	sw.buf = sw.buf[:0]
	slidingWindowPool(cap(sw.buf)).Put(sw)
}

func (sw *slidingWindow) write(p []byte) {
	if len(p) > cap(sw.buf) {
		sw.buf = sw.buf[:cap(sw.buf)]
		copy(sw.buf, p[len(p)-cap(sw.buf):])
		return
	}

	left := cap(sw.buf) - len(sw.buf)
	if left < len(p) {
		spaceNeeded := len(p) - left
		n := copy(sw.buf, sw.buf[spaceNeeded:])
		sw.buf = sw.buf[:n]
	}

	sw.buf = append(sw.buf, p...)
}
