package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

func TestFrameHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header frameHeader
	}{
		{
			name: "short payload",
			header: frameHeader{
				fin:        true,
				opCode:     opText,
				payloadLen: 125,
			},
		},
		{
			name: "extended payload with mask",
			header: frameHeader{
				rsv1:       true,
				opCode:     opBinary,
				mask:       true,
				maskKey:    0x01020304,
				payloadLen: 126,
			},
		},
		{
			name: "extended 64-bit payload",
			header: frameHeader{
				fin:        true,
				rsv2:       true,
				rsv3:       true,
				opCode:     opPing,
				payloadLen: 65536,
			},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var data bytes.Buffer
			writer := bufio.NewWriter(&data)
			var buf [8]byte
			if err := writeFrameHeader(writer, tc.header, buf[:]); err != nil {
				t.Fatalf("writeFrameHeader failed: %v", err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			reader := bufio.NewReader(bytes.NewReader(data.Bytes()))
			got, err := readFrameHeader(reader, buf[:])
			if err != nil {
				t.Fatalf("readFrameHeader failed: %v", err)
			}

			if got != tc.header {
				t.Fatalf("frameHeader round-trip mismatch: got %#v, want %#v", got, tc.header)
			}
		})
	}
}

func TestReadFrameHeaderInvalidPayloadLen(t *testing.T) {
	t.Parallel()

	data := []byte{
		0x82,
		0x7f,
		0x80, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	reader := bufio.NewReader(bytes.NewReader(data))
	var buf [8]byte
	_, err := readFrameHeader(reader, buf[:])
	if !errors.Is(err, errors.New("websocket: invalid frame payload length")) && (err == nil || err.Error() != "websocket: invalid frame payload length") {
		t.Fatalf("readFrameHeader error = %v, want invalid frame payload length", err)
	}
}

func TestReadFrameMaskedPayload(t *testing.T) {
	t.Parallel()

	maskedPayload := []byte{0x49, 0x67, 0x6f, 0x68, 0x6e}
	data := []byte{
		0x81,
		0x80 | byte(len(maskedPayload)),
		0x01, 0x02, 0x03, 0x04,
	}
	data = append(data, maskedPayload...)

	reader := bufio.NewReader(bytes.NewReader(data))
	var buf [8]byte
	header, payload, err := readFrame(reader, buf[:])
	if err != nil {
		t.Fatalf("readFrame failed: %v", err)
	}

	if !header.mask {
		t.Fatal("readFrame header.mask = false, want true")
	}
	if string(payload) != "Hello" {
		t.Fatalf("readFrame payload = %q, want %q", payload, "Hello")
	}
}
