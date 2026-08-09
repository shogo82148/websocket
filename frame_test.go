package websocket

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	mathrand "math/rand/v2"
	"slices"
	"testing"
)

func basicMask(payload []byte, key uint32) uint32 {
	for i := range payload {
		payload[i] ^= byte(key >> 24)
		key = bits.RotateLeft32(key, 8)
	}
	return key
}

func TestMaskFramePayload(t *testing.T) {
	testMask(t, "maskFramePayload", maskFramePayload)
	testMask(t, "maskFramePayloadBigEndian", maskFramePayloadBigEndian)
	testMask(t, "maskFramePayloadLittleEndian", maskFramePayloadLittleEndian)
}

func testMask(t *testing.T, name string, fn func(payload []byte, key uint32) uint32) {
	t.Run(name, func(t *testing.T) {
		for range 100 {
			// generate key
			var buf [4]byte
			rand.Read(buf[:])
			key := binary.BigEndian.Uint32(buf[:])

			n := mathrand.IntN(1024 * 1024)
			b := make([]byte, n)
			rand.Read(b)

			b2 := slices.Clone(b)
			b3 := slices.Clone(b)

			key2 := basicMask(b2, key)
			key3 := fn(b3, key)

			if key2 != key3 {
				t.Errorf("mask key = %x, want %x", key3, key2)
			}
			if !bytes.Equal(b2, b3) {
				t.Error("masked payload mismatch")
			}
		}
	})
}

func BenchmarkMaskFramePayload(b *testing.B) {
	b.Run("basic", func(b *testing.B) {
		data := make([]byte, 1024*1024) // 1MB
		maskKey := uint32(0x01020304)
		b.ResetTimer()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			basicMask(data, maskKey)
		}
	})

	b.Run("maskFramePayload", func(b *testing.B) {
		data := make([]byte, 1024*1024) // 1MB
		maskKey := uint32(0x01020304)
		b.ResetTimer()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			maskFramePayload(data, maskKey)
		}
	})

	b.Run("maskFramePayloadBigEndian", func(b *testing.B) {
		data := make([]byte, 1024*1024) // 1MB
		maskKey := uint32(0x01020304)
		b.ResetTimer()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			maskFramePayloadBigEndian(data, maskKey)
		}
	})

	b.Run("maskFramePayloadLittleEndian", func(b *testing.B) {
		data := make([]byte, 1024*1024) // 1MB
		maskKey := uint32(0x01020304)
		b.ResetTimer()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			maskFramePayloadLittleEndian(data, maskKey)
		}
	})
}

func TestWriteFrameHeader(t *testing.T) {
	tests := []struct {
		name     string
		header   frameHeader
		expected []byte
	}{
		{
			"short payload",
			frameHeader{
				fin:        true,
				rsv1:       false,
				rsv2:       false,
				rsv3:       false,
				opCode:     opText,
				mask:       false,
				maskKey:    0,
				payloadLen: 5,
			},
			[]byte{0x81, 0x05},
		},
		{
			"extended payload with mask",
			frameHeader{
				fin:        true,
				rsv1:       false,
				rsv2:       false,
				rsv3:       false,
				opCode:     opText,
				mask:       true,
				maskKey:    0x01020304,
				payloadLen: 300,
			},
			[]byte{0x81, 126 | 0x80, 0x01, 0x2c, 0x01, 0x02, 0x03, 0x04},
		},
		{
			"maximum 16-bit payload",
			frameHeader{
				fin:        true,
				rsv1:       false,
				rsv2:       false,
				rsv3:       false,
				opCode:     opText,
				mask:       false,
				maskKey:    0,
				payloadLen: 65535,
			},
			[]byte{0x81, 126, 0xff, 0xff},
		},
		{
			"extended 64-bit payload",
			frameHeader{
				fin:        true,
				rsv1:       false,
				rsv2:       false,
				rsv3:       false,
				opCode:     opText,
				mask:       false,
				maskKey:    0,
				payloadLen: 65536,
			},
			[]byte{0x81, 127, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			bw := bufio.NewWriter(buf)
			err := writeFrameHeader(bw, test.header)
			if err != nil {
				t.Fatalf("writeFrameHeader failed: %v", err)
			}
			bw.Flush()
			if !bytes.Equal(buf.Bytes(), test.expected) {
				t.Errorf("expected %v, got %v", test.expected, buf.Bytes())
			}
		})
	}
}

func TestReadFrameHeader(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expected    frameHeader
		wantErr     error
		wantErrText string
	}{
		{
			name:  "short payload",
			input: []byte{0x81, 0x05},
			expected: frameHeader{
				fin:        true,
				opCode:     opText,
				mask:       false,
				payloadLen: 5,
			},
		},
		{
			name:  "extended payload with mask",
			input: []byte{0x81, 0xFE, 0x01, 0x2C, 0x01, 0x02, 0x03, 0x04},
			expected: frameHeader{
				fin:        true,
				opCode:     opText,
				mask:       true,
				maskKey:    0x01020304,
				payloadLen: 300,
			},
		},
		{
			name:  "extended 64-bit payload",
			input: []byte{0x82, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
			expected: frameHeader{
				fin:        true,
				opCode:     opBinary,
				mask:       false,
				payloadLen: 65536,
			},
		},
		{
			name:    "missing first byte",
			input:   []byte{},
			wantErr: io.EOF,
		},
		{
			name:    "truncated extended payload length",
			input:   []byte{0x81, 0x7E, 0x01},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "truncated mask key",
			input:   []byte{0x81, 0x85, 0x01, 0x02, 0x03},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:        "invalid negative payload length",
			input:       []byte{0x81, 0x7F, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantErrText: "websocket: invalid payload length",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(test.input))
			got, err := readFrameHeader(br)

			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("readFrameHeader error = %v; want %v", err, test.wantErr)
				}
				return
			}
			if test.wantErrText != "" {
				if err == nil || err.Error() != test.wantErrText {
					t.Fatalf("readFrameHeader error = %v; want %q", err, test.wantErrText)
				}
				return
			}

			if err != nil {
				t.Fatalf("readFrameHeader failed: %v", err)
			}
			if got != test.expected {
				t.Fatalf("readFrameHeader = %+v; want %+v", got, test.expected)
			}
		})
	}
}
