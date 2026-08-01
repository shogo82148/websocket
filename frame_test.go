package websocket

import (
	"bufio"
	"bytes"
	"testing"
)

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
