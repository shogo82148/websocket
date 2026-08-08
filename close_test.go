package websocket

import (
	"bytes"
	"testing"
)

func TestCloseError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		ce      CloseError
		want    []byte
		success bool
	}{
		{
			name: "normal closure",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: "normal closure",
			},
			want:    []byte{0x03, 0xE8, 'n', 'o', 'r', 'm', 'a', 'l', ' ', 'c', 'l', 'o', 's', 'u', 'r', 'e'},
			success: true,
		},
		{
			name: "reason too long",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: string(bytes.Repeat([]byte{'a'}, maxCloseReason+1)),
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
		{
			name: "invalid utf-8 reason",
			ce: CloseError{
				Code:   StatusNormalClosure,
				Reason: string([]byte{0xff}),
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
		{
			name: "invalid close code",
			ce: CloseError{
				Code:   statusReserved,
				Reason: "invalid close code",
			},
			want:    []byte{0x03, 0xF3}, // StatusInternalError
			success: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.ce.bytes()
			if (err == nil) != tc.success {
				t.Fatalf("CloseError.bytes() error = %v, want success = %v", err, tc.success)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("CloseError.bytes() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseClosePayload(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload []byte
		want    CloseError
		success bool
	}{
		{
			name:    "no payload",
			payload: []byte{},
			want: CloseError{
				Code:   StatusNoStatusRcvd,
				Reason: "",
			},
			success: true,
		},
		{
			name:    "valid payload",
			payload: []byte{0x03, 0xE8, 'n', 'o', 'r', 'm', 'a', 'l', ' ', 'c', 'l', 'o', 's', 'u', 'r', 'e'},
			want: CloseError{
				Code:   StatusNormalClosure,
				Reason: "normal closure",
			},
			success: true,
		},
		{
			name:    "payload too short",
			payload: []byte{0x03},
			want:    CloseError{},
			success: false,
		},
		{
			name:    "invalid utf-8 reason",
			payload: []byte{0x03, 0xE8, 0xff},
			want:    CloseError{},
			success: false,
		},
		{
			name:    "invalid close code",
			payload: []byte{0x00, 0x00, 'i', 'n', 'v', 'a', 'l', 'i', 'd'},
			want:    CloseError{},
			success: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseClosePayload(tc.payload)
			if (err == nil) != tc.success {
				t.Fatalf("parseClosePayload() error = %v, want success = %v", err, tc.success)
			}
			if got != tc.want {
				t.Errorf("parseClosePayload() = %v, want %v", got, tc.want)
			}
		})
	}
}
