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
