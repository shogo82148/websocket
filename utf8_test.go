package websocket

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

var utf8TestCases = []struct {
	input string
	valid bool
}{
	{"", true},
	{"Hello World!", true},
	{"こんにちは", true},
	{"Hello, 世界", true},
	{"🍣!=🍺", true},
	{"\xff", false},
	{"\xE3\x81", false},
}

func TestUTF8Reader(t *testing.T) {
	for _, tc := range utf8TestCases {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			ctx := t.Context()
			conn, _ := newTestConnWithInput(t, []byte{})
			r := strings.NewReader(tc.input)
			reader := &utf8Reader{ctx: ctx, r: r, conn: conn}
			got, err := io.ReadAll(reader)
			if (err == nil) != tc.valid {
				t.Errorf("expected valid=%v, got err=%v", tc.valid, err)
			}
			if tc.valid && string(got) != tc.input {
				t.Errorf("expected %q, got %q", tc.input, string(got))
			}
		})
	}
}

func TestUTF8Writer(t *testing.T) {
	write := func(w io.WriteCloser, data []byte) error {
		_, err := w.Write(data)
		if err != nil {
			return err
		}
		return w.Close()
	}

	for _, tc := range utf8TestCases {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			ctx := t.Context()
			conn, _ := newTestConnWithInput(t, []byte{})
			rwc := new(testReadWriteCloser)
			writer := &utf8Writer{ctx: ctx, w: rwc, conn: conn}
			err := write(writer, []byte(tc.input))
			if (err == nil) != tc.valid {
				t.Errorf("expected valid=%v, got err=%v", tc.valid, err)
			}
			if tc.valid && rwc.w.String() != tc.input {
				t.Errorf("expected %q, got %q", tc.input, rwc.w.String())
			}
		})
	}
}
