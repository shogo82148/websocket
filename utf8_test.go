package websocket

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

var utf8TestCases = []struct {
	input string
	valid bool
}{
	// valid UTF-8 strings
	{"", true},
	{"Hello World!", true},
	{"こんにちは", true},
	{"Hello, 世界", true},
	{"🍣!=🍺", true},

	// invalid UTF-8 strings
	{"\xff", false},                     // invalid byte
	{"\xE3\x81", false},                 // incomplete 3-byte sequence
	{"\xc0\xaf", false},                 // overlong encoding
	{"\xed\xa0\x80", false},             // surrogate half
	{"\xf0\x90\x80\x80", false},         // overlong encoding
	{"\xf8\x88\x80\x80\x80", false},     // 5-byte sequence (invalid)
	{"\xfc\x84\x80\x80\x80\x80", false}, // 6-byte sequence (invalid)
}

func TestUTF8States(t *testing.T) {
	for i, state := range utf8States {
		for j, b := range state {
			if int(b) < 0 || int(b) >= len(utf8States) {
				t.Errorf("invalid state at utf8States[%d][%d]: %d", i, j, b)
			}
		}
	}
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

func TestUTF8Reader_OneByte(t *testing.T) {
	for _, tc := range utf8TestCases {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			ctx := t.Context()
			conn, _ := newTestConnWithInput(t, []byte{})
			r := iotest.OneByteReader(strings.NewReader(tc.input))
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

func TestUTF8Writer_OneByte(t *testing.T) {
	write := func(w io.WriteCloser, data []byte) error {
		for _, b := range data {
			_, err := w.Write([]byte{b})
			if err != nil {
				return err
			}
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

func FuzzUTF8Reader(f *testing.F) {
	for _, tc := range utf8TestCases {
		f.Add(tc.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		valid := utf8.ValidString(input)
		ctx := t.Context()
		conn, _ := newTestConnWithInput(t, []byte{})
		r := strings.NewReader(input)
		reader := &utf8Reader{ctx: ctx, r: r, conn: conn}
		_, err := io.Copy(io.Discard, reader)
		if (err == nil) != valid {
			t.Errorf("expected valid=%v, got err=%v", valid, err)
		}
	})
}

func BenchmarkUTF8Reader(b *testing.B) {
	b.Run("ascii-only", func(b *testing.B) {
		var r strings.Reader
		input := strings.Repeat("Hello, World!", 1000)
		ctx := b.Context()
		conn, _ := newTestConnWithInput(b, []byte{})
		reader := &utf8Reader{conn: conn}
		b.ResetTimer()
		b.SetBytes(int64(len(input)))
		for b.Loop() {
			r.Reset(input)
			reader.reset(ctx, &r)
			_, err := io.Copy(io.Discard, reader)
			if err != nil {
				b.Fatalf("io.Copy failed: %v", err)
			}
		}
	})

	b.Run("grouped", func(b *testing.B) {
		var r strings.Reader
		input := strings.Repeat("Hello, 世界🍺", 1000)
		ctx := b.Context()
		conn, _ := newTestConnWithInput(b, []byte{})
		reader := &utf8Reader{conn: conn}
		b.ResetTimer()
		b.SetBytes(int64(len(input)))
		for b.Loop() {
			r.Reset(input)
			reader.reset(ctx, &r)
			_, err := io.Copy(io.Discard, reader)
			if err != nil {
				b.Fatalf("io.Copy failed: %v", err)
			}
		}
	})

	b.Run("one-byte", func(b *testing.B) {
		var r strings.Reader
		input := strings.Repeat("Hello, 世界🍺", 1000)
		ctx := b.Context()
		conn, _ := newTestConnWithInput(b, []byte{})
		reader := &utf8Reader{conn: conn}
		b.ResetTimer()
		b.SetBytes(int64(len(input)))
		for b.Loop() {
			r.Reset(input)
			reader.reset(ctx, iotest.OneByteReader(&r))
			_, err := io.Copy(io.Discard, reader)
			if err != nil {
				b.Fatalf("io.Copy failed: %v", err)
			}
		}
	})
}

func BenchmarkUTF8(b *testing.B) {
	input := strings.Repeat("Hello, 世界🍺", 1000)
	b.ResetTimer()
	b.SetBytes(int64(len(input)))
	for b.Loop() {
		if !utf8.ValidString(input) {
			b.Fatalf("invalid UTF-8 string")
		}
	}
}
