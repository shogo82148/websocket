package websocket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

type opCode byte

const (
	opContinuation opCode = 0x0
	opText         opCode = 0x1
	opBinary       opCode = 0x2
	opClose        opCode = 0x8
	opPing         opCode = 0x9
	opPong         opCode = 0xA
)

// maxControlPayload is the maximum length of a control frame payload.
// See https://tools.ietf.org/html/rfc6455#section-5.5.
const maxControlPayload = 125

type frameHeader struct {
	fin        bool
	rsv1       bool
	rsv2       bool
	rsv3       bool
	opCode     opCode
	mask       bool
	maskKey    uint32
	payloadLen int64
}

func maskFramePayload(payload []byte, key uint32) uint32 {
	for i := range payload {
		payload[i] ^= byte(key >> 24)
		key = bits.RotateLeft32(key, 8)
	}
	return key
}

func readFrameHeader(br *bufio.Reader) (frameHeader, error) {
	var h frameHeader

	// Read the first byte of the frame header.
	b, err := br.ReadByte()
	if err != nil {
		return h, err
	}
	h.fin = b&0x80 != 0
	h.rsv1 = b&0x40 != 0
	h.rsv2 = b&0x20 != 0
	h.rsv3 = b&0x10 != 0
	h.opCode = opCode(b & 0x0F)

	// Read the second byte of the frame header (payload length and mask).
	b, err = br.ReadByte()
	if err != nil {
		return h, err
	}
	h.mask = b&0x80 != 0
	payloadLen := int64(b & 0x7F)

	// Read the extended payload length if necessary.
	switch payloadLen {
	case 126:
		var buf [2]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return h, err
		}
		h.payloadLen = int64(binary.BigEndian.Uint16(buf[:]))
	case 127:
		var buf [8]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return h, err
		}
		h.payloadLen = int64(binary.BigEndian.Uint64(buf[:]))
		if h.payloadLen < 0 {
			return h, errors.New("websocket: invalid payload length")
		}
	default:
		h.payloadLen = payloadLen
	}

	// Read the mask key if necessary.
	if h.mask {
		var buf [4]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return h, err
		}
		h.maskKey = binary.BigEndian.Uint32(buf[:])
	}

	return h, nil
}

func writeFrameHeader(bw *bufio.Writer, h frameHeader) error {
	// Write the first byte of the frame header.
	var b byte
	if h.fin {
		b |= 0x80
	}
	if h.rsv1 {
		b |= 0x40
	}
	if h.rsv2 {
		b |= 0x20
	}
	if h.rsv3 {
		b |= 0x10
	}
	b |= byte(h.opCode)
	if err := bw.WriteByte(b); err != nil {
		return err
	}

	// Write the second byte of the frame header (payload length and mask).
	switch {
	case h.payloadLen < 126:
		b = byte(h.payloadLen)
	case h.payloadLen < 65536:
		b = 126
	default:
		b = 127
	}
	payloadLen := b
	if h.mask {
		b |= 0x80
	}
	if err := bw.WriteByte(b); err != nil {
		return err
	}

	// Write the extended payload length if necessary.
	var buf [8]byte
	switch payloadLen {
	case 126:
		binary.BigEndian.PutUint16(buf[:], uint16(h.payloadLen))
		if _, err := bw.Write(buf[:2]); err != nil {
			return err
		}
	case 127:
		binary.BigEndian.PutUint64(buf[:8], uint64(h.payloadLen))
		if _, err := bw.Write(buf[:8]); err != nil {
			return err
		}
	}

	// Write the mask key if necessary.
	if h.mask {
		binary.BigEndian.PutUint32(buf[:4], h.maskKey)
		if _, err := bw.Write(buf[:4]); err != nil {
			return err
		}
	}
	return nil
}
