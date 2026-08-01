package websocket

import (
	"bufio"
	"encoding/binary"
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

func maskFramePayload(payload []byte, maskKey uint32) {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], maskKey)
	for i := range payload {
		payload[i] ^= key[i%4]
	}
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
