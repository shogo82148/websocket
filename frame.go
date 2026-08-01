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

func writeFrameHeader(w *bufio.Writer, header frameHeader, buf []byte) error {
	// Write the first byte of the frame header.
	var b byte
	if header.fin {
		b |= 0x80
	}
	if header.rsv1 {
		b |= 0x40
	}
	if header.rsv2 {
		b |= 0x20
	}
	if header.rsv3 {
		b |= 0x10
	}
	b |= byte(header.opCode)
	if err := w.WriteByte(b); err != nil {
		return err
	}

	// Write the second byte of the frame header (payload length and mask).
	var payloadLen byte
	switch {
	case header.payloadLen < 126:
		payloadLen = byte(header.payloadLen)
	case header.payloadLen < 65536:
		payloadLen = 126
	default:
		payloadLen = 127
	}
	if header.mask {
		payloadLen |= 0x80
	}
	if err := w.WriteByte(payloadLen); err != nil {
		return err
	}

	// Write the extended payload length if necessary.
	if payloadLen == 126 {
		binary.BigEndian.PutUint16(buf, uint16(header.payloadLen))
		if _, err := w.Write(buf[:2]); err != nil {
			return err
		}
	}
	if payloadLen == 127 {
		binary.BigEndian.PutUint64(buf, uint64(header.payloadLen))
		if _, err := w.Write(buf[:8]); err != nil {
			return err
		}
	}

	// Write the mask key if necessary.
	if header.mask {
		binary.BigEndian.PutUint32(buf, header.maskKey)
		if _, err := w.Write(buf[:4]); err != nil {
			return err
		}
	}
	return nil
}
