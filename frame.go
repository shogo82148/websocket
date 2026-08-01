package websocket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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
	if len(payload) == 0 {
		return
	}

	var key [4]byte
	binary.BigEndian.PutUint32(key[:], maskKey)
	for i := range payload {
		payload[i] ^= key[i%4]
	}
}

func readFrameHeader(r *bufio.Reader, buf []byte) (frameHeader, error) {
	var header frameHeader

	if _, err := io.ReadFull(r, buf[:2]); err != nil {
		return header, err
	}
	first := buf[0]
	second := buf[1]

	header.fin = first&0x80 != 0
	header.rsv1 = first&0x40 != 0
	header.rsv2 = first&0x20 != 0
	header.rsv3 = first&0x10 != 0
	header.opCode = opCode(first & 0x0F)
	header.mask = second&0x80 != 0

	switch second & 0x7F {
	case 126:
		if _, err := io.ReadFull(r, buf[:2]); err != nil {
			return header, err
		}
		header.payloadLen = int64(binary.BigEndian.Uint16(buf[:2]))
	case 127:
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return header, err
		}
		payloadLen := binary.BigEndian.Uint64(buf[:8])
		if payloadLen&(uint64(1)<<63) != 0 {
			return header, errors.New("websocket: invalid frame payload length")
		}
		header.payloadLen = int64(payloadLen)
	default:
		header.payloadLen = int64(second & 0x7F)
	}

	if header.mask {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return header, err
		}
		header.maskKey = binary.BigEndian.Uint32(buf[:4])
	}

	return header, nil
}

func readFramePayload(r *bufio.Reader, header frameHeader) ([]byte, error) {
	if header.payloadLen < 0 {
		return nil, errors.New("websocket: invalid frame payload length")
	}

	payload := make([]byte, header.payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if header.mask {
		maskFramePayload(payload, header.maskKey)
	}
	return payload, nil
}

func readFrame(r *bufio.Reader, buf []byte) (frameHeader, []byte, error) {
	header, err := readFrameHeader(r, buf)
	if err != nil {
		return frameHeader{}, nil, err
	}
	payload, err := readFramePayload(r, header)
	if err != nil {
		return frameHeader{}, nil, err
	}
	return header, payload, nil
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
	extendedPayloadLen := byte(0)
	switch {
	case header.payloadLen < 126:
		payloadLen = byte(header.payloadLen)
	case header.payloadLen < 65536:
		payloadLen = 126
	default:
		payloadLen = 127
	}
	extendedPayloadLen = payloadLen
	if header.mask {
		payloadLen |= 0x80
	}
	if err := w.WriteByte(payloadLen); err != nil {
		return err
	}

	// Write the extended payload length if necessary.
	if extendedPayloadLen == 126 {
		binary.BigEndian.PutUint16(buf, uint16(header.payloadLen))
		if _, err := w.Write(buf[:2]); err != nil {
			return err
		}
	}
	if extendedPayloadLen == 127 {
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
