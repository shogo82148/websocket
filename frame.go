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

var maskFramePayload func(payload []byte, key uint32) uint32

func init() {
	buf := [2]byte{0x01, 0x00}
	v := binary.NativeEndian.Uint16(buf[:])
	if v == 0x0100 {
		maskFramePayload = maskFramePayloadBigEndian
	} else {
		maskFramePayload = maskFramePayloadLittleEndian
	}
}

func maskFramePayloadBigEndian(payload []byte, key uint32) uint32 {
	key64 := uint64(key)<<32 | uint64(key)
	for len(payload) >= 128 {
		v := binary.BigEndian.Uint64(payload[:8])
		binary.BigEndian.PutUint64(payload[:8], v^key64)
		v = binary.BigEndian.Uint64(payload[8:16])
		binary.BigEndian.PutUint64(payload[8:16], v^key64)
		v = binary.BigEndian.Uint64(payload[16:24])
		binary.BigEndian.PutUint64(payload[16:24], v^key64)
		v = binary.BigEndian.Uint64(payload[24:32])
		binary.BigEndian.PutUint64(payload[24:32], v^key64)
		v = binary.BigEndian.Uint64(payload[32:40])
		binary.BigEndian.PutUint64(payload[32:40], v^key64)
		v = binary.BigEndian.Uint64(payload[40:48])
		binary.BigEndian.PutUint64(payload[40:48], v^key64)
		v = binary.BigEndian.Uint64(payload[48:56])
		binary.BigEndian.PutUint64(payload[48:56], v^key64)
		v = binary.BigEndian.Uint64(payload[56:64])
		binary.BigEndian.PutUint64(payload[56:64], v^key64)
		v = binary.BigEndian.Uint64(payload[64:72])
		binary.BigEndian.PutUint64(payload[64:72], v^key64)
		v = binary.BigEndian.Uint64(payload[72:80])
		binary.BigEndian.PutUint64(payload[72:80], v^key64)
		v = binary.BigEndian.Uint64(payload[80:88])
		binary.BigEndian.PutUint64(payload[80:88], v^key64)
		v = binary.BigEndian.Uint64(payload[88:96])
		binary.BigEndian.PutUint64(payload[88:96], v^key64)
		v = binary.BigEndian.Uint64(payload[96:104])
		binary.BigEndian.PutUint64(payload[96:104], v^key64)
		v = binary.BigEndian.Uint64(payload[104:112])
		binary.BigEndian.PutUint64(payload[104:112], v^key64)
		v = binary.BigEndian.Uint64(payload[112:120])
		binary.BigEndian.PutUint64(payload[112:120], v^key64)
		v = binary.BigEndian.Uint64(payload[120:128])
		binary.BigEndian.PutUint64(payload[120:128], v^key64)
		payload = payload[128:]
	}

	for len(payload) >= 64 {
		v := binary.BigEndian.Uint64(payload[:8])
		binary.BigEndian.PutUint64(payload[:8], v^key64)
		v = binary.BigEndian.Uint64(payload[8:16])
		binary.BigEndian.PutUint64(payload[8:16], v^key64)
		v = binary.BigEndian.Uint64(payload[16:24])
		binary.BigEndian.PutUint64(payload[16:24], v^key64)
		v = binary.BigEndian.Uint64(payload[24:32])
		binary.BigEndian.PutUint64(payload[24:32], v^key64)
		v = binary.BigEndian.Uint64(payload[32:40])
		binary.BigEndian.PutUint64(payload[32:40], v^key64)
		v = binary.BigEndian.Uint64(payload[40:48])
		binary.BigEndian.PutUint64(payload[40:48], v^key64)
		v = binary.BigEndian.Uint64(payload[48:56])
		binary.BigEndian.PutUint64(payload[48:56], v^key64)
		v = binary.BigEndian.Uint64(payload[56:64])
		binary.BigEndian.PutUint64(payload[56:64], v^key64)
		payload = payload[64:]
	}

	for len(payload) >= 32 {
		v := binary.BigEndian.Uint64(payload[:8])
		binary.BigEndian.PutUint64(payload[:8], v^key64)
		v = binary.BigEndian.Uint64(payload[8:16])
		binary.BigEndian.PutUint64(payload[8:16], v^key64)
		v = binary.BigEndian.Uint64(payload[16:24])
		binary.BigEndian.PutUint64(payload[16:24], v^key64)
		v = binary.BigEndian.Uint64(payload[24:32])
		binary.BigEndian.PutUint64(payload[24:32], v^key64)
		payload = payload[32:]
	}

	for len(payload) >= 16 {
		v := binary.BigEndian.Uint64(payload[:8])
		binary.BigEndian.PutUint64(payload[:8], v^key64)
		v = binary.BigEndian.Uint64(payload[8:16])
		binary.BigEndian.PutUint64(payload[8:16], v^key64)
		payload = payload[16:]
	}

	for len(payload) >= 8 {
		v := binary.BigEndian.Uint64(payload[:8])
		binary.BigEndian.PutUint64(payload[:8], v^key64)
		payload = payload[8:]
	}

	for len(payload) >= 4 {
		v := binary.BigEndian.Uint32(payload[:4])
		binary.BigEndian.PutUint32(payload[:4], v^key)
		payload = payload[4:]
	}

	// xor remaining bytes.
	for i := range payload {
		payload[i] ^= byte(key >> 24)
		key = bits.RotateLeft32(key, 8)
	}
	return key
}

func maskFramePayloadLittleEndian(payload []byte, key uint32) uint32 {
	key = bits.ReverseBytes32(key)
	key64 := uint64(key)<<32 | uint64(key)

	for len(payload) >= 128 {
		v := binary.LittleEndian.Uint64(payload[:8])
		binary.LittleEndian.PutUint64(payload[:8], v^key64)
		v = binary.LittleEndian.Uint64(payload[8:16])
		binary.LittleEndian.PutUint64(payload[8:16], v^key64)
		v = binary.LittleEndian.Uint64(payload[16:24])
		binary.LittleEndian.PutUint64(payload[16:24], v^key64)
		v = binary.LittleEndian.Uint64(payload[24:32])
		binary.LittleEndian.PutUint64(payload[24:32], v^key64)
		v = binary.LittleEndian.Uint64(payload[32:40])
		binary.LittleEndian.PutUint64(payload[32:40], v^key64)
		v = binary.LittleEndian.Uint64(payload[40:48])
		binary.LittleEndian.PutUint64(payload[40:48], v^key64)
		v = binary.LittleEndian.Uint64(payload[48:56])
		binary.LittleEndian.PutUint64(payload[48:56], v^key64)
		v = binary.LittleEndian.Uint64(payload[56:64])
		binary.LittleEndian.PutUint64(payload[56:64], v^key64)
		v = binary.LittleEndian.Uint64(payload[64:72])
		binary.LittleEndian.PutUint64(payload[64:72], v^key64)
		v = binary.LittleEndian.Uint64(payload[72:80])
		binary.LittleEndian.PutUint64(payload[72:80], v^key64)
		v = binary.LittleEndian.Uint64(payload[80:88])
		binary.LittleEndian.PutUint64(payload[80:88], v^key64)
		v = binary.LittleEndian.Uint64(payload[88:96])
		binary.LittleEndian.PutUint64(payload[88:96], v^key64)
		v = binary.LittleEndian.Uint64(payload[96:104])
		binary.LittleEndian.PutUint64(payload[96:104], v^key64)
		v = binary.LittleEndian.Uint64(payload[104:112])
		binary.LittleEndian.PutUint64(payload[104:112], v^key64)
		v = binary.LittleEndian.Uint64(payload[112:120])
		binary.LittleEndian.PutUint64(payload[112:120], v^key64)
		v = binary.LittleEndian.Uint64(payload[120:128])
		binary.LittleEndian.PutUint64(payload[120:128], v^key64)
		payload = payload[128:]
	}

	for len(payload) >= 64 {
		v := binary.LittleEndian.Uint64(payload[:8])
		binary.LittleEndian.PutUint64(payload[:8], v^key64)
		v = binary.LittleEndian.Uint64(payload[8:16])
		binary.LittleEndian.PutUint64(payload[8:16], v^key64)
		v = binary.LittleEndian.Uint64(payload[16:24])
		binary.LittleEndian.PutUint64(payload[16:24], v^key64)
		v = binary.LittleEndian.Uint64(payload[24:32])
		binary.LittleEndian.PutUint64(payload[24:32], v^key64)
		v = binary.LittleEndian.Uint64(payload[32:40])
		binary.LittleEndian.PutUint64(payload[32:40], v^key64)
		v = binary.LittleEndian.Uint64(payload[40:48])
		binary.LittleEndian.PutUint64(payload[40:48], v^key64)
		v = binary.LittleEndian.Uint64(payload[48:56])
		binary.LittleEndian.PutUint64(payload[48:56], v^key64)
		v = binary.LittleEndian.Uint64(payload[56:64])
		binary.LittleEndian.PutUint64(payload[56:64], v^key64)
		payload = payload[64:]
	}

	for len(payload) >= 32 {
		v := binary.LittleEndian.Uint64(payload[:8])
		binary.LittleEndian.PutUint64(payload[:8], v^key64)
		v = binary.LittleEndian.Uint64(payload[8:16])
		binary.LittleEndian.PutUint64(payload[8:16], v^key64)
		v = binary.LittleEndian.Uint64(payload[16:24])
		binary.LittleEndian.PutUint64(payload[16:24], v^key64)
		v = binary.LittleEndian.Uint64(payload[24:32])
		binary.LittleEndian.PutUint64(payload[24:32], v^key64)
		payload = payload[32:]
	}

	for len(payload) >= 16 {
		v := binary.LittleEndian.Uint64(payload[:8])
		binary.LittleEndian.PutUint64(payload[:8], v^key64)
		v = binary.LittleEndian.Uint64(payload[8:16])
		binary.LittleEndian.PutUint64(payload[8:16], v^key64)
		payload = payload[16:]
	}

	for len(payload) >= 8 {
		v := binary.LittleEndian.Uint64(payload[:8])
		binary.LittleEndian.PutUint64(payload[:8], v^key64)
		payload = payload[8:]
	}

	for len(payload) >= 4 {
		v := binary.LittleEndian.Uint32(payload[:4])
		binary.LittleEndian.PutUint32(payload[:4], v^key)
		payload = payload[4:]
	}

	// xor remaining bytes.
	for i := range payload {
		payload[i] ^= byte(key)
		key = bits.RotateLeft32(key, -8)
	}
	key = bits.ReverseBytes32(key)
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
