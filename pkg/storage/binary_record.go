package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	// MagicHeader is "ANCH" in ASCII (0x41, 0x4E, 0x43, 0x48)
	MagicHeader = 0x48434E41 // Little-endian for "ANCH"
	DBVersion   = 1

	HeaderSize = 64
	RecordSize = 40 // Exact 40-byte fixed binary record per node

	GridDim = 256 // 256x256 spatial cells
)

// Header represents the 64-byte file header of .anchordb
type Header struct {
	Magic        uint32  // "ANCH"
	Version      uint32  // 1
	NodeCount    uint64  // Total nodes stored
	GridCellsX   uint16  // 256
	GridCellsY   uint16  // 256
	GridOffset   uint64  // File offset of grid index
	NodesOffset  uint64  // File offset of fixed node records array
	StringOffset uint64  // File offset of word string pool
	EntityCount  uint64  // Total entity records stored
	EntityOffset uint64  // File offset of entity attributes section
	Reserved     [4]byte // Padding to 64 bytes
}

// Encode writes Header to a 64-byte buffer.
func (h *Header) Encode(buf []byte) {
	if len(buf) < HeaderSize {
		panic("buffer too small for Header")
	}
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], h.Version)
	binary.LittleEndian.PutUint64(buf[8:16], h.NodeCount)
	binary.LittleEndian.PutUint16(buf[16:18], h.GridCellsX)
	binary.LittleEndian.PutUint16(buf[18:20], h.GridCellsY)
	binary.LittleEndian.PutUint64(buf[20:28], h.GridOffset)
	binary.LittleEndian.PutUint64(buf[28:36], h.NodesOffset)
	binary.LittleEndian.PutUint64(buf[36:44], h.StringOffset)
	binary.LittleEndian.PutUint64(buf[44:52], h.EntityCount)
	binary.LittleEndian.PutUint64(buf[52:60], h.EntityOffset)
	copy(buf[60:64], h.Reserved[:])
}

// DecodeHeader reads Header from 64-byte slice.
func DecodeHeader(buf []byte) (*Header, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("buffer too small for header, need 64 bytes")
	}
	h := &Header{
		Magic:        binary.LittleEndian.Uint32(buf[0:4]),
		Version:      binary.LittleEndian.Uint32(buf[4:8]),
		NodeCount:    binary.LittleEndian.Uint64(buf[8:16]),
		GridCellsX:   binary.LittleEndian.Uint16(buf[16:18]),
		GridCellsY:   binary.LittleEndian.Uint16(buf[18:20]),
		GridOffset:   binary.LittleEndian.Uint64(buf[20:28]),
		NodesOffset:  binary.LittleEndian.Uint64(buf[28:36]),
		StringOffset: binary.LittleEndian.Uint64(buf[36:44]),
		EntityCount:  binary.LittleEndian.Uint64(buf[44:52]),
		EntityOffset: binary.LittleEndian.Uint64(buf[52:60]),
	}
	copy(h.Reserved[:], buf[60:64])

	if h.Magic != MagicHeader {
		return nil, fmt.Errorf("invalid magic header: expected 0x%X, got 0x%X", MagicHeader, h.Magic)
	}
	if h.Version != DBVersion {
		return nil, fmt.Errorf("unsupported database version: expected %d, got %d", DBVersion, h.Version)
	}
	return h, nil
}

// BinaryNodeRecord is an exact 40-byte fixed-size record on disk
type BinaryNodeRecord struct {
	ID         uint64     // 8 bytes
	X          float32    // 4 bytes
	Y          float32    // 4 bytes
	Roles      [4]float32 // 16 bytes: [Subject, Predicate, Object, Modifier]
	DomainID   uint16     // 2 bytes
	WordOffset uint32     // 4 bytes: byte offset in string pool
	WordLen    uint16     // 2 bytes: length of word in string pool
}

// Encode writes BinaryNodeRecord to a 40-byte buffer.
func (r *BinaryNodeRecord) Encode(buf []byte) {
	if len(buf) < RecordSize {
		panic("buffer too small for Record")
	}
	binary.LittleEndian.PutUint64(buf[0:8], r.ID)
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(r.X))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(r.Y))

	for i := 0; i < 4; i++ {
		offset := 16 + i*4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(r.Roles[i]))
	}

	binary.LittleEndian.PutUint16(buf[32:34], r.DomainID)
	binary.LittleEndian.PutUint32(buf[34:38], r.WordOffset)
	binary.LittleEndian.PutUint16(buf[38:40], r.WordLen)
}

// DecodeRecord reads BinaryNodeRecord from a 40-byte slice without heap allocations.
func DecodeRecord(buf []byte, r *BinaryNodeRecord) {
	r.ID = binary.LittleEndian.Uint64(buf[0:8])

	uX := binary.LittleEndian.Uint32(buf[8:12])
	r.X = math.Float32frombits(uX)

	uY := binary.LittleEndian.Uint32(buf[12:16])
	r.Y = math.Float32frombits(uY)

	for i := 0; i < 4; i++ {
		offset := 16 + i*4
		uR := binary.LittleEndian.Uint32(buf[offset : offset+4])
		r.Roles[i] = math.Float32frombits(uR)
	}

	r.DomainID = binary.LittleEndian.Uint16(buf[32:34])
	r.WordOffset = binary.LittleEndian.Uint32(buf[34:38])
	r.WordLen = binary.LittleEndian.Uint16(buf[38:40])
}

// WriteRecord writes a BinaryNodeRecord to an io.Writer.
func (r *BinaryNodeRecord) Write(w io.Writer) error {
	var buf [RecordSize]byte
	r.Encode(buf[:])
	_, err := w.Write(buf[:])
	return err
}
