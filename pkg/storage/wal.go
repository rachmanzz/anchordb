package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// WAL OpCodes
const (
	walMagic byte = 0xA1 // AnchorDB WAL Magic
	walVer   byte = 2

	WalOpSave   byte = 1
	WalOpDelete byte = 2
	WalOpNode   byte = 3
)

// WALManager manages an append-only write-ahead log for AnchorDB crash durability.
type WALManager struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// OpenWAL creates or opens a WAL file at path in append mode.
func OpenWAL(path string) (*WALManager, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal %s: %w", path, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WALManager{
		file: f,
		path: path,
	}, nil
}

// Close closes the underlying WAL file descriptor.
func (w *WALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// CurrentOffset returns the current byte offset of the WAL file.
func (w *WALManager) CurrentOffset() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, fmt.Errorf("wal file is closed")
	}
	return w.file.Seek(0, io.SeekCurrent)
}

// writeEntry encodes and appends a framed entry:
// [1 byte Magic][1 byte Version][1 byte OpCode][4 byte PayloadLen][4 byte CRC32][Payload]
func (w *WALManager) writeEntry(op byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("wal file is closed")
	}

	checksum := crc32.ChecksumIEEE(payload)
	var hdr [11]byte
	hdr[0] = walMagic
	hdr[1] = walVer
	hdr[2] = op
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[7:11], checksum)

	if _, err := w.file.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.file.Write(payload); err != nil {
			return err
		}
	}
	return w.file.Sync()
}

// WriteSave writes an entity save/update event to WAL.
// Payload: [2 byte NameLen][Name][1 byte Operation][2 byte AttrCount][(2 byte KeyLen, Key, 4 byte Value)...]
func (w *WALManager) WriteSave(name string, cr CompactRecord) error {
	nameBytes := []byte(name)
	payloadLen := 2 + len(nameBytes) + 1 + 2
	for _, attr := range cr.Attrs {
		payloadLen += 2 + len(attr.Key) + 4
	}

	buf := make([]byte, payloadLen)
	offset := 0

	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(nameBytes)))
	offset += 2
	copy(buf[offset:], nameBytes)
	offset += len(nameBytes)

	buf[offset] = cr.Operation
	offset += 1

	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(cr.Attrs)))
	offset += 2

	for _, attr := range cr.Attrs {
		kBytes := []byte(attr.Key)
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(kBytes)))
		offset += 2
		copy(buf[offset:], kBytes)
		offset += len(kBytes)

		binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(attr.Value))
		offset += 4
	}

	return w.writeEntry(WalOpSave, buf)
}

// WriteDelete writes an entity deletion event to WAL.
// Payload: [2 byte NameLen][Name]
func (w *WALManager) WriteDelete(name string) error {
	nameBytes := []byte(name)
	buf := make([]byte, 2+len(nameBytes))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(nameBytes)))
	copy(buf[2:], nameBytes)

	return w.writeEntry(WalOpDelete, buf)
}

// WriteNode writes a graph node append event to WAL.
// Payload: [2 byte WordLen][Word][4 byte X][4 byte Y][16 byte Roles][2 byte DomainID]
func (w *WALManager) WriteNode(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) error {
	wordBytes := []byte(word)
	payloadLen := 2 + len(wordBytes) + 4 + 4 + 16 + 2
	buf := make([]byte, payloadLen)
	offset := 0

	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(wordBytes)))
	offset += 2
	copy(buf[offset:], wordBytes)
	offset += len(wordBytes)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(coord.X))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(coord.Y))
	offset += 4

	for r := 0; r < 4; r++ {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(roles[r]))
		offset += 4
	}

	binary.LittleEndian.PutUint16(buf[offset:offset+2], domainID)
	return w.writeEntry(WalOpNode, buf)
}

// Checkpoint truncates WAL entries that were already captured in a disk snapshot.
func (w *WALManager) Checkpoint(checkpointOffset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}

	fi, err := w.file.Stat()
	if err != nil {
		return err
	}
	currentSize := fi.Size()

	if checkpointOffset >= currentSize {
		// All writes captured in snapshot; truncate to 0
		if err := w.file.Truncate(0); err != nil {
			return err
		}
		_, err = w.file.Seek(0, io.SeekStart)
		return err
	}

	// Writes arrived after snapshot; preserve remaining bytes from checkpointOffset to end
	remSize := currentSize - checkpointOffset
	buf := make([]byte, remSize)
	if _, err := w.file.ReadAt(buf, checkpointOffset); err != nil {
		return err
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads and verifies all valid WAL entries from the beginning of the file.
func (w *WALManager) Replay(handler func(op byte, name string, cr CompactRecord, word string, coord spatial.Point2D, roles [4]float32, domainID uint16) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, fmt.Errorf("wal file is closed")
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	count := 0
	var hdr [11]byte

	for {
		_, err := io.ReadFull(w.file, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			break
		}

		if hdr[0] != walMagic || hdr[1] != walVer {
			// Corrupt entry or partial write; stop replay gracefully
			break
		}

		op := hdr[2]
		payloadLen := binary.LittleEndian.Uint32(hdr[3:7])
		expectedChecksum := binary.LittleEndian.Uint32(hdr[7:11])

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			// Partial payload; stop at last valid entry
			break
		}

		if crc32.ChecksumIEEE(payload) != expectedChecksum {
			// Checksum mismatch; stop at last valid entry
			break
		}

		// Decode payload according to op
		switch op {
		case WalOpSave:
			if len(payload) < 4 {
				continue
			}
			offset := 0
			nLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
			offset += 2
			if offset+nLen > len(payload) {
				continue
			}
			name := string(payload[offset : offset+nLen])
			offset += nLen

			opStatus := uint8(1) // Default active
			if hdr[1] >= 2 {
				if offset+1 > len(payload) {
					continue
				}
				opStatus = payload[offset]
				offset += 1
			}

			if offset+2 > len(payload) {
				continue
			}
			attrCount := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
			offset += 2

			attrs := make([]AttrEntry, 0, attrCount)
			valid := true
			for a := 0; a < attrCount; a++ {
				if offset+2 > len(payload) {
					valid = false
					break
				}
				kLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
				offset += 2
				if offset+kLen+4 > len(payload) {
					valid = false
					break
				}
				k := string(payload[offset : offset+kLen])
				offset += kLen
				valBits := binary.LittleEndian.Uint32(payload[offset : offset+4])
				offset += 4
				attrs = append(attrs, AttrEntry{Key: k, Value: math.Float32frombits(valBits)})
			}
			if valid && handler != nil {
				cr := CompactRecord{Operation: opStatus, Attrs: attrs}
				if err := handler(op, name, cr, "", spatial.Point2D{}, [4]float32{}, 0); err != nil {
					return count, err
				}
				count++
			}

		case WalOpDelete:
			if len(payload) < 2 {
				continue
			}
			nLen := int(binary.LittleEndian.Uint16(payload[0:2]))
			if 2+nLen > len(payload) {
				continue
			}
			name := string(payload[2 : 2+nLen])
			if handler != nil {
				if err := handler(op, name, CompactRecord{}, "", spatial.Point2D{}, [4]float32{}, 0); err != nil {
					return count, err
				}
				count++
			}

		case WalOpNode:
			if len(payload) < 2+4+4+16+2 {
				continue
			}
			offset := 0
			wLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
			offset += 2
			if offset+wLen+26 > len(payload) {
				continue
			}
			word := string(payload[offset : offset+wLen])
			offset += wLen

			x := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
			offset += 4
			y := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
			offset += 4

			var roles [4]float32
			for r := 0; r < 4; r++ {
				roles[r] = math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
				offset += 4
			}

			domainID := binary.LittleEndian.Uint16(payload[offset : offset+2])
			if handler != nil {
				if err := handler(op, "", CompactRecord{}, word, spatial.Point2D{X: x, Y: y}, roles, domainID); err != nil {
					return count, err
				}
				count++
			}
		}
	}

	// Seek to end for subsequent writes
	_, err := w.file.Seek(0, io.SeekEnd)
	return count, err
}
