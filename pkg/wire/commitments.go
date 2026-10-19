package wire

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// CommitmentFileMagic is the 8-byte file header for commitments.bin.
	CommitmentFileMagic = "ANCHCOMM"
	// CommitmentFileVersion is the current version of the commitments file format.
	CommitmentFileVersion uint32 = 1

	// StateActive indicates the node commitment is active and confirmed.
	StateActive uint8 = 0x01
	// StateStagedPending indicates next commitment is staged awaiting operational confirmation.
	StateStagedPending uint8 = 0x02
)

var (
	// ErrUnknownNodeID is returned when reauth is requested for an unregistered node.
	ErrUnknownNodeID = errors.New("wire: node id not registered in commitments")
	// ErrInvalidPreimage is returned when validator SHA256 does not match active or staged commitment.
	ErrInvalidPreimage = errors.New("wire: invalid validator preimage")
	// ErrCorruptCommitmentFile is returned when commitments.bin fails validation.
	ErrCorruptCommitmentFile = errors.New("wire: corrupt commitment file")
)

// CommitmentRecord holds persistent commitment state for a registered connector node.
type CommitmentRecord struct {
	NodeID            string
	CurrentCommitment [32]byte
	StagedCommitment  [32]byte
	State             uint8
	LastRotatedAt     int64
}

// CommitmentStore manages persistent flat-binary storage of preimage commitments.
type CommitmentStore struct {
	mu       sync.RWMutex
	filePath string
	records  map[string]*CommitmentRecord
}

// OpenCommitmentStore opens or creates a commitment storage file.
func OpenCommitmentStore(filePath string) (*CommitmentStore, error) {
	if filePath == "" {
		return nil, errors.New("wire: commitment store path cannot be empty")
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wire: failed to create commitment directory: %w", err)
	}

	store := &CommitmentStore{
		filePath: filePath,
		records:  make(map[string]*CommitmentRecord),
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// New store, write initial header
		if err := store.persistSyncLocked(); err != nil {
			return nil, err
		}
		return store, nil
	}

	if err := store.load(); err != nil {
		return nil, err
	}

	return store, nil
}

func (cs *CommitmentStore) load() error {
	f, err := os.Open(cs.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var hdr [16]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("wire: failed to read commitment header: %w", err)
	}

	if string(hdr[0:8]) != CommitmentFileMagic {
		return fmt.Errorf("%w: invalid magic %q", ErrCorruptCommitmentFile, string(hdr[0:8]))
	}
	ver := binary.LittleEndian.Uint32(hdr[8:12])
	if ver != CommitmentFileVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrCorruptCommitmentFile, ver)
	}

	for {
		var lenBuf [2]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		nodeIDLen := int(binary.LittleEndian.Uint16(lenBuf[:]))

		recordPayloadLen := nodeIDLen + 32 + 32 + 1 + 8
		buf := make([]byte, recordPayloadLen+4) // payload + 4-byte CRC32
		if _, err := io.ReadFull(f, buf); err != nil {
			return fmt.Errorf("%w: truncated record", ErrCorruptCommitmentFile)
		}

		expectedCRC := binary.LittleEndian.Uint32(buf[recordPayloadLen:])

		h := crc32.NewIEEE()
		h.Write(lenBuf[:])
		h.Write(buf[:recordPayloadLen])
		if h.Sum32() != expectedCRC {
			return fmt.Errorf("%w: CRC32 mismatch on record", ErrCorruptCommitmentFile)
		}

		offset := 0
		nodeID := string(buf[offset : offset+nodeIDLen])
		offset += nodeIDLen

		var curr, staged [32]byte
		copy(curr[:], buf[offset:offset+32])
		offset += 32
		copy(staged[:], buf[offset:offset+32])
		offset += 32

		state := buf[offset]
		offset++

		lastRotated := int64(binary.LittleEndian.Uint64(buf[offset : offset+8]))

		cs.records[nodeID] = &CommitmentRecord{
			NodeID:            nodeID,
			CurrentCommitment: curr,
			StagedCommitment:  staged,
			State:             state,
			LastRotatedAt:     lastRotated,
		}
	}

	return nil
}

// RegisterInitial registers an initial commitment for a newly paired or enrolled node.
func (cs *CommitmentStore) RegisterInitial(nodeID string, commitment [32]byte) error {
	if nodeID == "" {
		return errors.New("wire: node id cannot be empty")
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	rec, exists := cs.records[nodeID]
	if exists {
		// Update existing node commitment
		rec.CurrentCommitment = commitment
		rec.StagedCommitment = [32]byte{}
		rec.State = StateActive
		rec.LastRotatedAt = time.Now().UnixNano()
	} else {
		cs.records[nodeID] = &CommitmentRecord{
			NodeID:            nodeID,
			CurrentCommitment: commitment,
			State:             StateActive,
			LastRotatedAt:     time.Now().UnixNano(),
		}
	}

	return cs.persistSyncLocked()
}

// StageReauth validates the current validator preimage and stages the next commitment.
// Returns (0, nil) on success, or an error code and error on failure.
func (cs *CommitmentStore) StageReauth(nodeID string, validator [32]byte, nextCommitment [32]byte) (uint16, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	rec, exists := cs.records[nodeID]
	if !exists {
		return ErrCodeUnauthorized, ErrUnknownNodeID
	}

	givenHash := sha256.Sum256(validator[:])

	// Case A: Validator matches CurrentCommitment (Normal Rotation)
	if subtle.ConstantTimeCompare(givenHash[:], rec.CurrentCommitment[:]) == 1 {
		rec.StagedCommitment = nextCommitment
		rec.State = StateStagedPending
		rec.LastRotatedAt = time.Now().UnixNano()
		if err := cs.persistSyncLocked(); err != nil {
			return 500, err
		}
		return 0, nil
	}

	// Case B: Validator matches StagedCommitment (Connection dropped after ACK, retry with new token)
	if subtle.ConstantTimeCompare(givenHash[:], rec.StagedCommitment[:]) == 1 {
		rec.CurrentCommitment = rec.StagedCommitment
		rec.StagedCommitment = nextCommitment
		rec.State = StateStagedPending
		rec.LastRotatedAt = time.Now().UnixNano()
		if err := cs.persistSyncLocked(); err != nil {
			return 500, err
		}
		return 0, nil
	}

	// Case C: Preimage hash mismatch
	return ErrCodeInvalidSession, ErrInvalidPreimage
}

// ConfirmOperational promotes any staged commitment to active once an operational frame is received.
func (cs *CommitmentStore) ConfirmOperational(nodeID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	rec, exists := cs.records[nodeID]
	if !exists || rec.State != StateStagedPending {
		return nil
	}

	rec.CurrentCommitment = rec.StagedCommitment
	rec.StagedCommitment = [32]byte{}
	rec.State = StateActive
	return cs.persistSyncLocked()
}

// Get retrieves a snapshot of the commitment record for a given node.
func (cs *CommitmentStore) Get(nodeID string) (CommitmentRecord, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	rec, exists := cs.records[nodeID]
	if !exists {
		return CommitmentRecord{}, false
	}
	return *rec, true
}

func (cs *CommitmentStore) persistSyncLocked() error {
	tmpPath := cs.filePath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("wire: failed to open temp commitment file: %w", err)
	}

	// 1. Write Header
	var hdr [16]byte
	copy(hdr[0:8], CommitmentFileMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], CommitmentFileVersion)
	binary.LittleEndian.PutUint32(hdr[12:16], 0) // reserved
	if _, err := f.Write(hdr[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// 2. Write Records
	for _, rec := range cs.records {
		nodeBytes := []byte(rec.NodeID)
		nodeLen := len(nodeBytes)
		if nodeLen > 65535 {
			nodeBytes = nodeBytes[:65535]
			nodeLen = 65535
		}

		payloadLen := 2 + nodeLen + 32 + 32 + 1 + 8
		buf := make([]byte, payloadLen+4)

		binary.LittleEndian.PutUint16(buf[0:2], uint16(nodeLen))
		copy(buf[2:2+nodeLen], nodeBytes)
		offset := 2 + nodeLen

		copy(buf[offset:offset+32], rec.CurrentCommitment[:])
		offset += 32
		copy(buf[offset:offset+32], rec.StagedCommitment[:])
		offset += 32

		buf[offset] = rec.State
		offset++

		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(rec.LastRotatedAt))
		offset += 8

		crc := crc32.ChecksumIEEE(buf[:payloadLen])
		binary.LittleEndian.PutUint32(buf[offset:offset+4], crc)

		if _, err := f.Write(buf); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}

	// 3. Fsync file data
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// 4. Atomic Rename
	if err := os.Rename(tmpPath, cs.filePath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// 5. Fsync directory metadata
	return syncWireDir(filepath.Dir(cs.filePath))
}

// syncWireDir flushes directory changes to durable storage.
func syncWireDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
