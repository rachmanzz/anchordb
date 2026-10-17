package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// WAL OpCodes
const (
	walMagic byte = 0xA1 // AnchorDB WAL Magic
	walVer   byte = 2
	// WAL entries are expected to be small; cap untrusted on-disk lengths before
	// allocation so a corrupt or malicious WAL cannot request multi-gigabyte RAM.
	maxWALPayloadSize = 64 << 20

	WalOpSave   byte = 1
	WalOpDelete byte = 2
	WalOpNode   byte = 3
	WalOpEdge   byte = 4
	// WalOpEdgeDelete tombstones a previously written edge.
	WalOpEdgeDelete byte = 5
	// WalOpSaveNode atomically persists an entity and its geometric node.
	WalOpSaveNode byte = 6
)

var ErrWALCorrupt = errors.New("anchordb: corrupt wal")

// SyncMode controls how often mutations are flushed to stable storage. The
// durability/throughput trade-off is explicit: SyncAlways is durable but caps
// write throughput at the fsync latency of the device, which is important on
// slow media such as SD cards.
type SyncMode int

const (
	// SyncAlways fsyncs after every WAL entry. Safest; the default.
	SyncAlways SyncMode = iota
	// SyncInterval fsyncs from a background goroutine at SyncInterval. Writes
	// are fast, but a crash can lose up to the last interval of mutations.
	SyncInterval
	// SyncNone never fsyncs explicitly; the OS flushes asynchronously. Fastest
	// and least durable.
	SyncNone
)

// WALOptions configures WAL durability behavior.
type WALOptions struct {
	SyncMode SyncMode
	// SyncInterval is used when SyncMode == SyncInterval. Defaults to 200ms.
	SyncInterval time.Duration
}

// DefaultWALOptions returns the safe default: fsync every write.
func DefaultWALOptions() WALOptions {
	return WALOptions{SyncMode: SyncAlways}
}

// WALManager manages an append-only write-ahead log for AnchorDB crash durability.
type WALManager struct {
	mu           sync.Mutex
	file         *os.File
	path         string
	corrupt      bool
	syncMode     SyncMode
	syncInterval time.Duration
	stopSync     chan struct{}
	syncWg       sync.WaitGroup
	syncErr      error
}

// OpenWAL creates or opens a WAL file at path in append mode using the safe
// default durability (fsync every write).
func OpenWAL(path string) (*WALManager, error) {
	return OpenWALWithOptions(path, DefaultWALOptions())
}

// OpenWALWithOptions creates or opens a WAL file with an explicit durability mode.
func OpenWALWithOptions(path string, opts WALOptions) (*WALManager, error) {
	// O_NOFOLLOW: never follow a symlink that a local attacker may have planted
	// at the WAL path; the WAL is a sensitive durability artifact.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal %s: %w", path, err)
	}
	// A freshly created WAL must be durable (file *and* directory entry) before
	// any frame is appended, or a crash right after creation can lose the file.
	if fi, statErr := f.Stat(); statErr == nil && fi.Size() == 0 {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to fsync new wal %s: %w", path, err)
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to fsync wal directory for %s: %w", path, err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	w := &WALManager{
		file:     f,
		path:     path,
		syncMode: opts.SyncMode,
	}
	if opts.SyncMode == SyncInterval {
		w.syncInterval = opts.SyncInterval
		if w.syncInterval <= 0 {
			w.syncInterval = 200 * time.Millisecond
		}
		w.stopSync = make(chan struct{})
		w.syncWg.Add(1)
		go w.syncLoop(w.stopSync)
	}
	return w, nil
}

// syncLoop periodically fsyncs the WAL when SyncMode == SyncInterval. The stop
// channel is passed as an argument (not read from the struct) so Close can
// detach it under the mutex without racing this goroutine.
func (w *WALManager) syncLoop(stop <-chan struct{}) {
	defer w.syncWg.Done()
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			w.mu.Lock()
			if w.file != nil && !w.corrupt {
				if err := w.file.Sync(); err != nil && w.syncErr == nil {
					w.syncErr = err
				}
			}
			w.mu.Unlock()
		}
	}
}

// Close stops any background sync goroutine, flushes, and closes the WAL file.
// It is safe to call more than once and concurrently.
func (w *WALManager) Close() error {
	// Detach the stop channel under the lock so two concurrent Close calls cannot
	// both observe it as non-nil (which would panic with "close of closed channel").
	w.mu.Lock()
	stop := w.stopSync
	w.stopSync = nil
	w.mu.Unlock()
	if stop != nil {
		close(stop)
		w.syncWg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		var syncErr error
		// Always flush on close unless syncing was explicitly disabled.
		if w.syncMode != SyncNone {
			syncErr = w.file.Sync()
		}
		closeErr := w.file.Close()
		w.file = nil
		if syncErr != nil {
			return errors.Join(fmt.Errorf("wal sync on close: %w", syncErr), closeErr)
		}
		if w.syncErr != nil {
			return errors.Join(fmt.Errorf("wal background sync failed: %w", w.syncErr), closeErr)
		}
		return closeErr
	}
	return nil
}

// SyncError returns the first error encountered by the background sync loop, if any.
func (w *WALManager) SyncError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncErr
}

// syncDir fsyncs a directory so that file creation/rename entries within it are
// committed to stable storage.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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
	if len(payload) > maxWALPayloadSize {
		return fmt.Errorf("wal payload exceeds maximum size of %d bytes", maxWALPayloadSize)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("wal file is closed")
	}
	if w.corrupt {
		return fmt.Errorf("%w: writes are disabled until the wal is repaired", ErrWALCorrupt)
	}
	start, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek wal end: %w", err)
	}
	rollback := func(cause error) error {
		if err := w.file.Truncate(start); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback partial wal entry: %w", err))
		}
		if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
			return errors.Join(cause, fmt.Errorf("seek wal after rollback: %w", err))
		}
		if err := w.file.Sync(); err != nil {
			return errors.Join(cause, fmt.Errorf("sync wal after rollback: %w", err))
		}
		return cause
	}

	checksum := crc32.ChecksumIEEE(payload)
	var hdr [11]byte
	hdr[0] = walMagic
	hdr[1] = walVer
	hdr[2] = op
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[7:11], checksum)

	if _, err := w.file.Write(hdr[:]); err != nil {
		return rollback(err)
	}
	if len(payload) > 0 {
		if _, err := w.file.Write(payload); err != nil {
			return rollback(err)
		}
	}
	// SyncAlways flushes every entry. SyncInterval/SyncNone defer flushing to
	// the background loop / OS, trading durability for throughput.
	if w.syncMode == SyncAlways {
		if err := w.file.Sync(); err != nil {
			return rollback(err)
		}
	}
	return nil
}

// WriteSave writes an entity save/update event to WAL.
// Payload: [2 byte NameLen][Name][1 byte Operation][2 byte AttrCount][(2 byte KeyLen, Key, 4 byte Value)...]
func (w *WALManager) WriteSave(name string, cr CompactRecord) error {
	buf, err := encodeWALSave(name, cr)
	if err != nil {
		return err
	}
	return w.writeEntry(WalOpSave, buf)
}

func encodeWALSave(name string, cr CompactRecord) ([]byte, error) {
	nameBytes := []byte(name)
	if len(nameBytes) > math.MaxUint16 {
		return nil, fmt.Errorf("wal record name exceeds %d bytes", math.MaxUint16)
	}
	if len(cr.Attrs) > math.MaxUint16 {
		return nil, fmt.Errorf("wal record has more than %d attributes", math.MaxUint16)
	}
	payloadLen := 2 + len(nameBytes) + 1 + 2
	for _, attr := range cr.Attrs {
		if len(attr.Key) > math.MaxUint16 {
			return nil, fmt.Errorf("wal attribute key exceeds %d bytes", math.MaxUint16)
		}
		payloadLen += 2 + len(attr.Key) + 4
		if payloadLen > maxWALPayloadSize {
			return nil, fmt.Errorf("wal payload exceeds maximum size of %d bytes", maxWALPayloadSize)
		}
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

	return buf, nil
}

// WriteDelete writes an entity deletion event to WAL.
// Payload: [2 byte NameLen][Name]
func (w *WALManager) WriteDelete(name string) error {
	nameBytes := []byte(name)
	if len(nameBytes) > math.MaxUint16 {
		return fmt.Errorf("wal record name exceeds %d bytes", math.MaxUint16)
	}
	buf := make([]byte, 2+len(nameBytes))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(nameBytes)))
	copy(buf[2:], nameBytes)

	return w.writeEntry(WalOpDelete, buf)
}

// WriteNode writes a graph node append event to WAL.
// Payload: [2 byte WordLen][Word][4 byte X][4 byte Y][16 byte Roles][2 byte DomainID]
func (w *WALManager) WriteNode(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) error {
	buf, err := encodeWALNode(word, coord, roles, domainID)
	if err != nil {
		return err
	}
	return w.writeEntry(WalOpNode, buf)
}

func encodeWALNode(word string, coord spatial.Point2D, roles [4]float32, domainID uint16) ([]byte, error) {
	wordBytes := []byte(word)
	if len(wordBytes) > math.MaxUint16 {
		return nil, fmt.Errorf("wal node word exceeds %d bytes", math.MaxUint16)
	}
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
	return buf, nil
}

// WriteSaveNode commits both parts of a save as one WAL frame. A failed frame
// is rolled back by writeEntry, so recovery cannot replay only one part.
func (w *WALManager) WriteSaveNode(name string, cr CompactRecord, coord spatial.Point2D, roles [4]float32, domainID uint16) error {
	node, err := encodeWALNode(name, coord, roles, domainID)
	if err != nil {
		return err
	}
	save, err := encodeWALSave(name, cr)
	if err != nil {
		return err
	}
	if len(node)+len(save) > maxWALPayloadSize {
		return fmt.Errorf("wal payload exceeds maximum size of %d bytes", maxWALPayloadSize)
	}
	return w.writeEntry(WalOpSaveNode, append(node, save...))
}

// BatchSaveItem holds an entity and optional node geometry for batch WAL writing.
type BatchSaveItem struct {
	Name     string
	CR       CompactRecord
	Coord    *spatial.Point2D
	Roles    *[4]float32
	DomainID uint16
}

// WriteSaveBatch writes multiple entity save events to the WAL, syncing once at the end if SyncAlways.
func (w *WALManager) WriteSaveBatch(items []BatchSaveItem) error {
	if len(items) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("wal file is not open")
	}
	if w.corrupt {
		return fmt.Errorf("cannot write to corrupt wal: %w", w.syncErr)
	}
	if w.syncErr != nil {
		return fmt.Errorf("wal background sync failed: %w", w.syncErr)
	}

	start, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek wal end: %w", err)
	}

	rollback := func(cause error) error {
		if err := w.file.Truncate(start); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback partial wal batch: %w", err))
		}
		if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
			return errors.Join(cause, fmt.Errorf("seek wal after rollback: %w", err))
		}
		if err := w.file.Sync(); err != nil {
			return errors.Join(cause, fmt.Errorf("sync wal after rollback: %w", err))
		}
		return cause
	}

	for _, it := range items {
		var op byte
		var payload []byte

		if it.Coord != nil || it.Roles != nil {
			coord := spatial.Point2D{}
			if it.Coord != nil {
				coord = *it.Coord
			}
			var roles [4]float32
			if it.Roles != nil {
				roles = *it.Roles
			}
			node, err := encodeWALNode(it.Name, coord, roles, it.DomainID)
			if err != nil {
				return rollback(err)
			}
			save, err := encodeWALSave(it.Name, it.CR)
			if err != nil {
				return rollback(err)
			}
			if len(node)+len(save) > maxWALPayloadSize {
				return rollback(fmt.Errorf("wal payload exceeds maximum size of %d bytes", maxWALPayloadSize))
			}
			op = WalOpSaveNode
			payload = append(node, save...)
		} else {
			save, err := encodeWALSave(it.Name, it.CR)
			if err != nil {
				return rollback(err)
			}
			if len(save) > maxWALPayloadSize {
				return rollback(fmt.Errorf("wal payload exceeds maximum size of %d bytes", maxWALPayloadSize))
			}
			op = WalOpSave
			payload = save
		}

		checksum := crc32.ChecksumIEEE(payload)
		var hdr [11]byte
		hdr[0] = walMagic
		hdr[1] = walVer
		hdr[2] = op
		binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[7:11], checksum)

		if _, err := w.file.Write(hdr[:]); err != nil {
			return rollback(err)
		}
		if len(payload) > 0 {
			if _, err := w.file.Write(payload); err != nil {
				return rollback(err)
			}
		}
	}

	if w.syncMode == SyncAlways {
		if err := w.file.Sync(); err != nil {
			return rollback(err)
		}
	}
	return nil
}

func decodeWALNode(payload []byte) (WALEvent, error) {
	if len(payload) < 28 {
		return WALEvent{}, fmt.Errorf("truncated node payload")
	}
	wLen := int(binary.LittleEndian.Uint16(payload[:2]))
	if 2+wLen+26 != len(payload) {
		return WALEvent{}, fmt.Errorf("invalid node payload length")
	}
	offset := 2 + wLen
	ev := WALEvent{Word: string(payload[2:offset])}
	ev.Coord.X = math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
	offset += 4
	ev.Coord.Y = math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
	offset += 4
	for i := range ev.Roles {
		ev.Roles[i] = math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		offset += 4
	}
	ev.DomainID = binary.LittleEndian.Uint16(payload[offset : offset+2])
	return ev, nil
}

// WALEvent is a fully decoded WAL operation delivered by ReplayEvents. It
// carries every field any opcode can produce so a single callback can drive
// recovery without per-opcode plumbing.
type WALEvent struct {
	Op       byte
	Name     string
	Record   CompactRecord
	Word     string
	Coord    spatial.Point2D
	Roles    [4]float32
	DomainID uint16
	Edge     EdgeRecord
}

func appendWALString(buf []byte, s string) ([]byte, error) {
	b := []byte(s)
	if len(b) > math.MaxUint16 {
		return nil, fmt.Errorf("wal string exceeds %d bytes", math.MaxUint16)
	}
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], uint16(len(b)))
	buf = append(buf, l[:]...)
	buf = append(buf, b...)
	return buf, nil
}

func readWALString(payload []byte, off int) (string, int, error) {
	if off+2 > len(payload) {
		return "", off, fmt.Errorf("truncated string length")
	}
	n := int(binary.LittleEndian.Uint16(payload[off : off+2]))
	off += 2
	if off+n > len(payload) {
		return "", off, fmt.Errorf("truncated string")
	}
	return string(payload[off : off+n]), off + n, nil
}

func decodeWALEdge(payload []byte) (EdgeRecord, error) {
	src, off, err := readWALString(payload, 0)
	if err != nil {
		return EdgeRecord{}, err
	}
	rel, off, err := readWALString(payload, off)
	if err != nil {
		return EdgeRecord{}, err
	}
	tgt, off, err := readWALString(payload, off)
	if err != nil {
		return EdgeRecord{}, err
	}
	if off+4 != len(payload) {
		return EdgeRecord{}, fmt.Errorf("edge payload length mismatch: %d trailing bytes", len(payload)-off)
	}
	weight := math.Float32frombits(binary.LittleEndian.Uint32(payload[off : off+4]))
	return EdgeRecord{Source: src, Relation: rel, Target: tgt, Weight: weight}, nil
}

func decodeWALEdgeKey(payload []byte) (EdgeRecord, error) {
	src, off, err := readWALString(payload, 0)
	if err != nil {
		return EdgeRecord{}, err
	}
	rel, off, err := readWALString(payload, off)
	if err != nil {
		return EdgeRecord{}, err
	}
	tgt, off, err := readWALString(payload, off)
	if err != nil {
		return EdgeRecord{}, err
	}
	if off != len(payload) {
		return EdgeRecord{}, fmt.Errorf("edge delete payload length mismatch: %d trailing bytes", len(payload)-off)
	}
	return EdgeRecord{Source: src, Relation: rel, Target: tgt}, nil
}

// WriteEdge writes a causal edge add/replace event to WAL.
// Payload: [2 byte SrcLen][Src][2 byte RelLen][Rel][2 byte TgtLen][Tgt][4 byte Weight]
func (w *WALManager) WriteEdge(source, relation, target string, weight float32) error {
	buf, err := appendWALString(nil, source)
	if err != nil {
		return err
	}
	if buf, err = appendWALString(buf, relation); err != nil {
		return err
	}
	if buf, err = appendWALString(buf, target); err != nil {
		return err
	}
	var wb [4]byte
	binary.LittleEndian.PutUint32(wb[:], math.Float32bits(weight))
	buf = append(buf, wb[:]...)
	return w.writeEntry(WalOpEdge, buf)
}

// WriteEdgeDelete writes a causal edge removal event to WAL.
// Payload: [2 byte SrcLen][Src][2 byte RelLen][Rel][2 byte TgtLen][Tgt]
func (w *WALManager) WriteEdgeDelete(source, relation, target string) error {
	buf, err := appendWALString(nil, source)
	if err != nil {
		return err
	}
	if buf, err = appendWALString(buf, relation); err != nil {
		return err
	}
	if buf, err = appendWALString(buf, target); err != nil {
		return err
	}
	return w.writeEntry(WalOpEdgeDelete, buf)
}

// Checkpoint atomically replaces the WAL with entries written after the snapshot.
// The old WAL remains intact unless the replacement has been fully written and
// synced, so a failed checkpoint cannot discard the only durable copy.
func (w *WALManager) Checkpoint(checkpointOffset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if w.corrupt {
		return fmt.Errorf("%w: checkpoint is disabled until the wal is repaired", ErrWALCorrupt)
	}

	fi, err := w.file.Stat()
	if err != nil {
		return err
	}
	currentSize := fi.Size()
	if checkpointOffset < 0 || checkpointOffset > currentSize {
		return fmt.Errorf("invalid wal checkpoint offset %d (size %d)", checkpointOffset, currentSize)
	}

	tmp, err := os.CreateTemp(filepath.Dir(w.path), ".anchordb_wal_checkpoint_*.tmp")
	if err != nil {
		return fmt.Errorf("create wal checkpoint file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	// Writes arrived after the snapshot; preserve that suffix without allocating
	// a second in-memory copy of the WAL.
	remSize := currentSize - checkpointOffset
	if remSize > 0 {
		if _, err := io.CopyN(tmp, io.NewSectionReader(w.file, checkpointOffset, remSize), remSize); err != nil {
			return fmt.Errorf("copy wal checkpoint suffix: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync wal checkpoint: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek replacement wal to end: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("replace wal checkpoint: %w", err)
	}
	tmpPath = "" // The temporary path is now the live WAL path.

	// Keep using the already-open descriptor, which now refers to the renamed
	// file. No fallible reopen is needed after publishing the replacement.
	old := w.file
	w.file = tmp
	tmp = nil
	if err := old.Close(); err != nil {
		return fmt.Errorf("close previous wal after checkpoint: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("open wal directory for sync: %w", err)
	} else {
		defer dir.Close()
		if err := dir.Sync(); err != nil {
			return fmt.Errorf("sync wal directory after checkpoint: %w", err)
		}
	}
	return nil
}

// ReplayEvents reads and verifies all valid WAL entries from the beginning of
// the file, delivering each decoded operation as a WALEvent. This is the
// forward-looking API used by Open; it understands edge opcodes that the legacy
// Replay callback cannot express.
func (w *WALManager) ReplayEvents(handler func(ev WALEvent) error) (count int, retErr error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if errors.Is(retErr, ErrWALCorrupt) {
			w.corrupt = true
		}
	}()
	if w.file == nil {
		return 0, fmt.Errorf("wal file is closed")
	}
	if w.corrupt {
		return 0, fmt.Errorf("%w: previous replay detected corruption", ErrWALCorrupt)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	count, _, err := w.scanWALLocked(handler, true)
	if err != nil {
		return count, err
	}

	// Seek to end for subsequent writes
	_, err = w.file.Seek(0, io.SeekEnd)
	return count, err
}

// scanWALLocked walks frames from the start of the WAL. It decodes each valid
// frame and invokes handler (which may be nil). It stops at the first incomplete
// frame (truncating it only when truncateIncomplete is true) or the first
// corrupt frame, and returns the number of decoded events plus the offset of the
// end of the last fully valid frame. Caller must hold w.mu and have positioned
// the file at the start.
func (w *WALManager) scanWALLocked(handler func(WALEvent) error, truncateIncomplete bool) (count int, lastValidOffset int64, retErr error) {
	lastValidOffset = 0
	var hdr [11]byte
	for {
		frameOffset, seekErr := w.file.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return count, lastValidOffset, seekErr
		}
		_, err := io.ReadFull(w.file, hdr[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if truncateIncomplete {
				if terr := w.truncateReplayTail(lastValidOffset); terr != nil {
					return count, lastValidOffset, fmt.Errorf("truncate incomplete frame header at %d: %w", frameOffset, terr)
				}
			}
			return count, lastValidOffset, nil
		}
		if err != nil {
			return count, lastValidOffset, fmt.Errorf("read wal header at %d: %w", frameOffset, err)
		}

		if hdr[0] != walMagic || hdr[1] == 0 || hdr[1] > walVer {
			return count, lastValidOffset, fmt.Errorf("%w: invalid frame header at offset %d", ErrWALCorrupt, frameOffset)
		}

		op := hdr[2]
		payloadLen := binary.LittleEndian.Uint32(hdr[3:7])
		expectedChecksum := binary.LittleEndian.Uint32(hdr[7:11])
		if payloadLen > maxWALPayloadSize {
			return count, lastValidOffset, fmt.Errorf("%w: frame at offset %d declares payload %d > %d", ErrWALCorrupt, frameOffset, payloadLen, maxWALPayloadSize)
		}
		// A valid frame cannot exceed the bytes remaining in the WAL. Bound the
		// allocation before converting the untrusted on-disk length to int.
		pos, seekErr := w.file.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return count, lastValidOffset, seekErr
		}
		fi, statErr := w.file.Stat()
		if statErr != nil {
			return count, lastValidOffset, statErr
		}
		if uint64(payloadLen) > uint64(fi.Size()-pos) {
			if truncateIncomplete {
				if terr := w.truncateReplayTail(lastValidOffset); terr != nil {
					return count, lastValidOffset, fmt.Errorf("truncate incomplete payload at %d: %w", frameOffset, terr)
				}
			}
			return count, lastValidOffset, nil
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if truncateIncomplete {
				if terr := w.truncateReplayTail(lastValidOffset); terr != nil {
					return count, lastValidOffset, fmt.Errorf("truncate incomplete payload at %d: %w", frameOffset, terr)
				}
			}
			return count, lastValidOffset, nil
		}

		if crc32.ChecksumIEEE(payload) != expectedChecksum {
			return count, lastValidOffset, fmt.Errorf("%w: checksum mismatch at offset %d", ErrWALCorrupt, frameOffset)
		}

		// Decode exactly one known frame. A complete but invalid frame is a hard
		// error; only an incomplete final frame is safely truncatable.
		valid := true
		switch op {
		case WalOpSave, WalOpSaveNode:
			var node WALEvent
			if op == WalOpSaveNode {
				if len(payload) < 2 {
					valid = false
				} else {
					nodeLen := 2 + int(binary.LittleEndian.Uint16(payload[:2])) + 26
					if nodeLen > len(payload) {
						valid = false
					} else {
						var decodeErr error
						node, decodeErr = decodeWALNode(payload[:nodeLen])
						valid = decodeErr == nil
						payload = payload[nodeLen:]
					}
				}
			}
			if !valid {
				break
			}
			if len(payload) < 4 {
				valid = false
			} else {
				offset := 0
				nLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
				offset += 2
				if offset+nLen > len(payload) {
					valid = false
				} else {
					name := string(payload[offset : offset+nLen])
					offset += nLen

					opStatus := uint8(1) // Default active for legacy v1 WAL frames.
					if hdr[1] >= 2 {
						if offset+1 > len(payload) {
							valid = false
						} else {
							opStatus = payload[offset]
							offset++
						}
					}
					if valid && offset+2 > len(payload) {
						valid = false
					}
					var attrs []AttrEntry
					if valid {
						attrCount := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
						offset += 2
						attrs = make([]AttrEntry, 0, attrCount)
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
							key := string(payload[offset : offset+kLen])
							offset += kLen
							value := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
							offset += 4
							attrs = append(attrs, AttrEntry{Key: key, Value: value})
						}
					}
					if valid && offset != len(payload) {
						valid = false
					}
					if valid && op == WalOpSaveNode && node.Word != name {
						valid = false
					}
					if valid && handler != nil {
						cr := CompactRecord{Operation: opStatus, Attrs: attrs}
						if err := handler(WALEvent{Op: op, Name: name, Record: cr, Word: node.Word, Coord: node.Coord, Roles: node.Roles, DomainID: node.DomainID}); err != nil {
							return count, lastValidOffset, err
						}
						count++
					}
				}
			}

		case WalOpDelete:
			if len(payload) < 2 {
				valid = false
			} else {
				nLen := int(binary.LittleEndian.Uint16(payload[0:2]))
				if 2+nLen != len(payload) {
					valid = false
				} else if handler != nil {
					name := string(payload[2:])
					if err := handler(WALEvent{Op: op, Name: name}); err != nil {
						return count, lastValidOffset, err
					}
					count++
				}
			}

		case WalOpNode:
			node, err := decodeWALNode(payload)
			if err != nil {
				valid = false
			} else if handler != nil {
				node.Op = op
				if err := handler(node); err != nil {
					return count, lastValidOffset, err
				}
				count++
			}
		case WalOpEdge:
			edge, err := decodeWALEdge(payload)
			if err != nil {
				valid = false
			} else if handler != nil {
				if err := handler(WALEvent{Op: op, Edge: edge}); err != nil {
					return count, lastValidOffset, err
				}
				count++
			}
		case WalOpEdgeDelete:
			edge, err := decodeWALEdgeKey(payload)
			if err != nil {
				valid = false
			} else if handler != nil {
				if err := handler(WALEvent{Op: op, Edge: edge}); err != nil {
					return count, lastValidOffset, err
				}
				count++
			}
		default:
			valid = false
		}
		if !valid {
			return count, lastValidOffset, fmt.Errorf("%w: invalid opcode %d or payload at offset %d", ErrWALCorrupt, op, frameOffset)
		}
		// Only a completely decoded, checksummed frame advances the durable end.
		lastValidOffset, err = w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return count, lastValidOffset, err
		}
	}

	return count, lastValidOffset, nil
}

// WALRepairResult describes what RepairWALFile changed.
type WALRepairResult struct {
	// Path is the repaired WAL file.
	Path string
	// ValidBytes is the length of the intact prefix that was kept.
	ValidBytes int64
	// DiscardedBytes is the number of bytes removed from the tail (0 if healthy).
	DiscardedBytes int64
	// BackupPath is where the discarded tail was copied for forensics, if any.
	BackupPath string
	// Repaired reports whether the file was modified.
	Repaired bool
}

// RepairWALFile scans a WAL file and truncates it at the end of the last fully
// valid frame when a corrupt or incomplete frame is found, first copying the
// discarded suffix to a "<path>.corrupt-<timestamp>" backup. A healthy WAL is
// left untouched. Use this to recover a database whose Open previously failed
// with ErrWALCorrupt, then retry Open.
func RepairWALFile(path string) (*WALRepairResult, error) {
	w, err := OpenWALWithOptions(path, WALOptions{SyncMode: SyncNone})
	if err != nil {
		return nil, err
	}
	defer w.Close()
	return w.Repair()
}

// Repair performs the scan/truncate described by RepairWALFile on an open WAL.
func (w *WALManager) Repair() (*WALRepairResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, fmt.Errorf("wal file is closed")
	}
	fi, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	_, validEnd, scanErr := w.scanWALLocked(nil, false)

	res := &WALRepairResult{Path: w.path, ValidBytes: validEnd}
	if validEnd >= size {
		// Nothing recoverable was lost; the file is healthy (or empty).
		w.corrupt = false
		if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}
		return res, scanErr
	}

	// Preserve the bad suffix for forensics before discarding it.
	backup := fmt.Sprintf("%s.corrupt-%s", w.path, time.Now().UTC().Format("20060102T150405"))
	if err := copyFileRange(w.file, backup, validEnd, size-validEnd); err != nil {
		return nil, fmt.Errorf("wal repair: backup corrupt tail: %w", err)
	}
	res.BackupPath = backup
	res.DiscardedBytes = size - validEnd

	if err := w.file.Truncate(validEnd); err != nil {
		return nil, fmt.Errorf("wal repair: truncate wal: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return nil, fmt.Errorf("wal repair: sync wal: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	w.corrupt = false
	res.Repaired = true
	return res, nil
}

// copyFileRange copies [off, off+n) of src into a new file at dstPath.
func copyFileRange(src *os.File, dstPath string, off, n int64) error {
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, io.NewSectionReader(src, off, n)); err != nil {
		_ = dst.Close()
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dstPath))
}

// Replay is the legacy recovery callback. It keeps its original signature for
// backward compatibility and simply ignores edge opcodes, which the legacy
// callback shape cannot express. Combined saves are delivered as node then
// save callbacks. New code should use ReplayEvents.
func (w *WALManager) Replay(handler func(op byte, name string, cr CompactRecord, word string, coord spatial.Point2D, roles [4]float32, domainID uint16) error) (int, error) {
	return w.ReplayEvents(func(ev WALEvent) error {
		if ev.Op == WalOpEdge || ev.Op == WalOpEdgeDelete {
			return nil
		}
		if handler == nil {
			return nil
		}
		if ev.Op == WalOpSaveNode {
			if err := handler(WalOpNode, "", CompactRecord{}, ev.Word, ev.Coord, ev.Roles, ev.DomainID); err != nil {
				return err
			}
			return handler(WalOpSave, ev.Name, ev.Record, "", spatial.Point2D{}, [4]float32{}, 0)
		}
		return handler(ev.Op, ev.Name, ev.Record, ev.Word, ev.Coord, ev.Roles, ev.DomainID)
	})
}

func (w *WALManager) truncateReplayTail(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}
