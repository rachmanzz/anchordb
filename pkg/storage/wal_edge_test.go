package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriteRejectsPayloadExceedingLimit(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "oversized.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	// A dummy slice declaring length > maxWALPayloadSize without allocating 65MB in RAM
	// writeEntry checks len(payload) > maxWALPayloadSize before locking or writing.
	oversizedPayload := make([]byte, maxWALPayloadSize+1)
	err = wal.writeEntry(WalOpSave, oversizedPayload)
	if err == nil {
		t.Fatal("expected writeEntry to reject payload exceeding maxWALPayloadSize, got nil")
	}
}

func TestWALReplayRejectsOversizedPayloadLenInHeader(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "corrupt_payload_len.anchordb")
	walPath := dbPath + ".wal"

	// Create a dummy base db file so Open attempts to replay the WAL
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Write a crafted frame header declaring a 65 MB payload
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	var hdr [11]byte
	hdr[0] = walMagic
	hdr[1] = walVer
	hdr[2] = WalOpSave
	// Declare 65 MB (maxWALPayloadSize is 64 MB)
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(maxWALPayloadSize+1024))
	binary.LittleEndian.PutUint32(hdr[7:11], 0x12345678)

	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		t.Fatalf("Write header failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopening must detect corrupt declared size and fail closed with ErrWALCorrupt
	_, err = Open(dbPath)
	if err == nil {
		t.Fatal("expected Open to fail on WAL frame declaring payload > maxWALPayloadSize, got nil")
	}
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("expected ErrWALCorrupt, got: %v", err)
	}
}

func TestWALReplayRejectsInvalidMagicAndVersion(t *testing.T) {
	tests := []struct {
		name    string
		magic   byte
		version byte
	}{
		{"Invalid magic", 0xFF, walVer},
		{"Version zero", walMagic, 0},
		{"Future unsupported version", walMagic, walVer + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "bad_hdr.anchordb")
			walPath := dbPath + ".wal"

			db, err := Open(dbPath)
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			_ = db.Close()

			var hdr [11]byte
			hdr[0] = tt.magic
			hdr[1] = tt.version
			hdr[2] = WalOpSave
			binary.LittleEndian.PutUint32(hdr[3:7], 10)
			binary.LittleEndian.PutUint32(hdr[7:11], 0)

			if err := os.WriteFile(walPath, hdr[:], 0600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			_, err = Open(dbPath)
			if err == nil {
				t.Fatal("expected ErrWALCorrupt, got nil")
			}
			if !errors.Is(err, ErrWALCorrupt) {
				t.Fatalf("expected ErrWALCorrupt, got: %v", err)
			}
		})
	}
}
