package wire

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitmentStore_LifecycleAndReopen(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "commitments.bin")

	store, err := OpenCommitmentStore(storePath)
	if err != nil {
		t.Fatalf("failed to open commitment store: %v", err)
	}

	// 1. Initial Enrollment
	v1 := [32]byte{0x01, 0x02, 0x03, 0x04}
	e1 := sha256.Sum256(v1[:])
	nodeID := "connector-alpha"

	if err := store.RegisterInitial(nodeID, e1); err != nil {
		t.Fatalf("RegisterInitial failed: %v", err)
	}

	rec, ok := store.Get(nodeID)
	if !ok {
		t.Fatalf("expected node %s to exist", nodeID)
	}
	if rec.CurrentCommitment != e1 {
		t.Fatalf("commitment mismatch: got %x, want %x", rec.CurrentCommitment, e1)
	}
	if rec.State != StateActive {
		t.Fatalf("expected StateActive, got %d", rec.State)
	}

	// 2. Reopen store from disk and verify persistence
	store2, err := OpenCommitmentStore(storePath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	rec2, ok2 := store2.Get(nodeID)
	if !ok2 || rec2.CurrentCommitment != e1 {
		t.Fatalf("reopened store failed to retrieve persisted commitment: ok=%v, rec=%+v", ok2, rec2)
	}

	// 3. Stage Reauth (Case A: normal rotation with V1, registering E2)
	v2 := [32]byte{0x0A, 0x0B, 0x0C, 0x0D}
	e2 := sha256.Sum256(v2[:])

	code, err := store2.StageReauth(nodeID, v1, e2)
	if err != nil || code != 0 {
		t.Fatalf("StageReauth failed: code=%d, err=%v", code, err)
	}

	recStaged, _ := store2.Get(nodeID)
	if recStaged.State != StateStagedPending {
		t.Fatalf("expected StateStagedPending, got %d", recStaged.State)
	}
	if recStaged.StagedCommitment != e2 {
		t.Fatalf("staged commitment mismatch: got %x, want %x", recStaged.StagedCommitment, e2)
	}
	if recStaged.CurrentCommitment != e1 {
		t.Fatalf("current commitment must remain intact: got %x, want %x", recStaged.CurrentCommitment, e1)
	}

	// 4. Case B: Reconnect retry using V2 before confirmation (e.g. dropped connection)
	v3 := [32]byte{0x11, 0x22, 0x33, 0x44}
	e3 := sha256.Sum256(v3[:])

	codeRetry, errRetry := store2.StageReauth(nodeID, v2, e3)
	if errRetry != nil || codeRetry != 0 {
		t.Fatalf("StageReauth Case B retry failed: code=%d, err=%v", codeRetry, errRetry)
	}

	recPromoted, _ := store2.Get(nodeID)
	if recPromoted.CurrentCommitment != e2 {
		t.Fatalf("expected CurrentCommitment promoted to e2: got %x, want %x", recPromoted.CurrentCommitment, e2)
	}
	if recPromoted.StagedCommitment != e3 {
		t.Fatalf("expected StagedCommitment set to e3: got %x, want %x", recPromoted.StagedCommitment, e3)
	}

	// 5. ConfirmOperational (Piggyback Pruning)
	if err := store2.ConfirmOperational(nodeID); err != nil {
		t.Fatalf("ConfirmOperational failed: %v", err)
	}

	recConfirmed, _ := store2.Get(nodeID)
	if recConfirmed.State != StateActive {
		t.Fatalf("expected StateActive after confirmation, got %d", recConfirmed.State)
	}
	if recConfirmed.CurrentCommitment != e3 {
		t.Fatalf("expected CurrentCommitment to be e3: got %x", recConfirmed.CurrentCommitment)
	}
	if recConfirmed.StagedCommitment != [32]byte{} {
		t.Fatalf("expected StagedCommitment to be cleared: got %x", recConfirmed.StagedCommitment)
	}

	// 6. Case C: Invalid preimage hash rejected
	badValidator := [32]byte{0xFF, 0xEE}
	codeBad, errBad := store2.StageReauth(nodeID, badValidator, [32]byte{})
	if errBad != ErrInvalidPreimage || codeBad != ErrCodeInvalidSession {
		t.Fatalf("expected ErrInvalidPreimage and ErrCodeInvalidSession, got code=%d, err=%v", codeBad, errBad)
	}

	// 7. Unknown NodeID rejected
	codeUnknown, errUnknown := store2.StageReauth("unknown-node", v3, [32]byte{})
	if errUnknown != ErrUnknownNodeID || codeUnknown != ErrCodeUnauthorized {
		t.Fatalf("expected ErrUnknownNodeID, got code=%d, err=%v", codeUnknown, errUnknown)
	}
}

func TestCommitmentStore_CorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	badPath := filepath.Join(tmpDir, "corrupt.bin")

	// 1. Invalid magic
	if err := os.WriteFile(badPath, []byte("NOTMAGIC12345678"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCommitmentStore(badPath); err == nil {
		t.Fatal("expected error opening file with invalid magic")
	}

	// 2. Truncated record header (1 byte instead of 2-byte uint16 length)
	validHeader := []byte("ANCHCOMM\x01\x00\x00\x00\x00\x00\x00\x00\x01")
	if err := os.WriteFile(badPath, validHeader, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCommitmentStore(badPath); err == nil {
		t.Fatal("expected error opening file with truncated record header")
	}
}
