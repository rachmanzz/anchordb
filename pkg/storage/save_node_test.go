package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestSaveRecordWithNodeRejectsSchemaWithoutOrphan(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "model.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.AttachSchema(ModelSchema{"required": "float"}); err != nil {
		t.Fatal(err)
	}
	_, err = db.SaveRecordWithNode("orphan", OpStatusActive, map[string]float32{"unknown": 1}, spatial.Point2D{X: 0.2, Y: 0.3}, [4]float32{})
	if !errors.Is(err, ErrMissingRequiredField) && !errors.Is(err, ErrUnknownField) {
		t.Fatalf("expected schema rejection, got %v", err)
	}
	if _, exists := db.Lexicon["orphan"]; exists {
		t.Fatal("rejected save left a graph node")
	}
	if _, exists := db.EntityStore["orphan"]; exists {
		t.Fatal("rejected save left an entity")
	}
	count, err := db.wal.ReplayEvents(func(ev WALEvent) error { return nil })
	if err != nil || count != 0 {
		t.Fatalf("rejected save wrote WAL events: count=%d err=%v", count, err)
	}
}

func TestSaveRecordWithNodeReplaysAsOneWALEvent(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "model.anchordb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coord := spatial.Point2D{X: 0.2, Y: -0.3}
	roles := [4]float32{1, 2, 3, 4}
	name, err := db.SaveRecordWithNode("  placed  ", OpStatusActive, map[string]float32{"score": 7}, coord, roles)
	if err != nil || name != "placed" {
		t.Fatalf("SaveRecordWithNode: name=%q err=%v", name, err)
	}
	var events []WALEvent
	count, err := db.wal.ReplayEvents(func(ev WALEvent) error {
		events = append(events, ev)
		return nil
	})
	if err != nil || count != 1 || len(events) != 1 {
		t.Fatalf("WAL replay: count=%d events=%d err=%v", count, len(events), err)
	}
	ev := events[0]
	if ev.Op != WalOpSaveNode || ev.Name != name || ev.Word != name || ev.Coord != coord || ev.Roles != roles || len(ev.Record.Attrs) != 1 || ev.Record.Attrs[0].Value != 7 {
		t.Fatalf("combined WAL event lost data: %+v", ev)
	}
}

func TestOpenRecoversCombinedSaveNodeFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model.anchordb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	coord := spatial.Point2D{X: 0.4, Y: -0.2}
	roles := [4]float32{1, 0, 0, 0}
	record := CompactRecord{Operation: OpStatusActive, Attrs: []AttrEntry{{Key: "score", Value: 9}}}
	if err := wal.WriteSaveNode("placed", record, coord, roles, 1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	idx, exists := recovered.Lexicon["placed"]
	if !exists || recovered.getNode(idx).X != coord.X || recovered.getNode(idx).Y != coord.Y {
		t.Fatalf("recovered node missing or misplaced: exists=%v", exists)
	}
	props, err := recovered.GetRecord("placed")
	if err != nil || props["score"] != 9 {
		t.Fatalf("recovered entity: props=%v err=%v", props, err)
	}
}
