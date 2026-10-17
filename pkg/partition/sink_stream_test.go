package partition

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// LocalDiskSink must be able to publish a large archive from a source file
// without the caller buffering it in memory, preserving content and 0600 perms.
func TestLocalDiskSinkStreamsFromFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "part.anchordb")
	payload := bytes.Repeat([]byte("anchordb-stream-"), 200000) // ~3 MiB
	if err := os.WriteFile(src, payload, 0600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	sink := NewLocalDiskSink(filepath.Join(dir, "nested", "out"))
	target, err := sink.SavePartitionFromFile("2026-10-16_part.anchordb", src)
	if err != nil {
		t.Fatalf("SavePartitionFromFile: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read partition: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat partition: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0600 {
		t.Fatalf("partition perms = %o, want 600", perm)
	}
}

// S3Sink must stream the source file with a known Content-Length and an
// unaltered body.
func TestS3SinkStreamsFromFile(t *testing.T) {
	var gotBody []byte
	var gotLength int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLength = r.ContentLength
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dir := t.TempDir()
	src := filepath.Join(dir, "obj.anchordb")
	payload := bytes.Repeat([]byte{0xAB}, 1<<20) // 1 MiB
	if err := os.WriteFile(src, payload, 0600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	// No credentials -> no signing -> plaintext http is permitted.
	sink := NewS3Sink(server.URL, "bucket", "prefix", "", "")
	loc, err := sink.SavePartitionFromFile("obj.anchordb", src)
	if err != nil {
		t.Fatalf("SavePartitionFromFile: %v", err)
	}
	if !strings.HasPrefix(loc, "s3://bucket/") {
		t.Fatalf("unexpected target location %q", loc)
	}
	if gotLength != int64(len(payload)) {
		t.Fatalf("Content-Length = %d, want %d", gotLength, len(payload))
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("body mismatch: got %d bytes, want %d", len(gotBody), len(payload))
	}
}
