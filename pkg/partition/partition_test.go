package partition

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestPartition_Archive6Months_AndPrune(t *testing.T) {
	tmpDir := t.TempDir()
	hotDBPath := filepath.Join(tmpDir, "logs.anchordb")
	partDir := filepath.Join(tmpDir, "partitions")

	db, err := storage.Open(hotDBPath)
	if err != nil {
		t.Fatalf("failed to open hot db: %v", err)
	}
	defer db.Close()

	// Hitung tanggal:
	// Hari ini: 2026-10-15
	// 8 bulan lalu: ~240 hari lalu (harus di-archive)
	// 7 bulan lalu: ~210 hari lalu (harus di-archive)
	// 2 bulan lalu: ~60 hari lalu (harus TETAP di hot DB)
	// Hari ini: 0 hari (harus TETAP di hot DB)
	nowDays := float32(time.Now().Unix() / 86400)

	_ = db.SaveRecord("log_8_bulan_lalu", map[string]float32{
		"tanggal": nowDays - 240,
		"status":  500,
	})
	_ = db.SaveRecord("log_7_bulan_lalu", map[string]float32{
		"tanggal": nowDays - 210,
		"status":  502,
	})
	_ = db.SaveRecord("log_2_bulan_lalu", map[string]float32{
		"tanggal": nowDays - 60,
		"status":  200,
	})
	_ = db.SaveRecord("log_hari_ini", map[string]float32{
		"tanggal": nowDays,
		"status":  200,
	})

	_ = db.Sync()

	if len(db.EntityStore) != 4 {
		t.Fatalf("expected 4 records initially, got %d", len(db.EntityStore))
	}

	// Buat Sink & Policy: simpan data > 6 bulan (180 hari) ke partition file format {date}_{uuid}.anchordb
	sink := NewLocalDiskSink(partDir)
	policy := PartitionPolicy{
		ModelName:     "logs",
		DateField:     "tanggal",
		RetentionDays: 180, // 6 bulan terakhir
		Sink:          sink,
		FileFormat:    "{date}_{uuid}.anchordb",
		PruneHotDB:    true, // bersihkan dari hot db
	}

	// Eksekusi partisi
	result, err := ExecutePartition(db, policy)
	if err != nil {
		t.Fatalf("ExecutePartition failed: %v", err)
	}

	if result.ArchivedCount != 2 {
		t.Fatalf("expected 2 historical records archived, got %d", result.ArchivedCount)
	}

	t.Logf("[OK] Partisi Berhasil: %d record diarsipkan ke: %s", result.ArchivedCount, result.TargetLocation)

	// Verifikasi format nama berkas: YYYY-MM-DD_<uuid>.anchordb
	filename := filepath.Base(result.TargetLocation)
	if !strings.HasSuffix(filename, ".anchordb") || len(filename) < 20 {
		t.Fatalf("invalid partition filename format: %s", filename)
	}
	t.Logf("[OK] Format Berkas Partisi Terverifikasi: %s", filename)

	// Verifikasi Hot DB: 2 data lama terhapus, 2 data baru tetap utuh!
	if len(db.EntityStore) != 2 {
		t.Fatalf("expected 2 records remaining in hot DB, got %d", len(db.EntityStore))
	}
	if _, exists := db.EntityStore["log_hari_ini"]; !exists {
		t.Fatalf("expected log_hari_ini still in hot DB")
	}
	if _, exists := db.EntityStore["log_2_bulan_lalu"]; !exists {
		t.Fatalf("expected log_2_bulan_lalu still in hot DB")
	}
	if _, exists := db.EntityStore["log_8_bulan_lalu"]; exists {
		t.Fatalf("expected log_8_bulan_lalu pruned from hot DB")
	}

	// Verifikasi Berkas Partisi: buka secara mandiri dan pastikan data arsip utuh
	archivedDB, err := storage.Open(result.TargetLocation)
	if err != nil {
		t.Fatalf("failed to open archived partition file: %v", err)
	}
	defer archivedDB.Close()

	if len(archivedDB.EntityStore) != 2 {
		t.Fatalf("expected 2 records inside archived partition, got %d", len(archivedDB.EntityStore))
	}
	t.Logf("[OK] Berkas Arsip Partisi Mandiri Berhasil Diverifikasi!")
}

func TestScheduler_CalculateNextRun_2300(t *testing.T) {
	// Uji jadwal "23:00"
	now := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC) // Jam 10 pagi
	next, err := scheduler.CalculateNextRun("23:00", now)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	if next.Hour() != 23 || next.Minute() != 0 || next.Day() != 15 {
		t.Fatalf("expected today 23:00, got %v", next)
	}

	// Jika sudah lewat jam 23:00 hari ini (misal jam 23:30), harus jadwalkan besok jam 23:00
	nowPast := time.Date(2026, 10, 15, 23, 30, 0, 0, time.UTC)
	nextTomorrow, err := scheduler.CalculateNextRun("23:00", nowPast)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	if nextTomorrow.Day() != 16 || nextTomorrow.Hour() != 23 {
		t.Fatalf("expected tomorrow 23:00, got %v", nextTomorrow)
	}
	t.Logf("[OK] Jadwal Internal '23:00' Terverifikasi Akurat!")
}

func TestPartition_S3Sink_FailSafe(t *testing.T) {
	tmpDir := t.TempDir()
	hotDBPath := filepath.Join(tmpDir, "hot_logs.anchordb")

	db, err := storage.Open(hotDBPath)
	if err != nil {
		t.Fatalf("failed to open hot db: %v", err)
	}
	defer db.Close()

	nowDays := float32(time.Now().Unix() / 86400)
	_ = db.SaveRecord("old_record_1", map[string]float32{"tanggal": nowDays - 300})
	_ = db.SaveRecord("old_record_2", map[string]float32{"tanggal": nowDays - 250})
	_ = db.Sync()

	// S3 Sink pointing to unreachable address so upload fails
	s3Sink := NewS3Sink("http://127.0.0.1:54321", "non-existent-bucket", "prefix", "key", "secret")

	// Execute partition with PruneHotDB: true
	_, err = ExecutePartition(db, PartitionPolicy{
		ModelName:     "hot_logs",
		DateField:     "tanggal",
		RetentionDays: 180,
		Sink:          s3Sink,
		PruneHotDB:    true,
	})

	if err == nil {
		t.Fatalf("expected partition to fail on unreachable S3, got nil")
	}

	// CRITICAL VERIFICATION: Hot DB must NOT be pruned when sink fails!
	if len(db.EntityStore) != 2 {
		t.Fatalf("CRITICAL FAIL-SAFE VIOLATION: Hot DB was pruned despite S3 upload failure! Remaining records: %d", len(db.EntityStore))
	}
	t.Logf("[OK] Fail-safe verified! Hot DB preserved (count=%d) when S3 upload fails.", len(db.EntityStore))
}
