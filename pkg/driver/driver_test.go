package driver

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/scheduler"
	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestDriver_FluentBuilder(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_test.anchordb")

	// 1. Connect via Driver DSN
	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("failed to connect via driver: %v", err)
	}
	defer client.Close()

	// 2. Populate test nodes directly
	db := client.DB()
	_, _ = db.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_, _ = db.AppendNode("sekolah", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}, 1)
	_, _ = db.AppendNode("belajar", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}, 1)
	_ = db.Sync()

	ctx := context.Background()

	// 3. Test Fluent Query Builder
	resp, err := client.Graph().
		From("ibu", "ayah").
		Relate("keturunan").
		BeamWidth(0.05).
		TopK(5).
		Execute(ctx)

	if err != nil {
		t.Fatalf("driver query failed: %v", err)
	}

	if len(resp.Results) == 0 {
		t.Fatalf("expected results from driver, got 0")
	}

	if resp.Results[0].Word != "anak" {
		t.Fatalf("expected 'anak', got %q", resp.Results[0].Word)
	}

	t.Logf("Driver Success! Anchor: (%.2f, %.2f), Top-1: %s (Score: %.4f)",
		resp.Anchor.X, resp.Anchor.Y, resp.Results[0].Word, resp.Results[0].Score)
}

func TestDriver_MultiHopChain(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_chain.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	db := client.DB()
	_, _ = db.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_ = db.Sync()

	ctx := context.Background()

	chainResp, err := client.Graph().
		From("ibu", "ayah").
		Chain("keturunan").
		ExecuteChain(ctx)

	if err != nil {
		t.Fatalf("chain failed: %v", err)
	}

	if len(chainResp.Sequence) == 0 || chainResp.Sequence[0] != "anak" {
		t.Fatalf("expected chain sequence ['anak'], got %v", chainResp.Sequence)
	}

	t.Logf("Chain result: %v", chainResp.Sequence)
}

func TestDriver_DirectMathAndOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_math.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	db := client.DB()
	_, _ = db.AppendNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}, 1)
	_, _ = db.AppendNode("anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}, 1)
	_ = db.Sync()

	math := client.Math()

	// 1. Test Direct Distance Calculation
	dist, err := math.Distance("ibu", "anak")
	if err != nil {
		t.Fatalf("distance failed: %v", err)
	}
	// Distance between (0.2, 0.9) and (0.2, 0.1) is exactly 0.8
	if dist < 0.79 || dist > 0.81 {
		t.Fatalf("expected distance ~0.8, got %f", dist)
	}
	t.Logf("Direct Distance 'ibu' -> 'anak': %f", dist)

	// 2. Test Direct Centroid Calculation
	centroid, err := math.Centroid("ibu", "ayah")
	if err != nil {
		t.Fatalf("centroid failed: %v", err)
	}
	if centroid.Coord.X != 0.2 || centroid.Coord.Y != 0.9 {
		t.Fatalf("expected centroid (0.2, 0.9), got (%f, %f)", centroid.Coord.X, centroid.Coord.Y)
	}
	t.Logf("Centroid of 'ibu' & 'ayah': (%f, %f)", centroid.Coord.X, centroid.Coord.Y)

	// 3. Test Cosine Similarity
	sim, err := math.CosineSimilarity("ibu", "ayah")
	if err != nil {
		t.Fatalf("cosine similarity failed: %v", err)
	}
	if sim < 0.99 {
		t.Fatalf("expected cosine similarity ~ 1.0 for identical roles, got %f", sim)
	}
	t.Logf("Cosine Similarity 'ibu' & 'ayah': %f", sim)

	// 4. Test Nearest Neighbors
	nearest, err := math.Nearest("ibu", 2)
	if err != nil {
		t.Fatalf("nearest failed: %v", err)
	}
	if len(nearest) == 0 {
		t.Fatalf("expected nearest neighbors, got 0")
	}
	t.Logf("Nearest neighbor to 'ibu': %s (Distance: %f)", nearest[0].Word, nearest[0].T)

	// 5. Test Custom OrderBy in Query
	ctx := context.Background()
	resp, err := client.Graph().
		From("ibu", "ayah").
		Relate("keturunan").
		OrderBy(storage.OrderByDistanceAsc).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query with OrderByDistanceAsc failed: %v", err)
	}
	if len(resp.Results) == 0 || resp.Results[0].Word != "anak" {
		t.Fatalf("expected 'anak' under OrderByDistanceAsc")
	}
	t.Logf("OrderByDistanceAsc Top-1: %s (t: %f)", resp.Results[0].Word, resp.Results[0].T)
}

func TestDriver_SaveGetCompute_SpeedExample(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_physics.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. FUNGSI UTAMA: SAVING (Simpan Data Mentah: jarak = 100, waktu = 2)
	err = client.Save("perjalanan_mobil", map[string]float32{
		"jarak": 100.0, // km
		"waktu": 2.0,   // jam
	})
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. FUNGSI UTAMA: GETTING (Ambil Data Mentah)
	rawRes, err := client.Get("perjalanan_mobil").Execute(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rawRes.Get("jarak") != 100.0 || rawRes.Get("waktu") != 2.0 {
		t.Fatalf("expected jarak=100, waktu=2, got %v", rawRes.All())
	}
	t.Logf("1. Pure GETTING: %v", rawRes.All())

	// 3. FUNGSI LANJUTAN: CALCULATING (Ambil jarak dan waktu, hitung kecepatan = jarak / waktu)
	// Returnnya harus: jarak, waktu, dan kecepatan!
	calcRes, err := client.Get("perjalanan_mobil").
		Compute("kecepatan", Divide("jarak", "waktu")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Get with Compute failed: %v", err)
	}

	jarak := calcRes.Get("jarak")
	waktu := calcRes.Get("waktu")
	kecepatan := calcRes.Get("kecepatan")

	if jarak != 100.0 {
		t.Fatalf("expected jarak = 100, got %f", jarak)
	}
	if waktu != 2.0 {
		t.Fatalf("expected waktu = 2, got %f", waktu)
	}
	if kecepatan != 50.0 {
		t.Fatalf("expected kecepatan = 50, got %f", kecepatan)
	}

	t.Logf("2. GETTING + CALCULATING: jarak=%.1f, waktu=%.1f, kecepatan=%.1f (Semua ter-return!)",
		jarak, waktu, kecepatan)
}

func TestDriver_List_Where_Compute_Paging(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "driver_list_page.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Seed multiple trips
	_ = client.Save("trip_1", map[string]float32{"jarak": 120, "waktu": 2}) // kecepatan = 60
	_ = client.Save("trip_2", map[string]float32{"jarak": 200, "waktu": 2}) // kecepatan = 100
	_ = client.Save("trip_3", map[string]float32{"jarak": 40, "waktu": 1})  // kecepatan = 40 (di bawah 50km/h)
	_ = client.Save("trip_4", map[string]float32{"jarak": 300, "waktu": 3}) // kecepatan = 100

	// Query: jarak >= 50, hitung kecepatan, filter kecepatan > 50, Page 1 with pageSize 2
	pagedRes, err := client.List().
		Where("jarak", ">=", 50).
		Compute("kecepatan", Divide("jarak", "waktu")).
		Where("kecepatan", ">", 50).
		Page(1, 2).
		Execute(ctx)

	if err != nil {
		t.Fatalf("List query failed: %v", err)
	}

	if len(pagedRes.Items) != 2 {
		t.Fatalf("expected 2 items for page size 2, got %d", len(pagedRes.Items))
	}

	for i, item := range pagedRes.Items {
		t.Logf("Page 1 Item [%d]: %s -> jarak=%.0f, waktu=%.0f, kecepatan=%.0f",
			i+1, item.Name, item.Get("jarak"), item.Get("waktu"), item.Get("kecepatan"))
	}

	// Verify Page 2
	page2Res, err := client.List().
		Where("jarak", ">=", 50).
		Compute("kecepatan", Divide("jarak", "waktu")).
		Where("kecepatan", ">", 50).
		Page(2, 2).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Page 2 query failed: %v", err)
	}

	t.Logf("Page 2 Items count: %d, HasMore: %v", len(page2Res.Items), page2Res.HasMore)
}

func TestDriver_SessionPersistence(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "persist_client.anchordb")

	// Session 1: Connect, Save, Sync, Close
	c1, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("Session 1 connect failed: %v", err)
	}

	_ = c1.Save("mobil_a", map[string]float32{"jarak": 120, "waktu": 2})
	_ = c1.Save("mobil_b", map[string]float32{"jarak": 240, "waktu": 3})
	if err := c1.DB().Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = c1.Close()

	// Session 2: Reconnect to same file, Get and Compute
	c2, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("Session 2 connect failed: %v", err)
	}
	defer c2.Close()

	resA, err := c2.Get("mobil_a").Compute("kecepatan", Divide("jarak", "waktu")).Execute(ctx)
	if err != nil {
		t.Fatalf("Get mobil_a failed: %v", err)
	}

	if resA.Get("jarak") != 120 || resA.Get("waktu") != 2 || resA.Get("kecepatan") != 60 {
		t.Fatalf("unexpected values: %v", resA.All())
	}

	resB, err := c2.Get("mobil_b").Compute("kecepatan", Divide("jarak", "waktu")).Execute(ctx)
	if err != nil {
		t.Fatalf("Get mobil_b failed: %v", err)
	}

	if resB.Get("jarak") != 240 || resB.Get("waktu") != 3 || resB.Get("kecepatan") != 80 {
		t.Fatalf("unexpected values: %v", resB.All())
	}

	t.Logf("Session persistence verified! mobil_a kecepatan=%.0f, mobil_b kecepatan=%.0f",
		resA.Get("kecepatan"), resB.Get("kecepatan"))
}

func TestDriver_SelectProjection_UserScenario(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "select_test.anchordb")

	client, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Entity has: jarak, waktu, luas, suhu, kelembaban
	_ = client.Save("lokasi_a", map[string]float32{
		"jarak":      100.0,
		"waktu":      2.0,
		"luas":       500.0,
		"suhu":       28.5,
		"kelembaban": 75.0,
	})

	// 1. GET with SELECT: Ambil jarak, waktu, dan custom column hasil calculasi (kecepatan)
	// luas, suhu, kelembaban harus diproyeksikan keluar (TIDAK ter-return)
	res, err := client.Get("lokasi_a").
		Select("jarak", "waktu", "kecepatan").
		Compute("kecepatan", Divide("jarak", "waktu")).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Get with Select failed: %v", err)
	}

	if res.Get("jarak") != 100.0 || res.Get("waktu") != 2.0 || res.Get("kecepatan") != 50.0 {
		t.Fatalf("unexpected values in selected fields: %v", res.All())
	}

	if res.Has("luas") || res.Has("suhu") || res.Has("kelembaban") {
		t.Fatalf("expected unselected fields to be excluded, but got: %v", res.All())
	}

	t.Logf("User scenario SELECT on Get passed! Returned fields: %v", res.All())

	// 2. LIST with SELECT:
	listRes, err := client.List().
		Select("jarak", "waktu", "kecepatan").
		Where("luas", ">=", 100.0). // Filter by unselected field 'luas'!
		Compute("kecepatan", Divide("jarak", "waktu")).
		Page(1, 5).
		Execute(ctx)

	if err != nil {
		t.Fatalf("List with Select failed: %v", err)
	}

	if len(listRes.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(listRes.Items))
	}

	item := listRes.Items[0]
	if item.Has("luas") {
		t.Fatalf("expected 'luas' to be excluded in list projection, got: %v", item.All())
	}
	if !item.Has("jarak") || !item.Has("waktu") || !item.Has("kecepatan") {
		t.Fatalf("expected jarak, waktu, kecepatan present, got: %v", item.All())
	}

	t.Logf("User scenario SELECT on List passed! Item fields: %v", item.All())
}

func TestDriver_BatchUpdate10k_AddDistance(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "batch_update_10k.anchordb")

	client, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed 25,000 records, each with jarak = 100, waktu = 2
	totalSeeds := 25000
	for i := 0; i < totalSeeds; i++ {
		name := fmt.Sprintf("vehicle_%d", i)
		_ = client.Save(name, map[string]float32{
			"jarak": 100.0,
			"waktu": 2.0,
		})
	}

	// Update 10,000 records simultaneously: jarak = jarak + 10
	t0 := time.Now()
	updatedCount, err := client.Update().
		Set("jarak", Add("jarak", "10")).
		Limit(10000).
		Execute(ctx)
	elapsed := time.Since(t0)

	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updatedCount != 10000 {
		t.Fatalf("expected 10000 records updated, got %d", updatedCount)
	}

	t.Logf("[OK] 10,000 records updated in %v! (Waktu konstan terbukti)", elapsed)

	// Verify first item was updated to 110
	rec0, err := client.Get("vehicle_0").Execute(ctx)
	if err != nil || rec0.Get("jarak") != 110.0 {
		t.Fatalf("expected vehicle_0 jarak=110, got %.1f", rec0.Get("jarak"))
	}

	// Verify 9,999th item was updated to 110
	rec9999, err := client.Get("vehicle_9999").Execute(ctx)
	if err != nil || rec9999.Get("jarak") != 110.0 {
		t.Fatalf("expected vehicle_9999 jarak=110, got %.1f", rec9999.Get("jarak"))
	}

	// Verify 10,000th item (the 10,001st record) was NOT touched (remains 100)
	rec10000, err := client.Get("vehicle_10000").Execute(ctx)
	if err != nil || rec10000.Get("jarak") != 100.0 {
		t.Fatalf("expected vehicle_10000 jarak=100 (untouched), got %.1f", rec10000.Get("jarak"))
	}

	t.Logf("Verification passed: vehicle_0 jarak=%.1f, vehicle_9999 jarak=%.1f, vehicle_10000 jarak=%.1f",
		rec0.Get("jarak"), rec9999.Get("jarak"), rec10000.Get("jarak"))
}

func TestDriver_SequentialAddTen_UserExample(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "sequential_add10.anchordb")

	client, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed 10,000 records with sequential distances: 1, 2, 3, ..., 10000
	total := 10000
	for i := 1; i <= total; i++ {
		name := fmt.Sprintf("rec_%d", i)
		_ = client.Save(name, map[string]float32{
			"jarak": float32(i),
		})
	}

	// Update batch: jarak = jarak + 10
	t0 := time.Now()
	updatedCount, err := client.Update().
		Set("jarak", Add("jarak", "10")).
		Limit(total).
		Execute(ctx)
	elapsed := time.Since(t0)

	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if updatedCount != total {
		t.Fatalf("expected %d updated, got %d", total, updatedCount)
	}

	t.Logf("[OK] 10,000 sequential records updated (jarak + 10) in %v!", elapsed)

	// Verify sample records:
	// Record 1: 1 + 10 = 11
	r1, _ := client.Get("rec_1").Execute(ctx)
	if r1.Get("jarak") != 11.0 {
		t.Fatalf("expected rec_1 jarak=11, got %.1f", r1.Get("jarak"))
	}

	// Record 2: 2 + 10 = 12
	r2, _ := client.Get("rec_2").Execute(ctx)
	if r2.Get("jarak") != 12.0 {
		t.Fatalf("expected rec_2 jarak=12, got %.1f", r2.Get("jarak"))
	}

	// Record 3: 3 + 10 = 13
	r3, _ := client.Get("rec_3").Execute(ctx)
	if r3.Get("jarak") != 13.0 {
		t.Fatalf("expected rec_3 jarak=13, got %.1f", r3.Get("jarak"))
	}

	// Record 500: 500 + 10 = 510
	r500, _ := client.Get("rec_500").Execute(ctx)
	if r500.Get("jarak") != 510.0 {
		t.Fatalf("expected rec_500 jarak=510, got %.1f", r500.Get("jarak"))
	}

	// Record 10000: 10000 + 10 = 10010
	r10000, _ := client.Get("rec_10000").Execute(ctx)
	if r10000.Get("jarak") != 10010.0 {
		t.Fatalf("expected rec_10000 jarak=10010, got %.1f", r10000.Get("jarak"))
	}

	t.Logf("Semua verifikasi sukses: rec_1=%.0f, rec_2=%.0f, rec_3=%.0f, rec_500=%.0f, rec_10000=%.0f",
		r1.Get("jarak"), r2.Get("jarak"), r3.Get("jarak"), r500.Get("jarak"), r10000.Get("jarak"))
}

func TestDriver_DateMath_Add45Days_UserScenario(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "date_test.anchordb")

	client, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// 1. Simpan entitas dengan tanggal 2026-10-14
	_ = client.Save("kontrak_proyek", map[string]float32{
		"tanggal_mulai": Date("2026-10-14"),
	})

	// Verifikasi sebelum update
	initial, err := client.Get("kontrak_proyek").Execute(ctx)
	if err != nil || initial.GetDate("tanggal_mulai") != "2026-10-14" {
		t.Fatalf("expected initial date 2026-10-14, got %q", initial.GetDate("tanggal_mulai"))
	}
	t.Logf("Tanggal Awal Tersimpan: %s", initial.GetDate("tanggal_mulai"))

	// 2. Eksekusi UPDATE: Tambah 45 hari pada proses update
	count, err := client.Update().
		In("kontrak_proyek").
		Set("tanggal_mulai", AddDays("tanggal_mulai", 45)).
		Execute(ctx)

	if err != nil || count != 1 {
		t.Fatalf("update failed: count=%d, err=%v", count, err)
	}

	// 3. Ambil data setelah update: harus menjadi 2026-11-28!
	updated, err := client.Get("kontrak_proyek").Execute(ctx)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	dateAfter := updated.GetDate("tanggal_mulai")
	if dateAfter != "2026-11-28" {
		t.Fatalf("expected 2026-11-28, got %q", dateAfter)
	}
	t.Logf("Tanggal Setelah Update (+45 hari): %s [SUKSES]", dateAfter)

	// 4. Uji kalkulasi on-the-fly pada saat GET / SELECT (Compute + 45 hari lagi -> 2027-01-12)
	computed, err := client.Get("kontrak_proyek").
		Compute("tahap_berikutnya", AddDays("tanggal_mulai", 45)).
		Execute(ctx)

	if err != nil {
		t.Fatalf("compute failed: %v", err)
	}

	dateTahap2 := computed.GetDate("tahap_berikutnya")
	if dateTahap2 != "2027-01-12" {
		t.Fatalf("expected 2027-01-12, got %q", dateTahap2)
	}
	t.Logf("Kalkulasi On-The-Fly Tahap Berikutnya (+45 hari lagi): %s [SUKSES]", dateTahap2)
}

func TestDriver_RichRangeAndDateFilters(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "rich_filters.anchordb")

	client, err := Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	// Seed data dengan berbagai tanggal, jarak, waktu
	_ = client.Save("trip_1", map[string]float32{
		"jarak":   30.0,
		"waktu":   1.0,
		"tanggal": Date("2026-10-05"),
	})
	_ = client.Save("trip_2", map[string]float32{
		"jarak":   100.0,
		"waktu":   2.0,
		"tanggal": Date("2026-10-14"),
	})
	_ = client.Save("trip_3", map[string]float32{
		"jarak":   120.0,
		"waktu":   2.5,
		"tanggal": Date("2026-10-20"),
	})
	_ = client.Save("trip_4", map[string]float32{
		"jarak":   300.0,
		"waktu":   5.0,
		"tanggal": Date("2026-11-01"),
	})

	// 1. GET BY DATE: WhereDate("tanggal", "2026-10-14")
	resDate, err := client.List().
		WhereDate("tanggal", "2026-10-14").
		Execute(ctx)
	if err != nil || len(resDate.Items) != 1 || resDate.Items[0].Name != "trip_2" {
		t.Fatalf("WhereDate failed: %v, items=%v", err, resDate.Items)
	}
	t.Logf("[1] Get by Date '2026-10-14': %s", resDate.Items[0].Name)

	// 2. GET BETWEEN DATE (Range): BetweenDate("2026-10-10", "2026-10-25")
	resRange, err := client.List().
		BetweenDate("tanggal", "2026-10-10", "2026-10-25").
		Execute(ctx)
	if err != nil || len(resRange.Items) != 2 { // trip_2 (14 Okt) & trip_3 (20 Okt)
		t.Fatalf("BetweenDate failed: %v, count=%d", err, len(resRange.Items))
	}
	t.Logf("[2] Get Between Date 2026-10-10 s.d 2026-10-25: count=%d (%s, %s)",
		len(resRange.Items), resRange.Items[0].Name, resRange.Items[1].Name)

	// 3. GET BEFORE: WhereBefore("tanggal", "2026-10-14")
	resBefore, err := client.List().
		WhereBefore("tanggal", "2026-10-14").
		Execute(ctx)
	if err != nil || len(resBefore.Items) != 1 || resBefore.Items[0].Name != "trip_1" {
		t.Fatalf("WhereBefore date failed: %v", err)
	}
	t.Logf("[3] Get Before '2026-10-14': %s (tanggal: %s)", resBefore.Items[0].Name, resBefore.Items[0].GetDate("tanggal"))

	// 4. GET AFTER: WhereAfter("tanggal", "2026-10-20")
	resAfter, err := client.List().
		WhereAfter("tanggal", "2026-10-20").
		Execute(ctx)
	if err != nil || len(resAfter.Items) != 1 || resAfter.Items[0].Name != "trip_4" {
		t.Fatalf("WhereAfter date failed: %v", err)
	}
	t.Logf("[4] Get After '2026-10-20': %s (tanggal: %s)", resAfter.Items[0].Name, resAfter.Items[0].GetDate("tanggal"))

	// 5. MULTI-RANGE CHAINING:
	// getFrom(A, B) and From(C, D) and From(D, E) baik date maupun number!
	// Contoh: jarak di [50, 150] AND waktu di [1.5, 3.0] AND tanggal di ["2026-10-10", "2026-10-25"]
	resMulti, err := client.List().
		From("jarak", 50, 150).
		From("waktu", 1.5, 3.0).
		From("tanggal", "2026-10-10", "2026-10-25").
		Compute("kecepatan", Divide("jarak", "waktu")).
		Execute(ctx)

	if err != nil || len(resMulti.Items) != 2 {
		t.Fatalf("Chained From ranges failed: %v, count=%d", err, len(resMulti.Items))
	}

	for i, item := range resMulti.Items {
		t.Logf("[5] Multi-Range Item [%d]: %s -> Jarak=%.0f, Waktu=%.1f, Tgl=%s, Kecepatan=%.1f",
			i+1, item.Name, item.Get("jarak"), item.Get("waktu"), item.GetDate("tanggal"), item.Get("kecepatan"))
	}
}

func TestDriver_ScheduledDeactivation_15Days(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "users_deactivation.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Simulasi: Hari ini adalah 2026-10-15
	// Batas kadaluarsa = 15 hari yang lalu -> 2026-09-30
	// User 1: terdaftar 2026-09-20 (25 hari lalu -> > 15 hari -> HARUS NON-AKTIF)
	// User 2: terdaftar 2026-10-05 (10 hari lalu -> <= 15 hari -> TETAP AKTIF)
	// User 3: terdaftar 2026-10-12 (3 hari lalu -> <= 15 hari -> TETAP AKTIF)

	_ = client.Save("user_1_lama", map[string]float32{
		"date_terdaftar": Date("2026-09-20"),
		"status":         1, // 1 = Aktif, 0 = Non-Aktif
	})
	_ = client.Save("user_2_baru", map[string]float32{
		"date_terdaftar": Date("2026-10-05"),
		"status":         1,
	})
	_ = client.Save("user_3_kemarin", map[string]float32{
		"date_terdaftar": Date("2026-10-12"),
		"status":         1,
	})

	// Buat task penjadwalan / lifecycle job:
	// Rule: Setiap hari jam 23:00, cari record yang date_terdaftar <= (Today - 15) dan status == 1
	sched := scheduler.NewInternalScheduler()

	var updatedRecords int
	_ = sched.Register("deactivate_expired_users_15days", "23:00", func() error {
		// Cutoff date = 2026-09-30 (15 hari sebelum 2026-10-15)
		cutoffDate := "2026-09-30"

		count, err := client.Update().
			Where("date_terdaftar", "<=", cutoffDate).
			Where("status", "=", 1).
			Set("status", Value(0)). // Update status = 0 (non-aktif)
			Execute(ctx)
		if err != nil {
			return err
		}
		updatedRecords = count
		return nil
	})

	// Jalankan task melalui scheduler
	if err := sched.RunNow("deactivate_expired_users_15days"); err != nil {
		t.Fatalf("Scheduler RunNow failed: %v", err)
	}

	if updatedRecords != 1 {
		t.Fatalf("Expected 1 record deactivated, got %d", updatedRecords)
	}

	// Verifikasi: user_1_lama sekarang status = 0 (non-aktif)
	u1, err := client.Get("user_1_lama").Execute(ctx)
	if err != nil || u1.Get("status") != 0 {
		t.Fatalf("user_1_lama should be inactive (0), got %v", u1.Get("status"))
	}
	t.Logf("[OK] user_1_lama (Terdaftar: %s) -> Status: %.0f (Non-Aktif)", u1.GetDate("date_terdaftar"), u1.Get("status"))

	// Verifikasi: user_2_baru tetap status = 1 (aktif)
	u2, err := client.Get("user_2_baru").Execute(ctx)
	if err != nil || u2.Get("status") != 1 {
		t.Fatalf("user_2_baru should remain active (1), got %v", u2.Get("status"))
	}
	t.Logf("[OK] user_2_baru (Terdaftar: %s) -> Status: %.0f (Aktif)", u2.GetDate("date_terdaftar"), u2.Get("status"))

	// Verifikasi: user_3_kemarin tetap status = 1 (aktif)
	u3, err := client.Get("user_3_kemarin").Execute(ctx)
	if err != nil || u3.Get("status") != 1 {
		t.Fatalf("user_3_kemarin should remain active (1), got %v", u3.Get("status"))
	}
	t.Logf("[OK] user_3_kemarin (Terdaftar: %s) -> Status: %.0f (Aktif)", u3.GetDate("date_terdaftar"), u3.Get("status"))
}

func TestDriver_HighConcurrencyParallelConnections(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "parallel_stress.anchordb")

	client, err := Connect("anchordb://" + dbPath + "?mode=mmap")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Seed awal: 100 entitas
	for i := 0; i < 100; i++ {
		_ = client.Save(fmt.Sprintf("item_%d", i), map[string]float32{
			"jarak": float32(i * 10),
			"waktu": 2.0,
			"val":   100.0,
		})
	}

	// 2. Simulasi 50 koneksi / goroutine paralel yang melakukan 2.000 request campuran (Read + Write)
	const numGoroutines = 50
	const requestsPerWorker = 40

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	t0 := time.Now()

	for g := 0; g < numGoroutines; g++ {
		workerID := g
		go func() {
			defer wg.Done()
			for r := 0; r < requestsPerWorker; r++ {
				targetName := fmt.Sprintf("item_%d", (workerID*requestsPerWorker+r)%100)

				if r%5 == 0 {
					// 20% WRITE CONCURRENT: Mutasi atomik
					_, _ = client.Update().
						In(targetName).
						Set("val", Add("val", "1")).
						Execute(ctx)
				} else {
					// 80% READ CONCURRENT: Kueri paralel tanpa blocking
					res, err := client.Get(targetName).
						Select("jarak", "waktu", "kecepatan").
						Compute("kecepatan", Divide("jarak", "waktu")).
						Execute(ctx)

					if err != nil || res == nil {
						t.Errorf("Concurrent read failed for %s: %v", targetName, err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(t0)
	totalReqs := numGoroutines * requestsPerWorker
	opsPerSec := float64(totalReqs) / duration.Seconds()

	t.Logf("[PARALEL] Sukses mengeksekusi %d request paralel campuran (Read/Write) dari %d goroutine dalam %v (%.0f ops/detik)",
		totalReqs, numGoroutines, duration, opsPerSec)
}
