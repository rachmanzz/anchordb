package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func main() {
	fmt.Println("==========================================================")
	fmt.Println("    ANCHORDB: COMPUTATIONAL GRAPH STORAGE ENGINE (CPU)    ")
	fmt.Println("==========================================================")

	_ = os.MkdirAll("data", 0755)
	dbFile := filepath.Join("data", "world.anchordb")
	_ = os.Remove(dbFile) // Start fresh for demo

	// 1. Membuka koneksi via Dedicated Driver SDK
	t0 := time.Now()
	client, err := driver.Connect("anchordb://" + dbFile + "?mode=mmap")
	if err != nil {
		log.Fatalf("Gagal menghubungkan ke AnchorDB: %v", err)
	}
	defer client.Close()
	fmt.Printf("[OK] Terhubung ke AnchorDB via Driver dalam %v (mmap zero-copy)\n", time.Since(t0))

	// 2. Populasi Node Awal ke File Biner (Fixed-Size 40 Bytes / Node)
	db := client.DB()
	nodes := []struct {
		word  string
		coord spatial.Point2D
		roles [4]float32
	}{
		{"ibu", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}},
		{"ayah", spatial.Point2D{X: 0.2, Y: 0.9}, [4]float32{0.9, 0.1, 0.2, 0.2}},
		{"anak", spatial.Point2D{X: 0.2, Y: 0.1}, [4]float32{0.2, 0.1, 0.95, 0.2}},
		{"sekolah", spatial.Point2D{X: 0.2, Y: -0.5}, [4]float32{0.1, 0.1, 0.9, 0.5}},
		{"belajar", spatial.Point2D{X: 0.2, Y: -0.7}, [4]float32{0.2, 0.9, 0.5, 0.1}},
		{"buku", spatial.Point2D{X: 0.6, Y: -0.7}, [4]float32{0.1, 0.1, 0.9, 0.2}},
		{"pintar", spatial.Point2D{X: 0.8, Y: -0.9}, [4]float32{0.2, 0.1, 0.95, 0.4}},
		{"air", spatial.Point2D{X: -0.5, Y: 0.5}, [4]float32{0.8, 0.1, 0.2, 0.1}},
		{"dingin", spatial.Point2D{X: -0.5, Y: 0.5}, [4]float32{0.1, 0.1, 0.2, 0.9}},
		{"es", spatial.Point2D{X: -0.5, Y: -0.2}, [4]float32{0.2, 0.1, 0.95, 0.3}},
	}

	for _, n := range nodes {
		if _, err := db.AppendNode(n.word, n.coord, n.roles, 1); err != nil {
			log.Fatalf("failed to append node %q: %v", n.word, err)
		}
	}
	if err := db.SetRelation("demo-down", spatial.Point2D{Y: -1}); err != nil {
		log.Fatalf("failed to configure demo relation: %v", err)
	}
	_ = db.Sync()

	fi, _ := os.Stat(dbFile)
	fmt.Printf("[OK] File Biner .anchordb tersimpan: %d bytes (Ukuran Ultra-Padat!)\n\n", fi.Size())

	ctx := context.Background()

	// 3. Eksekusi Kueri Graf Menggunakan Fluent Builder Driver
	fmt.Println("----------------------------------------------------------")
	fmt.Println(">>> KUERI 1: In-Storage Pushdown Graph Step")
	fmt.Println("Query: client.Graph().From('ibu', 'ayah').Relate('demo-down').TopK(5).Execute()")

	qStart := time.Now()
	resp, err := client.Graph().
		From("ibu", "ayah").
		Relate("demo-down").
		BeamWidth(0.05).
		TopK(5).
		Execute(ctx)
	qDuration := time.Since(qStart)

	if err != nil {
		log.Fatalf("Query gagal: %v", err)
	}

	fmt.Printf("Titik Anchor Hasil Barycenter: (%.2f, %.2f)\n", resp.Anchor.X, resp.Anchor.Y)
	fmt.Printf("Waktu Komputasi Internal DB: %v (Sub-Mikrodetik!)\n", qDuration)
	fmt.Printf("Kandidat Teratas yang Lolos DDA Ray & Pythagoras:\n")
	for i, r := range resp.Results {
		fmt.Printf("   [%d] '%s' | Jarak Sumbu t: %.2f | Deviasi d_perp: %.4f | Skor: %.4f\n",
			i+1, r.Word, r.T, r.DistSqPerp, r.Score)
	}

	// 4. Kueri 2: In-Storage Multi-Hop Chaining
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> KUERI 2: In-Storage Multi-Hop Chain Execution")
	fmt.Println("Query: client.Graph().From('ibu', 'ayah').Chain('demo-down').ExecuteChain()")

	chainStart := time.Now()
	chainResp, err := client.Graph().
		From("ibu", "ayah").
		Chain("demo-down").
		ExecuteChain(ctx)
	chainDuration := time.Since(chainStart)

	if err != nil {
		log.Fatalf("Chain query gagal: %v", err)
	}

	fmt.Printf("Hasil Penalaran Berantai: %v (Waktu: %v)\n", chainResp.Sequence, chainDuration)

	// 5. Kueri 3: Kalkulasi Matematis Langsung di Dalam DB (Math Engine)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> KUERI 3: Kalkulasi Matematis Langsung di Dalam Storage (Math Engine)")

	mathEng := client.Math()

	// A. Jarak Euclidean Langsung
	dist, _ := mathEng.Distance("ibu", "anak")
	fmt.Printf("1. Jarak Spasial Langsung 'ibu' <-> 'anak': %.4f\n", dist)

	// B. Pusat Massa (Centroid) Langsung
	centroid, _ := mathEng.Centroid("ibu", "ayah", "anak")
	fmt.Printf("2. Pusat Gravitasi (Centroid) ['ibu', 'ayah', 'anak']: (%.2f, %.2f)\n",
		centroid.Coord.X, centroid.Coord.Y)

	// C. Sudut Kosinus Keselarasan Peran Semantik
	sim, _ := mathEng.CosineSimilarity("ibu", "ayah")
	fmt.Printf("3. Kesamaan Kosinus Peran 'ibu' & 'ayah': %.4f (Identik)\n", sim)

	// D. Tetangga Spasial Terdekat
	nearest, _ := mathEng.Nearest("ibu", 3)
	fmt.Println("4. Tetangga Spasial Terdekat dari 'ibu':")
	for i, n := range nearest {
		fmt.Printf("   [%d] '%s' (Jarak: %.4f)\n", i+1, n.Word, n.T)
	}

	// 6. Kueri 4: SAVING, GETTING, dan CALCULATING (Contoh Jarak, Waktu -> Kecepatan)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> KUERI 4: FUNGSI UTAMA DATABASE (SAVING & GETTING + CALCULATING)")
	fmt.Println("Contoh: Simpan jarak & waktu, ambil dan hitung kecepatan = jarak / waktu")

	// 1. SAVING: Simpan Data Mentah
	_ = client.Save("perjalanan_mobil", map[string]float32{
		"jarak": 100.0, // km
		"waktu": 2.0,   // jam
	})
	fmt.Println("1. SAVING: Data tersimpan -> 'perjalanan_mobil' {jarak: 100.0, waktu: 2.0}")

	// 2. GETTING: Ambil Data Murni
	rawEntity, _ := client.Get("perjalanan_mobil").Execute(ctx)
	fmt.Printf("2. GETTING: Data terambil murni -> %v\n", rawEntity.All())

	// 3. CALCULATING: Ambil + Hitung Kecepatan secara internal saat retrieval
	calcEntity, _ := client.Get("perjalanan_mobil").
		Compute("kecepatan", driver.Divide("jarak", "waktu")).
		Execute(ctx)

	fmt.Printf("3. CALCULATING (In-DB Compute): Jarak = %.1f km, Waktu = %.1f jam ==> Kecepatan = %.1f km/jam\n",
		calcEntity.Get("jarak"), calcEntity.Get("waktu"), calcEntity.Get("kecepatan"))
	fmt.Printf("   Return Lengkap dari DB: %v\n", calcEntity.All())

	// 7. Kueri 5: LIST, WHERE, COMPUTE, dan PAGING (WAKTU KONSTAN)
	fmt.Println("\n----------------------------------------------------------")
	fmt.Println(">>> KUERI 5: LIST + WHERE + COMPUTE + PAGING (KONSTAN O(Limit))")
	fmt.Println("Filter jarak >= 50, hitung kecepatan, filter kecepatan > 50, Limit 2 per halaman")

	_ = client.Save("trip_jakarta_bandung", map[string]float32{"jarak": 150, "waktu": 2})  // v = 75
	_ = client.Save("trip_surabaya_malang", map[string]float32{"jarak": 90, "waktu": 1.5}) // v = 60
	_ = client.Save("trip_keliling_komplek", map[string]float32{"jarak": 10, "waktu": 1})  // v = 10 (ditolak filter)
	_ = client.Save("trip_tol_trans_jawa", map[string]float32{"jarak": 400, "waktu": 4})   // v = 100

	// Halaman 1
	p1, _ := client.List().
		Where("jarak", ">=", 50).
		Compute("kecepatan", driver.Divide("jarak", "waktu")).
		Where("kecepatan", ">", 50).
		Page(1, 2).
		Execute(ctx)

	fmt.Println("--- HALAMAN 1 (Limit: 2) ---")
	for i, item := range p1.Items {
		fmt.Printf("   [%d] %-22s | Jarak: %5.0f km | Waktu: %3.1f jam | Kecepatan: %5.0f km/jam\n",
			i+1, item.Name, item.Get("jarak"), item.Get("waktu"), item.Get("kecepatan"))
	}
	fmt.Printf("   HasMore: %v\n", p1.HasMore)

	// Halaman 2
	p2, _ := client.List().
		Where("jarak", ">=", 50).
		Compute("kecepatan", driver.Divide("jarak", "waktu")).
		Where("kecepatan", ">", 50).
		Page(2, 2).
		Execute(ctx)

	fmt.Println("--- HALAMAN 2 (Next Page) ---")
	for i, item := range p2.Items {
		fmt.Printf("   [%d] %-22s | Jarak: %5.0f km | Waktu: %3.1f jam | Kecepatan: %5.0f km/jam\n",
			i+1, item.Name, item.Get("jarak"), item.Get("waktu"), item.Get("kecepatan"))
	}
	fmt.Printf("   HasMore: %v (Paging Selesai!)\n", p2.HasMore)

	fmt.Println("==========================================================")
}
