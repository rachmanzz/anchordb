package storage

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestAnchorDB_GeometryQueries(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geom_test.anchordb")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Seed nodes:
	// Monas Jakarta (106.8272, -6.1754)
	// Gedung Sate Bandung (107.6186, -6.9025)
	// Surabaya Tugu Pahlawan (112.7378, -7.2458)
	// Yogyakarta Malioboro (110.3658, -7.7956)
	_, _ = db.AppendNode("jakarta_monas", spatial.Point2D{X: 106.8272, Y: -6.1754}, [4]float32{1, 0, 0, 0}, 1)
	_, _ = db.AppendNode("bandung_gedung_sate", spatial.Point2D{X: 107.6186, Y: -6.9025}, [4]float32{0, 1, 0, 0}, 1)
	_, _ = db.AppendNode("surabaya_tugu", spatial.Point2D{X: 112.7378, Y: -7.2458}, [4]float32{0, 0, 1, 0}, 1)
	_, _ = db.AppendNode("jogja_malioboro", spatial.Point2D{X: 110.3658, Y: -7.7956}, [4]float32{0, 0, 0, 1}, 1)

	// 1. CalculateHaversineDistance
	distJB, err := db.CalculateHaversineDistance("jakarta_monas", "bandung_gedung_sate")
	if err != nil {
		t.Fatalf("CalculateHaversineDistance error: %v", err)
	}
	if distJB < 115 || distJB > 125 {
		t.Errorf("expected Jakarta-Bandung distance ~119 km, got %f km", distJB)
	}

	// Non-existent node error check
	if _, err := db.CalculateHaversineDistance("jakarta_monas", "unknown_node"); err == nil {
		t.Errorf("expected error for non-existent node, got nil")
	}

	// 2. FindNodesInPolygon (Bounding Box around West Java / Jakarta & Bandung)
	westJavaPoly, err := spatial.NewPolygon2D([]spatial.Point2D{
		{X: 106.5, Y: -6.0},
		{X: 108.0, Y: -6.0},
		{X: 108.0, Y: -7.2},
		{X: 106.5, Y: -7.2},
	})
	if err != nil {
		t.Fatalf("failed constructing polygon: %v", err)
	}

	westJavaNodes, err := db.FindNodesInPolygon(westJavaPoly, 10)
	if err != nil {
		t.Fatalf("FindNodesInPolygon error: %v", err)
	}
	if len(westJavaNodes) != 2 {
		t.Fatalf("expected 2 nodes in West Java polygon (Jakarta and Bandung), got %d", len(westJavaNodes))
	}

	foundJakarta := false
	foundBandung := false
	for _, n := range westJavaNodes {
		if n.Word == "jakarta_monas" {
			foundJakarta = true
		}
		if n.Word == "bandung_gedung_sate" {
			foundBandung = true
		}
	}
	if !foundJakarta || !foundBandung {
		t.Errorf("expected jakarta and bandung in polygon, got %v", westJavaNodes)
	}

	// 3. FindNodesInRadius (around Jakarta center, radius 0.5 degrees)
	nearJakarta, err := db.FindNodesInRadius(spatial.Point2D{X: 106.82, Y: -6.17}, 0.5, 10)
	if err != nil {
		t.Fatalf("FindNodesInRadius error: %v", err)
	}
	if len(nearJakarta) != 1 || nearJakarta[0].Word != "jakarta_monas" {
		t.Fatalf("expected only jakarta_monas within 0.5 deg radius, got %v", nearJakarta)
	}

	// Verify distance and score are populated
	if nearJakarta[0].T <= 0 || nearJakarta[0].Score <= 0 {
		t.Errorf("expected valid T and Score in result: %+v", nearJakarta[0])
	}

	// Radius covering all Java nodes (radius 10 degrees)
	allJava, err := db.FindNodesInRadius(spatial.Point2D{X: 110.0, Y: -7.0}, 10.0, 2)
	if err != nil {
		t.Fatalf("FindNodesInRadius error: %v", err)
	}
	if len(allJava) != 2 {
		t.Errorf("expected limitK=2 to cap result count, got %d", len(allJava))
	}
}

func TestAnchorDB_GeometryErrorsWhenClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "closed_geom.anchordb")
	db, _ := Open(dbPath)
	db.Close()

	poly, _ := spatial.NewPolygon2D([]spatial.Point2D{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 0, Y: 1}})
	if _, err := db.FindNodesInPolygon(poly, 5); err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed on FindNodesInPolygon, got %v", err)
	}
	if _, err := db.FindNodesInRadius(spatial.Point2D{X: 0, Y: 0}, 1.0, 5); err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed on FindNodesInRadius, got %v", err)
	}
}

func BenchmarkAnchorDB_FindNodesInPolygon(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bench_poly.anchordb")
	db, _ := Open(dbPath)
	defer db.Close()

	// Seed 10,000 nodes
	for i := 0; i < 10000; i++ {
		angle := float64(i) * 0.01
		x := float32(math.Cos(angle) * 100)
		y := float32(math.Sin(angle) * 100)
		_, _ = db.AppendNode("node_"+string(rune(i)), spatial.Point2D{X: x, Y: y}, [4]float32{1, 0, 0, 0}, 1)
	}

	poly, _ := spatial.NewPolygon2D([]spatial.Point2D{
		{X: 0, Y: 0},
		{X: 50, Y: 0},
		{X: 50, Y: 50},
		{X: 0, Y: 50},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.FindNodesInPolygon(poly, 100)
	}
}
