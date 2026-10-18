package spatial

import (
	"math"
	"testing"
)

func TestBox2D_Contains_Intersects(t *testing.T) {
	// Create un-ordered points to test normalization
	p1 := Point2D{X: 10, Y: 20}
	p2 := Point2D{X: -5, Y: -10}
	box := NewBox2D(p1, p2)

	if box.Min.X != -5 || box.Min.Y != -10 {
		t.Fatalf("expected Min (-5, -10), got (%f, %f)", box.Min.X, box.Min.Y)
	}
	if box.Max.X != 10 || box.Max.Y != 20 {
		t.Fatalf("expected Max (10, 20), got (%f, %f)", box.Max.X, box.Max.Y)
	}

	// Contains
	if !box.Contains(Point2D{X: 0, Y: 0}) {
		t.Errorf("expected (0,0) to be inside box")
	}
	if !box.Contains(Point2D{X: -5, Y: 20}) {
		t.Errorf("expected boundary point (-5, 20) to be inside box")
	}
	if box.Contains(Point2D{X: 10.1, Y: 0}) {
		t.Errorf("expected (10.1, 0) to be outside box")
	}
	if box.Contains(Point2D{X: 0, Y: -10.1}) {
		t.Errorf("expected (0, -10.1) to be outside box")
	}

	// Intersects
	overlappingBox := NewBox2D(Point2D{X: 5, Y: 5}, Point2D{X: 15, Y: 25})
	if !box.Intersects(overlappingBox) {
		t.Errorf("expected overlappingBox to intersect")
	}
	if !overlappingBox.Intersects(box) {
		t.Errorf("expected intersection to be symmetric")
	}

	disjointBox := NewBox2D(Point2D{X: 20, Y: 30}, Point2D{X: 25, Y: 35})
	if box.Intersects(disjointBox) {
		t.Errorf("expected disjointBox to NOT intersect")
	}
}

func TestPolygon2D_Construction(t *testing.T) {
	// Too few vertices
	_, err := NewPolygon2D([]Point2D{{X: 0, Y: 0}, {X: 1, Y: 1}})
	if err == nil {
		t.Fatalf("expected error for 2 vertices, got nil")
	}

	// Valid triangle
	verts := []Point2D{
		{X: 0, Y: 0},
		{X: 4, Y: 0},
		{X: 0, Y: 3},
	}
	poly, err := NewPolygon2D(verts)
	if err != nil {
		t.Fatalf("unexpected error constructing valid polygon: %v", err)
	}

	if poly.BBox.Min.X != 0 || poly.BBox.Min.Y != 0 {
		t.Errorf("expected BBox Min (0,0), got (%f, %f)", poly.BBox.Min.X, poly.BBox.Min.Y)
	}
	if poly.BBox.Max.X != 4 || poly.BBox.Max.Y != 3 {
		t.Errorf("expected BBox Max (4,3), got (%f, %f)", poly.BBox.Max.X, poly.BBox.Max.Y)
	}
}

func TestPolygon2D_Contains_ConvexAndConcave(t *testing.T) {
	// 1. Convex Square [0, 10] x [0, 10]
	squareVerts := []Point2D{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 10, Y: 10},
		{X: 0, Y: 10},
	}
	square, err := NewPolygon2D(squareVerts)
	if err != nil {
		t.Fatalf("failed creating square: %v", err)
	}

	if !square.Contains(Point2D{X: 5, Y: 5}) {
		t.Errorf("expected center (5,5) to be inside square")
	}
	if square.Contains(Point2D{X: -1, Y: 5}) {
		t.Errorf("expected (-1, 5) to be outside square")
	}
	if square.Contains(Point2D{X: 11, Y: 5}) {
		t.Errorf("expected (11, 5) to be outside square")
	}

	// 2. Concave L-shape / Chevron Polygon
	// (0,0) -> (10,0) -> (10,5) -> (5,5) -> (5,10) -> (0,10)
	lShapeVerts := []Point2D{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 10, Y: 5},
		{X: 5, Y: 5},
		{X: 5, Y: 10},
		{X: 0, Y: 10},
	}
	lShape, err := NewPolygon2D(lShapeVerts)
	if err != nil {
		t.Fatalf("failed creating L-shape: %v", err)
	}

	// Point in lower arm
	if !lShape.Contains(Point2D{X: 7, Y: 2}) {
		t.Errorf("expected (7,2) to be inside L-shape")
	}
	// Point in upper arm
	if !lShape.Contains(Point2D{X: 2, Y: 7}) {
		t.Errorf("expected (2,7) to be inside L-shape")
	}
	// Point in empty cutout corner (7, 7) - within BBox [0,10]x[0,10], but outside polygon!
	if lShape.Contains(Point2D{X: 7, Y: 7}) {
		t.Errorf("expected cutout point (7,7) to be outside L-shape")
	}
	// Point far outside BBox
	if lShape.Contains(Point2D{X: 20, Y: 20}) {
		t.Errorf("expected (20,20) to be outside L-shape")
	}
}

func TestPolygon2D_Area_Perimeter(t *testing.T) {
	// Right triangle: base=4, height=3, hypotenuse=5
	verts := []Point2D{
		{X: 0, Y: 0},
		{X: 4, Y: 0},
		{X: 0, Y: 3},
	}
	poly, _ := NewPolygon2D(verts)

	area := poly.Area()
	expectedArea := float32(6.0)
	if math.Abs(float64(area-expectedArea)) > 1e-4 {
		t.Errorf("expected area %f, got %f", expectedArea, area)
	}

	perim := poly.Perimeter()
	expectedPerim := float32(12.0) // 4 + 3 + 5
	if math.Abs(float64(perim-expectedPerim)) > 1e-4 {
		t.Errorf("expected perimeter %f, got %f", expectedPerim, perim)
	}
}

func TestWithinRadius(t *testing.T) {
	center := Point2D{X: 10, Y: 10}

	// Distance 3: (10, 13)
	if !WithinRadius(Point2D{X: 10, Y: 13}, center, 5) {
		t.Errorf("expected point at distance 3 to be within radius 5")
	}
	// Exactly at boundary: distance 5: (10, 15)
	if !WithinRadius(Point2D{X: 10, Y: 15}, center, 5) {
		t.Errorf("expected boundary point at distance 5 to be within radius 5")
	}
	// Distance > 5: (10, 16)
	if WithinRadius(Point2D{X: 10, Y: 16}, center, 5) {
		t.Errorf("expected point at distance 6 to NOT be within radius 5")
	}
	// Non-positive radius
	if WithinRadius(Point2D{X: 10, Y: 10}, center, 0) {
		t.Errorf("expected radius 0 to return false")
	}
}

func TestHaversineDistanceKM(t *testing.T) {
	// Monas Jakarta: Lon 106.8272, Lat -6.1754
	monas := Point2D{X: 106.8272, Y: -6.1754}
	// Gedung Sate Bandung: Lon 107.6186, Lat -6.9025
	gedungSate := Point2D{X: 107.6186, Y: -6.9025}

	// Identical points distance should be 0
	if dist := HaversineDistanceKM(monas, monas); dist != 0 {
		t.Errorf("expected distance between identical points to be 0, got %f", dist)
	}

	// Real distance is ~119 - 120 km
	dist := HaversineDistanceKM(monas, gedungSate)
	if dist < 115 || dist > 125 {
		t.Errorf("expected Jakarta - Bandung distance to be ~119 km, got %f km", dist)
	}
}

func TestInitialBearing(t *testing.T) {
	origin := Point2D{X: 0, Y: 0}

	// North (Lon 0, Lat 10)
	bearingNorth := InitialBearing(origin, Point2D{X: 0, Y: 10})
	if math.Abs(float64(bearingNorth-0)) > 1e-2 && math.Abs(float64(bearingNorth-360)) > 1e-2 {
		t.Errorf("expected Due North bearing ~0°, got %f°", bearingNorth)
	}

	// East (Lon 10, Lat 0)
	bearingEast := InitialBearing(origin, Point2D{X: 10, Y: 0})
	if math.Abs(float64(bearingEast-90)) > 1e-2 {
		t.Errorf("expected Due East bearing ~90°, got %f°", bearingEast)
	}

	// South (Lon 0, Lat -10)
	bearingSouth := InitialBearing(origin, Point2D{X: 0, Y: -10})
	if math.Abs(float64(bearingSouth-180)) > 1e-2 {
		t.Errorf("expected Due South bearing ~180°, got %f°", bearingSouth)
	}

	// West (Lon -10, Lat 0)
	bearingWest := InitialBearing(origin, Point2D{X: -10, Y: 0})
	if math.Abs(float64(bearingWest-270)) > 1e-2 {
		t.Errorf("expected Due West bearing ~270°, got %f°", bearingWest)
	}
}

func BenchmarkPolygon2D_Contains_BroadPhaseExit(b *testing.B) {
	poly, _ := NewPolygon2D([]Point2D{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 10, Y: 10},
		{X: 0, Y: 10},
	})
	ptOutside := Point2D{X: 50, Y: 50}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = poly.Contains(ptOutside)
	}
}

func BenchmarkPolygon2D_Contains_NarrowPhase(b *testing.B) {
	poly, _ := NewPolygon2D([]Point2D{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 10, Y: 10},
		{X: 0, Y: 10},
	})
	ptInside := Point2D{X: 5, Y: 5}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = poly.Contains(ptInside)
	}
}

func BenchmarkWithinRadius(b *testing.B) {
	center := Point2D{X: 10, Y: 10}
	p := Point2D{X: 12, Y: 13}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = WithinRadius(p, center, 5)
	}
}

func BenchmarkHaversineDistanceKM(b *testing.B) {
	p1 := Point2D{X: 106.8272, Y: -6.1754}
	p2 := Point2D{X: 107.6186, Y: -6.9025}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = HaversineDistanceKM(p1, p2)
	}
}
