package spatial

import (
	"math"
	"testing"
)

func TestPoint2D_DistanceSq(t *testing.T) {
	p1 := Point2D{X: 0, Y: 0}
	p2 := Point2D{X: 3, Y: 4}

	distSq := p1.DistanceSq(p2)
	if distSq != 25 {
		t.Fatalf("expected DistanceSq = 25, got %f", distSq)
	}

	dist := p1.Distance(p2)
	if dist != 5 {
		t.Fatalf("expected Distance = 5, got %f", dist)
	}
}

func TestRay_Project_OnLine(t *testing.T) {
	// Ray from (0, 0) pointing directly along +X axis: (1, 0)
	ray := NewRay(Point2D{X: 0, Y: 0}, Point2D{X: 1, Y: 0})

	// Target point directly on the line at (5, 0)
	target := Point2D{X: 5, Y: 0}
	projT, distSqPerp := ray.Project(target)

	if math.Abs(float64(projT-5.0)) > 1e-5 {
		t.Fatalf("expected t = 5.0, got %f", projT)
	}
	if distSqPerp > 1e-5 {
		t.Fatalf("expected distSqPerp ~ 0, got %f", distSqPerp)
	}

	energy := ray.BeamEnergy(distSqPerp, 0.05)
	if energy < 0.99 {
		t.Fatalf("expected energy ~ 1.0 for point directly on ray, got %f", energy)
	}
}

func TestRay_Project_Offset(t *testing.T) {
	// Ray from (0, 0) pointing along +Y axis: (0, 1)
	ray := NewRay(Point2D{X: 0, Y: 0}, Point2D{X: 0, Y: 1})

	// Target point offset at (3, 4): t should be 4, d_perp should be 3 (distSqPerp = 9)
	target := Point2D{X: 3, Y: 4}
	projT, distSqPerp := ray.Project(target)

	if math.Abs(float64(projT-4.0)) > 1e-5 {
		t.Fatalf("expected t = 4.0, got %f", projT)
	}
	if math.Abs(float64(distSqPerp-9.0)) > 1e-4 {
		t.Fatalf("expected distSqPerp = 9.0, got %f", distSqPerp)
	}
}

func TestRay_Project_Behind(t *testing.T) {
	// Ray pointing along +X: (1, 0)
	ray := NewRay(Point2D{X: 0, Y: 0}, Point2D{X: 1, Y: 0})

	// Target point behind ray at (-2, 0)
	target := Point2D{X: -2, Y: 0}
	projT, _ := ray.Project(target)

	if projT != 0 {
		t.Fatalf("expected t = 0 for point behind ray, got %f", projT)
	}
}
