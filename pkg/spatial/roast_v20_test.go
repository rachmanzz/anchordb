package spatial

import (
	"math"
	"testing"
)

func TestRoastV20_NormalizeAndWithinRadiusExtremeFloat32(t *testing.T) {
	nLarge := (Point2D{X: 1e30, Y: 1e30}).Normalize()
	if math.Abs(float64(nLarge.X)-math.Sqrt2/2) > 1e-4 || math.Abs(float64(nLarge.Y)-math.Sqrt2/2) > 1e-4 {
		t.Fatalf("Normalize(1e30, 1e30) = %+v, want (~0.7071, ~0.7071)", nLarge)
	}

	nTinyX := (Point2D{X: 1e-25, Y: 0}).Normalize()
	if nTinyX.X != 1 || nTinyX.Y != 0 {
		t.Fatalf("Normalize(1e-25, 0) = %+v, want {1, 0}", nTinyX)
	}

	nTinyY := (Point2D{X: 0, Y: 1e-25}).Normalize()
	if nTinyY.X != 0 || nTinyY.Y != 1 {
		t.Fatalf("Normalize(0, 1e-25) = %+v, want {0, 1}", nTinyY)
	}

	// Far point outside large radius must return false instead of overflowing both sides to +Inf.
	if WithinRadius(Point2D{X: 3e20, Y: 0}, Point2D{X: 0, Y: 0}, 2e20) {
		t.Fatalf("WithinRadius returned true when distance 3e20 > radius 2e20")
	}
	if !WithinRadius(Point2D{X: 1e20, Y: 1e20}, Point2D{X: 0, Y: 0}, 2e20) {
		t.Fatalf("WithinRadius returned false when distance ~1.414e20 <= radius 2e20")
	}
}
