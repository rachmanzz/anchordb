package spatial

import (
	"errors"
	"math"
	"testing"
)

func TestRoastV20_NormalizeAndWithinRadiusExtremeFloat32(t *testing.T) {
	nLarge, err := (Point2D{X: 1e30, Y: 1e30}).Normalize()
	if err != nil {
		t.Fatalf("Normalize(1e30, 1e30) unexpected err: %v", err)
	}
	if math.Abs(float64(nLarge.X)-math.Sqrt2/2) > 1e-4 || math.Abs(float64(nLarge.Y)-math.Sqrt2/2) > 1e-4 {
		t.Fatalf("Normalize(1e30, 1e30) = %+v, want (~0.7071, ~0.7071)", nLarge)
	}

	nTinyX, err := (Point2D{X: 1e-25, Y: 0}).Normalize()
	if err != nil {
		t.Fatalf("Normalize(1e-25, 0) unexpected err: %v", err)
	}
	if nTinyX.X != 1 || nTinyX.Y != 0 {
		t.Fatalf("Normalize(1e-25, 0) = %+v, want {1, 0}", nTinyX)
	}

	nTinyY, err := (Point2D{X: 0, Y: 1e-25}).Normalize()
	if err != nil {
		t.Fatalf("Normalize(0, 1e-25) unexpected err: %v", err)
	}
	if nTinyY.X != 0 || nTinyY.Y != 1 {
		t.Fatalf("Normalize(0, 1e-25) = %+v, want {0, 1}", nTinyY)
	}

	// Zero, NaN, and Inf vectors must fail explicitly instead of silently returning (1, 0).
	for _, invalid := range []Point2D{
		{X: 0, Y: 0},
		{X: float32(math.NaN()), Y: 1},
		{X: float32(math.Inf(1)), Y: 0},
	} {
		if _, err := invalid.Normalize(); !errors.Is(err, ErrInvalidVector) {
			t.Fatalf("Normalize(%+v) error = %v, want ErrInvalidVector", invalid, err)
		}
		if _, err := NewRay(Point2D{X: 0, Y: 0}, invalid); !errors.Is(err, ErrInvalidVector) {
			t.Fatalf("NewRay with invalid dir %+v error = %v, want ErrInvalidVector", invalid, err)
		}
	}

	// Far point outside large radius must return false instead of overflowing both sides to +Inf.
	if WithinRadius(Point2D{X: 3e20, Y: 0}, Point2D{X: 0, Y: 0}, 2e20) {
		t.Fatalf("WithinRadius returned true when distance 3e20 > radius 2e20")
	}
	if !WithinRadius(Point2D{X: 1e20, Y: 1e20}, Point2D{X: 0, Y: 0}, 2e20) {
		t.Fatalf("WithinRadius returned false when distance ~1.414e20 <= radius 2e20")
	}
}
