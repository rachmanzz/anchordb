package spatial

import (
	"errors"
	"math"
)

// ErrInvalidVector is returned when a vector has zero length or non-finite components.
var ErrInvalidVector = errors.New("spatial: cannot normalize zero, NaN, or non-finite vector")

// Point2D represents a 2D coordinate in semantic space.
type Point2D struct {
	X float32 `json:"x"`
	Y float32 `json:"y"`
}

// DistanceSq computes squared Euclidean distance (avoids math.Sqrt).
func (p Point2D) DistanceSq(other Point2D) float32 {
	dx := float64(p.X) - float64(other.X)
	dy := float64(p.Y) - float64(other.Y)
	return float32(dx*dx + dy*dy)
}

// Distance computes exact Euclidean distance without float32 intermediate overflow/underflow.
func (p Point2D) Distance(other Point2D) float32 {
	return float32(math.Hypot(float64(p.X)-float64(other.X), float64(p.Y)-float64(other.Y)))
}

// Add adds two points/vectors.
func (p Point2D) Add(other Point2D) Point2D {
	return Point2D{X: p.X + other.X, Y: p.Y + other.Y}
}

// Sub subtracts other from p.
func (p Point2D) Sub(other Point2D) Point2D {
	return Point2D{X: p.X - other.X, Y: p.Y - other.Y}
}

// Mul scales point by scalar.
func (p Point2D) Mul(s float32) Point2D {
	return Point2D{X: p.X * s, Y: p.Y * s}
}

// Normalize normalizes vector to unit length (length = 1.0).
// Returns ErrInvalidVector when the vector is zero, NaN, or non-finite.
func (p Point2D) Normalize() (Point2D, error) {
	x, y := float64(p.X), float64(p.Y)
	if math.IsNaN(x) || math.IsInf(x, 0) || math.IsNaN(y) || math.IsInf(y, 0) {
		return Point2D{}, ErrInvalidVector
	}
	l := math.Hypot(x, y)
	if l == 0 || math.IsNaN(l) {
		return Point2D{}, ErrInvalidVector
	}
	if math.IsInf(l, 0) {
		x *= 0.5
		y *= 0.5
		l = math.Hypot(x, y)
		if l == 0 || math.IsNaN(l) || math.IsInf(l, 0) {
			return Point2D{}, ErrInvalidVector
		}
	}
	nx, ny := float32(x/l), float32(y/l)
	if math.IsNaN(float64(nx)) || math.IsNaN(float64(ny)) || math.IsInf(float64(nx), 0) || math.IsInf(float64(ny), 0) || (nx == 0 && ny == 0) {
		return Point2D{}, ErrInvalidVector
	}
	return Point2D{X: nx, Y: ny}, nil
}
