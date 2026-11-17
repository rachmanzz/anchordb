package spatial

import "math"

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
func (p Point2D) Normalize() Point2D {
	x, y := float64(p.X), float64(p.Y)
	l := math.Hypot(x, y)
	if l == 0 || math.IsNaN(l) {
		return Point2D{X: 1, Y: 0}
	}
	if math.IsInf(l, 0) {
		x *= 0.5
		y *= 0.5
		l = math.Hypot(x, y)
		if l == 0 || math.IsNaN(l) || math.IsInf(l, 0) {
			return Point2D{X: 1, Y: 0}
		}
	}
	return Point2D{X: float32(x / l), Y: float32(y / l)}
}
