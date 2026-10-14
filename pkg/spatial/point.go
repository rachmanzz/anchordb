package spatial

import "math"

// Point2D represents a 2D coordinate in semantic space.
type Point2D struct {
	X float32 `json:"x"`
	Y float32 `json:"y"`
}

// DistanceSq computes squared Euclidean distance (avoids math.Sqrt).
func (p Point2D) DistanceSq(other Point2D) float32 {
	dx := p.X - other.X
	dy := p.Y - other.Y
	return dx*dx + dy*dy
}

// Distance computes exact Euclidean distance.
func (p Point2D) Distance(other Point2D) float32 {
	return float32(math.Sqrt(float64(p.DistanceSq(other))))
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
	l := p.Distance(Point2D{0, 0})
	if l == 0 {
		return Point2D{X: 1, Y: 0}
	}
	return Point2D{X: p.X / l, Y: p.Y / l}
}
