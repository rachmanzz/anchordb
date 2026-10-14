package spatial

import "math"

// Ray represents a straight directional line projected from an Anchor origin.
type Ray struct {
	Origin    Point2D `json:"origin"`
	Direction Point2D `json:"direction"` // Should be a normalized unit vector
}

// NewRay creates a new Ray with guaranteed normalized direction.
func NewRay(origin, direction Point2D) Ray {
	return Ray{
		Origin:    origin,
		Direction: direction.Normalize(),
	}
}

// Project projects a target point onto the ray.
// Returns:
// - t: forward distance along the ray (t > 0 is in front of the ray).
// - distSqPerp: squared perpendicular deviation from the ray line (using Pythagorean theorem, 100% free of math.Sqrt).
func (r Ray) Project(target Point2D) (t float32, distSqPerp float32) {
	dx := target.X - r.Origin.X
	dy := target.Y - r.Origin.Y

	// Dot product gives the forward projection distance along the unit direction
	t = dx*r.Direction.X + dy*r.Direction.Y
	if t <= 0 {
		return 0, math.MaxFloat32 // Point is behind the ray or at the origin
	}

	// Total squared Euclidean distance from ray origin to target
	totalDistSq := dx*dx + dy*dy

	// Pythagorean theorem: d_perp^2 = totalDistSq - t^2
	distSqPerp = totalDistSq - (t * t)
	if distSqPerp < 0 {
		distSqPerp = 0 // Guard against float precision inaccuracies
	}

	return t, distSqPerp
}

// BeamEnergy calculates the absorption / excitation energy of a point given its perpendicular distance squared.
// Uses fast Gaussian decay: exp(-distSqPerp / (2 * beamWidthSq)).
func (r Ray) BeamEnergy(distSqPerp float32, beamWidthSq float32) float32 {
	if distSqPerp > beamWidthSq*4 { // Quick pruning threshold for zero energy
		return 0
	}
	twoSigmaSq := 2 * beamWidthSq
	return float32(math.Exp(float64(-distSqPerp / twoSigmaSq)))
}
