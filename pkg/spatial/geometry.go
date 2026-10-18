package spatial

import (
	"errors"
	"math"
)

var (
	// ErrPolygonTooFewVertices is returned when constructing a Polygon2D with fewer than 3 vertices.
	ErrPolygonTooFewVertices = errors.New("polygon must have at least 3 vertices")
)

// Box2D represents an Axis-Aligned Bounding Box (AABB) in 2D space.
type Box2D struct {
	Min Point2D `json:"min"`
	Max Point2D `json:"max"`
}

// NewBox2D creates a normalized Box2D ensuring Min <= Max on both X and Y axes.
func NewBox2D(p1, p2 Point2D) Box2D {
	minX := p1.X
	maxX := p2.X
	if minX > maxX {
		minX, maxX = maxX, minX
	}
	minY := p1.Y
	maxY := p2.Y
	if minY > maxY {
		minY, maxY = maxY, minY
	}
	return Box2D{
		Min: Point2D{X: minX, Y: minY},
		Max: Point2D{X: maxX, Y: maxY},
	}
}

// Contains checks if point p lies inside or on the boundaries of the bounding box.
func (b Box2D) Contains(p Point2D) bool {
	return p.X >= b.Min.X && p.X <= b.Max.X && p.Y >= b.Min.Y && p.Y <= b.Max.Y
}

// Intersects checks if two bounding boxes overlap.
func (b Box2D) Intersects(other Box2D) bool {
	return b.Min.X <= other.Max.X && b.Max.X >= other.Min.X &&
		b.Min.Y <= other.Max.Y && b.Max.Y >= other.Min.Y
}

// Polygon2D represents a closed 2D polygon with a precomputed bounding box for fast broad-phase pruning.
type Polygon2D struct {
	Vertices []Point2D `json:"vertices"`
	BBox     Box2D     `json:"bbox"`
}

// NewPolygon2D constructs a Polygon2D with automatic bounding box calculation.
// Requires at least 3 distinct vertices.
func NewPolygon2D(vertices []Point2D) (Polygon2D, error) {
	if len(vertices) < 3 {
		return Polygon2D{}, ErrPolygonTooFewVertices
	}

	minX := vertices[0].X
	maxX := vertices[0].X
	minY := vertices[0].Y
	maxY := vertices[0].Y

	for i := 1; i < len(vertices); i++ {
		v := vertices[i]
		if v.X < minX {
			minX = v.X
		}
		if v.X > maxX {
			maxX = v.X
		}
		if v.Y < minY {
			minY = v.Y
		}
		if v.Y > maxY {
			maxY = v.Y
		}
	}

	// Make a defensive copy of vertices
	vertsCopy := make([]Point2D, len(vertices))
	copy(vertsCopy, vertices)

	return Polygon2D{
		Vertices: vertsCopy,
		BBox: Box2D{
			Min: Point2D{X: minX, Y: minY},
			Max: Point2D{X: maxX, Y: maxY},
		},
	}, nil
}

// Contains tests if point p lies within the polygon using the Ray-Casting algorithm (Even-Odd rule).
// Performs an O(1) broad-phase check against the polygon's bounding box before executing the ray-casting loop.
func (poly Polygon2D) Contains(p Point2D) bool {
	// 1. Broad-phase filter
	if !poly.BBox.Contains(p) {
		return false
	}

	// 2. Narrow-phase ray casting (Jordan curve theorem)
	n := len(poly.Vertices)
	inside := false

	j := n - 1
	for i := 0; i < n; i++ {
		vi := poly.Vertices[i]
		vj := poly.Vertices[j]

		// Check if ray cast horizontally from p to the right intersects edge (vi, vj)
		if ((vi.Y > p.Y) != (vj.Y > p.Y)) &&
			(p.X < (vj.X-vi.X)*(p.Y-vi.Y)/(vj.Y-vi.Y)+vi.X) {
			inside = !inside
		}
		j = i
	}

	return inside
}

// Area calculates the planar polygon area using the Shoelace formula.
func (poly Polygon2D) Area() float32 {
	n := len(poly.Vertices)
	if n < 3 {
		return 0
	}
	var area float64
	j := n - 1
	for i := 0; i < n; i++ {
		vi := poly.Vertices[i]
		vj := poly.Vertices[j]
		area += float64(vj.X+vi.X) * float64(vj.Y-vi.Y)
		j = i
	}
	if area < 0 {
		area = -area
	}
	return float32(area * 0.5)
}

// Perimeter calculates the total boundary perimeter length.
func (poly Polygon2D) Perimeter() float32 {
	n := len(poly.Vertices)
	if n < 2 {
		return 0
	}
	var perim float32
	j := n - 1
	for i := 0; i < n; i++ {
		perim += poly.Vertices[i].Distance(poly.Vertices[j])
		j = i
	}
	return perim
}

// WithinRadius tests if point p is within a circular radius from center.
// Eliminates math.Sqrt by comparing squared Euclidean distances.
func WithinRadius(p, center Point2D, radius float32) bool {
	if radius <= 0 {
		return false
	}
	return p.DistanceSq(center) <= radius*radius
}

// EarthRadiusKM is the standard spherical Earth mean radius in kilometers (WGS84 spherical approx).
const EarthRadiusKM = 6371.0

// HaversineDistanceKM computes the great-circle distance between two GPS coordinates (p.X = Longitude, p.Y = Latitude in degrees).
// Returns distance in kilometers.
func HaversineDistanceKM(p1, p2 Point2D) float32 {
	lat1 := float64(p1.Y) * math.Pi / 180.0
	lon1 := float64(p1.X) * math.Pi / 180.0
	lat2 := float64(p2.Y) * math.Pi / 180.0
	lon2 := float64(p2.X) * math.Pi / 180.0

	dLat := lat2 - lat1
	dLon := lon2 - lon1

	sinHalfLat := math.Sin(dLat / 2.0)
	sinHalfLon := math.Sin(dLon / 2.0)

	a := sinHalfLat*sinHalfLat + math.Cos(lat1)*math.Cos(lat2)*sinHalfLon*sinHalfLon
	if a > 1.0 {
		a = 1.0
	}
	c := 2.0 * math.Atan2(math.Sqrt(a), math.Sqrt(1.0-a))

	return float32(EarthRadiusKM * c)
}

// InitialBearing computes the initial forward compass azimuth (bearing) from p1 to p2 in degrees (0°..360°).
// Coordinates must be in degrees (p.X = Longitude, p.Y = Latitude).
func InitialBearing(from, to Point2D) float32 {
	lat1 := float64(from.Y) * math.Pi / 180.0
	lon1 := float64(from.X) * math.Pi / 180.0
	lat2 := float64(to.Y) * math.Pi / 180.0
	lon2 := float64(to.X) * math.Pi / 180.0

	dLon := lon2 - lon1

	y := math.Sin(dLon) * math.Cos(lat2)
	x := math.Cos(lat1)*math.Sin(lat2) - math.Sin(lat1)*math.Cos(lat2)*math.Cos(dLon)

	bearingRad := math.Atan2(y, x)
	bearingDeg := bearingRad * 180.0 / math.Pi

	bearing := float32(math.Mod(bearingDeg+360.0, 360.0))
	return bearing
}
