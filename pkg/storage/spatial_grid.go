package storage

import (
	"math"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// CellCoord represents an integer coordinate (0..255) in the spatial grid
type CellCoord struct {
	X int
	Y int
}

// PointToCell converts a continuous Point2D (-1.0 .. 1.0) into discrete grid cell (0..255).
func PointToCell(p spatial.Point2D, dim int) CellCoord {
	normX := (p.X + 1.0) * 0.5
	normY := (p.Y + 1.0) * 0.5

	if normX < 0 {
		normX = 0
	} else if normX > 1.0 {
		normX = 1.0
	}

	if normY < 0 {
		normY = 0
	} else if normY > 1.0 {
		normY = 1.0
	}

	cX := int(normX * float32(dim-1))
	cY := int(normY * float32(dim-1))

	return CellCoord{X: cX, Y: cY}
}

// CellIndex returns linear array index for (x, y) cell
func CellIndex(c CellCoord, dim int) int {
	return c.Y*dim + c.X
}

// DDAVisitor is a callback function called for each grid cell visited by the ray.
// If the visitor returns true, traversal continues; if false, traversal STOPS (Early Termination).
type DDAVisitor func(cell CellCoord) bool

// TraverseRayDDA steps through grid cells along a straight ray using the Fast Voxel DDA algorithm.
// Yields cells in strict order of increasing distance t!
func TraverseRayDDA(ray spatial.Ray, dim int, maxDistance float32, visitor DDAVisitor) {
	// Start cell
	curr := PointToCell(ray.Origin, dim)

	// Step direction (-1, 0, or 1)
	var stepX, stepY int
	var tDeltaX, tDeltaY float32
	var tMaxX, tMaxY float32

	cellSize := 2.0 / float32(dim) // Coordinate length of one cell in [-1, 1] range

	if ray.Direction.X > 0 {
		stepX = 1
		nextCellBoundaryX := -1.0 + float32(curr.X+1)*cellSize
		tMaxX = (nextCellBoundaryX - ray.Origin.X) / ray.Direction.X
		tDeltaX = cellSize / ray.Direction.X
	} else if ray.Direction.X < 0 {
		stepX = -1
		nextCellBoundaryX := -1.0 + float32(curr.X)*cellSize
		tMaxX = (nextCellBoundaryX - ray.Origin.X) / ray.Direction.X
		tDeltaX = cellSize / -ray.Direction.X
	} else {
		stepX = 0
		tMaxX = math.MaxFloat32
		tDeltaX = math.MaxFloat32
	}

	if ray.Direction.Y > 0 {
		stepY = 1
		nextCellBoundaryY := -1.0 + float32(curr.Y+1)*cellSize
		tMaxY = (nextCellBoundaryY - ray.Origin.Y) / ray.Direction.Y
		tDeltaY = cellSize / ray.Direction.Y
	} else if ray.Direction.Y < 0 {
		stepY = -1
		nextCellBoundaryY := -1.0 + float32(curr.Y)*cellSize
		tMaxY = (nextCellBoundaryY - ray.Origin.Y) / ray.Direction.Y
		tDeltaY = cellSize / -ray.Direction.Y
	} else {
		stepY = 0
		tMaxY = math.MaxFloat32
		tDeltaY = math.MaxFloat32
	}

	// Always visit initial origin cell first
	if !visitor(curr) {
		return
	}

	totalT := float32(0.0)

	// Step through cells
	for {
		if tMaxX < tMaxY {
			totalT = tMaxX
			tMaxX += tDeltaX
			curr.X += stepX
		} else {
			totalT = tMaxY
			tMaxY += tDeltaY
			curr.Y += stepY
		}

		// Boundary check
		if curr.X < 0 || curr.X >= dim || curr.Y < 0 || curr.Y >= dim {
			break
		}

		if totalT > maxDistance {
			break
		}

		// Visit cell, early terminate if visitor returns false!
		if !visitor(curr) {
			break
		}
	}
}
