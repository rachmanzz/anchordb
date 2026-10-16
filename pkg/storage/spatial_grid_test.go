package storage

import (
	"math"
	"testing"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func TestPointToCell_TableDrivenBoundaries(t *testing.T) {
	nan := float32(math.NaN())
	posInf := float32(math.Inf(1))
	negInf := float32(math.Inf(-1))

	tests := []struct {
		name  string
		point spatial.Point2D
		wantX int
		wantY int
	}{
		{"Bottom-left corner", spatial.Point2D{X: -1.0, Y: -1.0}, 0, 0},
		{"Center", spatial.Point2D{X: 0.0, Y: 0.0}, 127, 127},
		{"Top-right corner", spatial.Point2D{X: 1.0, Y: 1.0}, 255, 255},
		{"Far negative clamps to 0", spatial.Point2D{X: -99.0, Y: -50.0}, 0, 0},
		{"Far positive clamps to 255", spatial.Point2D{X: 100.0, Y: 200.0}, 255, 255},
		{"NaN coordinates map defensively to 0", spatial.Point2D{X: nan, Y: nan}, 0, 0},
		{"Mixed NaN and positive", spatial.Point2D{X: nan, Y: 1.0}, 0, 255},
		{"+Inf clamps to 255", spatial.Point2D{X: posInf, Y: posInf}, 255, 255},
		{"-Inf clamps to 0", spatial.Point2D{X: negInf, Y: negInf}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell := PointToCell(tt.point, GridDim)
			if cell.X < 0 || cell.X >= GridDim || cell.Y < 0 || cell.Y >= GridDim {
				t.Fatalf("PointToCell(%+v) produced out-of-bounds cell %+v (GridDim=%d)", tt.point, cell, GridDim)
			}
			if cell.X != tt.wantX || cell.Y != tt.wantY {
				t.Errorf("PointToCell(%+v) = %+v, want {X:%d, Y:%d}", tt.point, cell, tt.wantX, tt.wantY)
			}
		})
	}
}

func TestCellIndex_Calculation(t *testing.T) {
	tests := []struct {
		coord CellCoord
		dim   int
		want  int
	}{
		{CellCoord{X: 0, Y: 0}, 256, 0},
		{CellCoord{X: 10, Y: 0}, 256, 10},
		{CellCoord{X: 0, Y: 1}, 256, 256},
		{CellCoord{X: 255, Y: 255}, 256, 255*256 + 255},
	}

	for _, tt := range tests {
		got := CellIndex(tt.coord, tt.dim)
		if got != tt.want {
			t.Errorf("CellIndex(%+v, %d) = %d, want %d", tt.coord, tt.dim, got, tt.want)
		}
	}
}

func TestTraverseRayDDA_VisitsCellsAndEarlyTerminates(t *testing.T) {
	// Ray from center (0,0) shooting right (+X)
	ray := spatial.Ray{
		Origin:    spatial.Point2D{X: 0.0, Y: 0.0},
		Direction: spatial.Point2D{X: 1.0, Y: 0.0},
	}

	var visited []CellCoord
	TraverseRayDDA(ray, GridDim, 0.5, func(c CellCoord) bool {
		visited = append(visited, c)
		return true // continue
	})

	if len(visited) == 0 {
		t.Fatal("expected DDA to visit cells along ray, got 0")
	}

	// First cell must be origin cell
	first := visited[0]
	originCell := PointToCell(ray.Origin, GridDim)
	if first != originCell {
		t.Errorf("first cell = %+v, want origin cell %+v", first, originCell)
	}

	// Y coordinate should stay constant along pure +X ray
	for _, c := range visited {
		if c.Y != originCell.Y {
			t.Errorf("unexpected Y divergence along X ray: got cell %+v, origin Y %d", c, originCell.Y)
		}
	}

	// Test early termination after 3 cells
	var count int
	TraverseRayDDA(ray, GridDim, 0.5, func(c CellCoord) bool {
		count++
		return count < 3 // stop when count reaches 3
	})
	if count != 3 {
		t.Errorf("early termination failed: visited %d cells, want 3", count)
	}
}
