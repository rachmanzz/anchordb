package driver

import (
	"context"
	"fmt"
	"math"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// GraphResponse holds results returned to the client application
type GraphResponse struct {
	Anchor  spatial.Point2D            `json:"anchor"`
	Results []storage.GraphQueryResult `json:"results"`
}

// ChainResponse holds results of multi-hop chain reasoning
type ChainResponse struct {
	Sequence []string                   `json:"sequence"`
	Steps    []storage.GraphQueryResult `json:"steps"`
}

// GraphBuilder provides a fluent builder interface for graph queries
type GraphBuilder struct {
	backend    backend
	model      string
	inputs     []string
	relation   string
	relations  []string // for chaining
	beamWidth  float32
	limitK     int
	orderMode  storage.OrderByMode
	isChaining bool
}

// NewGraphBuilder initializes a new builder from local storage (for backward compatibility).
func NewGraphBuilder(db *storage.AnchorDB) *GraphBuilder {
	return &GraphBuilder{
		backend:   &localBackend{db: db},
		model:     "default",
		beamWidth: 0.05,
		limitK:    10,
		orderMode: storage.OrderByCompositeDesc,
	}
}

func newGraphBuilder(b backend, model string) *GraphBuilder {
	return &GraphBuilder{
		backend:   b,
		model:     model,
		beamWidth: 0.05,
		limitK:    10,
		orderMode: storage.OrderByCompositeDesc,
	}
}

// From sets input concepts
func (b *GraphBuilder) From(words ...string) *GraphBuilder {
	b.inputs = append(b.inputs, words...)
	return b
}

// Relate sets a single relation type
func (b *GraphBuilder) Relate(relation string) *GraphBuilder {
	b.relation = relation
	return b
}

// OrderBy sets custom ordering strategy (OrderByCompositeDesc, OrderByDistanceAsc, OrderByScoreDesc)
func (b *GraphBuilder) OrderBy(mode storage.OrderByMode) *GraphBuilder {
	b.orderMode = mode
	return b
}

// Chain sets a sequence of relations for multi-hop reasoning
func (b *GraphBuilder) Chain(relations ...string) *GraphBuilder {
	b.relations = append(b.relations, relations...)
	b.isChaining = true
	return b
}

// BeamWidth sets the straight line ray beam width
func (b *GraphBuilder) BeamWidth(width float32) *GraphBuilder {
	b.beamWidth = width
	return b
}

// TopK sets maximum results to return
func (b *GraphBuilder) TopK(k int) *GraphBuilder {
	b.limitK = k
	return b
}

// Execute triggers in-storage numerical compute
func (b *GraphBuilder) Execute(ctx context.Context) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(b.inputs) == 0 {
		return nil, fmt.Errorf("no inputs specified in query")
	}
	if b.limitK < 0 {
		return nil, fmt.Errorf("TopK must not be negative")
	}
	if b.beamWidth <= 0 || math.IsNaN(float64(b.beamWidth)) || math.IsInf(float64(b.beamWidth), 0) {
		return nil, fmt.Errorf("BeamWidth must be finite and greater than zero")
	}

	return b.backend.Graph(ctx, b.model, b.inputs, b.relation, b.limitK, b.orderMode, b.beamWidth)
}

// ExecuteChain executes multi-hop reasoning directly inside AnchorDB
func (b *GraphBuilder) ExecuteChain(ctx context.Context) (*ChainResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(b.inputs) == 0 {
		return nil, fmt.Errorf("no inputs specified for chain")
	}
	if len(b.relations) == 0 {
		return nil, fmt.Errorf("no relations specified for chain")
	}
	if b.limitK < 0 {
		return nil, fmt.Errorf("TopK must not be negative")
	}
	if b.beamWidth <= 0 || math.IsNaN(float64(b.beamWidth)) || math.IsInf(float64(b.beamWidth), 0) {
		return nil, fmt.Errorf("BeamWidth must be finite and greater than zero")
	}

	return b.backend.GraphChain(ctx, b.model, b.inputs, b.relations, b.limitK, b.beamWidth)
}
