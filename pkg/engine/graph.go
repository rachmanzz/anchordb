package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

func hashStringToAngle(s string) float64 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = 31*h + uint32(s[i])
	}
	return float64(h%360) * math.Pi / 180.0
}

// Graph manages the points, relations, and lexicon in CPU RAM.
type Graph struct {
	mu        sync.RWMutex
	Nodes     map[NodeID]*Node
	Lexicon   map[string]NodeID
	Relations map[string]spatial.Point2D // Maps relation name -> shift vector T
	nextID    NodeID
}

// NewGraph creates an empty Graph.
func NewGraph() *Graph {
	return &Graph{
		Nodes:     make(map[NodeID]*Node),
		Lexicon:   make(map[string]NodeID),
		Relations: make(map[string]spatial.Point2D),
		nextID:    1,
	}
}

// AddNode registers or updates a point in the semantic graph.
func (g *Graph) AddNode(word string, coord spatial.Point2D, roles RoleVector, domainID uint16) *Node {
	g.mu.Lock()
	defer g.mu.Unlock()

	if id, exists := g.Lexicon[word]; exists {
		node := g.Nodes[id]
		node.Coord = coord
		node.Roles = roles
		node.DomainID = domainID
		node.Freq++
		return node
	}

	id := g.nextID
	g.nextID++

	node := &Node{
		ID:       id,
		Word:     word,
		Coord:    coord,
		Roles:    roles,
		DomainID: domainID,
		Freq:     1,
	}

	g.Nodes[id] = node
	g.Lexicon[word] = id
	return node
}

// GetNodeByWord retrieves a node by its string label.
func (g *Graph) GetNodeByWord(word string) (*Node, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	id, exists := g.Lexicon[word]
	if !exists {
		return nil, false
	}
	return g.Nodes[id], true
}

// GetAllNodes returns a slice of all nodes currently in the graph.
func (g *Graph) GetAllNodes() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]*Node, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// RegisterRelation registers a directional displacement vector T for a relation.
func (g *Graph) RegisterRelation(relation string, shift spatial.Point2D) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Relations[relation] = shift
}

// GetRelation retrieves the vector shift T for a relation.
func (g *Graph) GetRelation(relation string) (spatial.Point2D, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	shift, exists := g.Relations[relation]
	return shift, exists
}

// LearnFact registers a structured fact in real-time.
func (g *Graph) LearnFact(fact TrainingFact) {
	// If relation shift is known, we can position output relative to inputs
	var sum spatial.Point2D
	count := float32(len(fact.Inputs))
	if count == 0 {
		return
	}

	// Ensure inputs exist
	for _, word := range fact.Inputs {
		if _, exists := g.GetNodeByWord(word); !exists {
			// Initialize unique coordinates based on word hash to avoid stacking at identical points
			angle := hashStringToAngle(word)
			coord := spatial.Point2D{
				X: float32(math.Cos(angle)) * 0.6,
				Y: float32(math.Sin(angle)) * 0.6,
			}
			g.AddNode(word, coord, RoleVector{0.8, 0.1, 0.2, 0.1}, 1)
		}
		node, _ := g.GetNodeByWord(word)
		sum = sum.Add(node.Coord)
	}

	barycenter := sum.Mul(1.0 / count)
	shift, hasShift := g.GetRelation(fact.Relation)
	if !hasShift {
		// Generate unique directional angle for dynamic relation
		angle := hashStringToAngle(fact.Relation)
		shift = spatial.Point2D{
			X: float32(math.Cos(angle)) * 0.7,
			Y: float32(math.Sin(angle)) * 0.7,
		}
		g.RegisterRelation(fact.Relation, shift)
	}

	// Output position along the straight line from barycenter
	outputCoord := barycenter.Add(shift)

	// Register output node as strong Object role
	g.AddNode(fact.Output, outputCoord, RoleVector{0.2, 0.1, 0.95, 0.2}, 1)
}

// LoadStructuredFacts reads a JSON file of TrainingFact items and registers them.
func (g *Graph) LoadStructuredFacts(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read facts file: %w", err)
	}

	var facts []TrainingFact
	if err := json.Unmarshal(data, &facts); err != nil {
		return fmt.Errorf("failed to parse JSON facts: %w", err)
	}

	// Calibrate standard relations
	g.RegisterRelation("keturunan", spatial.Point2D{X: 0.0, Y: -0.8})
	g.RegisterRelation("pertumbuhan", spatial.Point2D{X: 0.5, Y: 0.0})
	g.RegisterRelation("edukasi", spatial.Point2D{X: 0.0, Y: -0.6})
	g.RegisterRelation("kausalitas", spatial.Point2D{X: 0.4, Y: -0.4})
	g.RegisterRelation("vokalisasi", spatial.Point2D{X: -0.5, Y: 0.0})
	g.RegisterRelation("perubahan_fase", spatial.Point2D{X: 0.0, Y: -0.7})
	g.RegisterRelation("pembakaran", spatial.Point2D{X: 0.6, Y: -0.6})

	// Calibrate initial key nodes for demonstration
	g.AddNode("ibu", spatial.Point2D{X: 0.2, Y: 0.9}, RoleVector{0.9, 0.1, 0.8, 0.2}, 1)
	g.AddNode("ayah", spatial.Point2D{X: 0.2, Y: 0.9}, RoleVector{0.9, 0.1, 0.8, 0.2}, 1)

	for _, fact := range facts {
		g.LearnFact(fact)
	}

	return nil
}
