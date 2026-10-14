package engine

import (
	"fmt"
	"slices"

	"github.com/rachmanzz/anchordb/pkg/spatial"
)

// HourglassEngine executes the single-step or cascading anchoring pipeline.
type HourglassEngine struct {
	Graph       *Graph
	BeamWidthSq float32 // Squared tolerance width for the ray beam
	Alpha       float32 // Weight multiplier for role deficit fulfillment
}

// NewHourglassEngine creates a new engine instance.
func NewHourglassEngine(g *Graph) *HourglassEngine {
	return &HourglassEngine{
		Graph:       g,
		BeamWidthSq: 0.05, // Tight beam tolerance
		Alpha:       1.5,  // Prioritize candidates fulfilling the role deficit
	}
}

// SynthesizeAnchor condenses multiple input nodes into 1 Anchor state.
func (e *HourglassEngine) SynthesizeAnchor(inputs []*Node, relation string) (*AnchorState, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("cannot synthesize anchor from empty input nodes")
	}

	var sumCoord spatial.Point2D
	var sumRoles RoleVector
	originIDs := make([]NodeID, len(inputs))
	count := float32(len(inputs))

	for i, node := range inputs {
		sumCoord = sumCoord.Add(node.Coord)
		for r := 0; r < 4; r++ {
			sumRoles[r] += node.Roles[r]
		}
		originIDs[i] = node.ID
	}

	// Barycenter of coordinates
	barycenter := sumCoord.Mul(1.0 / count)

	// Calculate Role Deficit: propositions require Subject + Predicate + Object
	targetEquilibrium := RoleVector{1.0, 1.0, 1.0, 0.0}
	var deficit RoleVector
	for r := 0; r < 4; r++ {
		d := targetEquilibrium[r] - sumRoles[r]
		if d < 0 {
			d = 0
		}
		deficit[r] = d
	}

	// Determine Ray Direction
	shift, hasRelation := e.Graph.GetRelation(relation)
	var rayDir spatial.Point2D
	if hasRelation {
		rayDir = shift.Normalize()
	} else {
		// Default direction: forward along negative Y (toward Object / result space)
		rayDir = spatial.Point2D{X: 0.0, Y: -1.0}
	}

	ray := spatial.NewRay(barycenter, rayDir)

	return &AnchorState{
		OriginIDs: originIDs,
		Coord:     barycenter,
		Ray:       ray,
		Deficit:   deficit,
		DomainID:  inputs[0].DomainID,
	}, nil
}

// ExpandPotentials casts the ray and detects points excited along its path.
func (e *HourglassEngine) ExpandPotentials(anchor *AnchorState) []RayCandidate {
	allNodes := e.Graph.GetAllNodes()
	var candidates []RayCandidate

	// Create set of origin IDs to avoid self-selection
	originSet := make(map[NodeID]bool, len(anchor.OriginIDs))
	for _, id := range anchor.OriginIDs {
		originSet[id] = true
	}

	for _, node := range allNodes {
		if originSet[node.ID] {
			continue // Skip input words
		}

		t, distSqPerp := anchor.Ray.Project(node.Coord)
		if t <= 0 {
			continue // Behind the ray
		}

		// Calculate beam absorption energy (Pythagoras free of math.Sqrt)
		beamEnergy := anchor.Ray.BeamEnergy(distSqPerp, e.BeamWidthSq)
		if beamEnergy <= 0.01 {
			continue // Outside the ray corridor
		}

		// Role fit: How well does this node fulfill the deficit?
		roleFit := node.Roles.Dot(anchor.Deficit)

		// Final score combining beam proximity and semantic role fit
		finalScore := beamEnergy * (1.0 + e.Alpha*roleFit)

		candidates = append(candidates, RayCandidate{
			Node:       node,
			T:          t,
			DistSqPerp: distSqPerp,
			BeamEnergy: beamEnergy,
			RoleFit:    roleFit,
			FinalScore: finalScore,
		})
	}

	return candidates
}

// OrderPotentials ranks candidates and selects the top K results.
func (e *HourglassEngine) OrderPotentials(candidates []RayCandidate, topK int) []*Node {
	if len(candidates) == 0 {
		return nil
	}

	// Sort primarily by FinalScore descending; for close scores, sort by T ascending (closer first)
	slices.SortFunc(candidates, func(a, b RayCandidate) int {
		if a.FinalScore > b.FinalScore {
			return -1
		}
		if a.FinalScore < b.FinalScore {
			return 1
		}
		if a.T < b.T {
			return -1
		}
		return 1
	})

	limit := topK
	if limit > len(candidates) {
		limit = len(candidates)
	}

	result := make([]*Node, limit)
	for i := 0; i < limit; i++ {
		result[i] = candidates[i].Node
	}
	return result
}

// Step executes one full cycle: Inputs -> Anchor -> Potentials -> Top K Ordered Results.
func (e *HourglassEngine) Step(inputWords []string, relation string, topK int) ([]*Node, *AnchorState, []RayCandidate, error) {
	inputNodes := make([]*Node, 0, len(inputWords))
	for _, word := range inputWords {
		node, exists := e.Graph.GetNodeByWord(word)
		if !exists {
			return nil, nil, nil, fmt.Errorf("unknown input word: %q", word)
		}
		inputNodes = append(inputNodes, node)
	}

	// 1. Fan-In: Multiple points -> 1 Anchor
	anchor, err := e.SynthesizeAnchor(inputNodes, relation)
	if err != nil {
		return nil, nil, nil, err
	}

	// 2. Fan-Out: 1 Anchor -> Many Potentials along straight ray
	potentials := e.ExpandPotentials(anchor)

	// 3. Ordering & Pruning: Many Potentials -> Few Ordered Nodes
	selected := e.OrderPotentials(potentials, topK)

	return selected, anchor, potentials, nil
}
