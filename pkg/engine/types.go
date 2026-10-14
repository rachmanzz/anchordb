package engine

import "github.com/rachmanzz/anchordb/pkg/spatial"

// Role indices in RoleVector
const (
	RoleSubject   = 0
	RolePredicate = 1
	RoleObject    = 2
	RoleModifier  = 3
)

// RoleVector stores 4 semantic role values: [Subject, Predicate, Object, Modifier]
type RoleVector [4]float32

// Dot computes dot product between two role vectors
func (r RoleVector) Dot(other RoleVector) float32 {
	return r[0]*other[0] + r[1]*other[1] + r[2]*other[2] + r[3]*other[3]
}

// NodeID is a unique 64-bit integer identifier for a point
type NodeID uint64

// Node represents a word or conceptual point in the semantic manifold
type Node struct {
	ID       NodeID          `json:"id"`
	Word     string          `json:"word"`
	Coord    spatial.Point2D `json:"coord"`
	Roles    RoleVector      `json:"roles"`
	DomainID uint16          `json:"domain_id"`
	Freq     uint32          `json:"freq"`
}

// AnchorState represents the crystallized convergence point and its projected ray
type AnchorState struct {
	OriginIDs []NodeID        `json:"origin_ids"`
	Coord     spatial.Point2D `json:"coord"`
	Ray       spatial.Ray     `json:"ray"`
	Deficit   RoleVector      `json:"deficit"`
	DomainID  uint16          `json:"domain_id"`
}

// RayCandidate represents a point excited by the Anchor ray
type RayCandidate struct {
	Node       *Node   `json:"node"`
	T          float32 `json:"t"`            // Forward distance along the straight ray (Ordering Key)
	DistSqPerp float32 `json:"dist_sq_perp"` // Squared perpendicular deviation from the ray
	BeamEnergy float32 `json:"beam_energy"`  // Wave absorption energy
	RoleFit    float32 `json:"role_fit"`     // Deficit role fulfillment score
	FinalScore float32 `json:"final_score"`  // Combined rank score
}

// TrainingFact represents a structured relational fact
type TrainingFact struct {
	Inputs   []string `json:"inputs"`
	Relation string   `json:"relation"`
	Output   string   `json:"output"`
}
