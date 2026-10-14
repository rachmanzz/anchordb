package engine

import (
	"path/filepath"
	"testing"
)

func setupTestGraph(t *testing.T) *Graph {
	g := NewGraph()
	dataPath := filepath.Join("..", "..", "data", "seed_facts.json")
	if err := g.LoadStructuredFacts(dataPath); err != nil {
		t.Fatalf("failed to load seed facts: %v", err)
	}
	return g
}

func TestHourglass_IbuAyahAnak(t *testing.T) {
	g := setupTestGraph(t)
	engine := NewHourglassEngine(g)

	inputs := []string{"ibu", "ayah"}
	selected, anchor, candidates, err := engine.Step(inputs, "keturunan", 1)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	if len(selected) == 0 {
		t.Fatalf("expected at least 1 candidate, got 0")
	}

	topWord := selected[0].Word
	if topWord != "anak" {
		t.Fatalf("expected top result 'anak', got %q", topWord)
	}

	t.Logf("Anchor Origin: %v, Ray: %v", anchor.Coord, anchor.Ray.Direction)
	t.Logf("Found %d candidates. Top-1: %s (Score: %f, t: %f)",
		len(candidates), topWord, candidates[0].FinalScore, candidates[0].T)
}

func TestHourglass_MultiRelation_AirDingin(t *testing.T) {
	g := setupTestGraph(t)
	engine := NewHourglassEngine(g)

	inputs := []string{"air", "dingin"}
	selected, _, _, err := engine.Step(inputs, "perubahan_fase", 1)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	if len(selected) == 0 || selected[0].Word != "es" {
		t.Fatalf("expected top result 'es', got %+v", selected)
	}
}

func TestHourglass_RealTimeLearning(t *testing.T) {
	g := setupTestGraph(t)
	engine := NewHourglassEngine(g)

	// Teach a new fact on the fly: biji + tanah -> pohon
	newFact := TrainingFact{
		Inputs:   []string{"biji", "tanah"},
		Relation: "pertumbuhan",
		Output:   "pohon",
	}

	g.LearnFact(newFact)

	// Query immediately
	selected, _, _, err := engine.Step([]string{"biji", "tanah"}, "pertumbuhan", 1)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(selected) == 0 || selected[0].Word != "pohon" {
		t.Fatalf("expected live-learned result 'pohon', got %+v", selected)
	}
}

func BenchmarkHourglassStep(b *testing.B) {
	g := NewGraph()
	_ = g.LoadStructuredFacts(filepath.Join("..", "..", "data", "seed_facts.json"))
	engine := NewHourglassEngine(g)
	inputs := []string{"ibu", "ayah"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, _ = engine.Step(inputs, "keturunan", 1)
	}
}
