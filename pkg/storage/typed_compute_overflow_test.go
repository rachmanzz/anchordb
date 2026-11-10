package storage

import (
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestCombineIntegerArithmeticTyped_Boundaries(t *testing.T) {
	i := func(v int64) AttrEntry { return NewInt64Attr("a", v) }
	u := func(v uint64) AttrEntry { return NewUint64Attr("a", v) }

	tests := []struct {
		name     string
		op       byte
		a        AttrEntry
		litA     bool
		b        AttrEntry
		litB     bool
		wantErr  bool
		wantType AttrType
		wantI    int64
		wantU    uint64
	}{
		{name: "MaxInt64+1", op: '+', a: i(math.MaxInt64), b: i(1), litB: true, wantErr: true},
		{name: "MinInt64-1", op: '-', a: i(math.MinInt64), b: i(1), litB: true, wantErr: true},
		{name: "MaxInt64*2", op: '*', a: i(math.MaxInt64), b: i(2), litB: true, wantErr: true},
		{name: "MinInt64*-1", op: '*', a: i(math.MinInt64), b: i(-1), litB: true, wantErr: true},
		{name: "int64 above 2^53 exact", op: '+', a: i(9007199254740993), b: i(2), litB: true, wantType: AttrTypeInt64, wantI: 9007199254740995},
		{name: "int64 signed multiply", op: '*', a: i(-3037000499), b: i(3037000499), wantType: AttrTypeInt64, wantI: -9223372030926249001},
		{name: "MaxUint64+1", op: '+', a: u(math.MaxUint64), b: i(1), litB: true, wantErr: true},
		{name: "MaxUint64*2", op: '*', a: u(math.MaxUint64), b: i(2), litB: true, wantErr: true},
		{name: "uint64 underflow to int64", op: '-', a: u(5), b: i(7), litB: true, wantType: AttrTypeInt64, wantI: -2},
		{name: "uint64 underflow out of range", op: '-', a: u(0), b: u(math.MaxUint64), wantErr: true},
		{name: "uint64 plus negative literal", op: '+', a: u(10), b: i(-3), litB: true, wantType: AttrTypeUint64, wantU: 7},
		{name: "uint64 above MaxInt64 minus negative", op: '-', a: u(math.MaxUint64 - 1), b: i(-1), litB: true, wantType: AttrTypeUint64, wantU: math.MaxUint64},
		{name: "uint64 minus negative overflow", op: '-', a: u(math.MaxUint64), b: i(-1), litB: true, wantErr: true},
		{name: "int64 field plus large uint64 literal overflow", op: '+', a: i(1), b: u(math.MaxUint64), litB: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, handled, err := combineIntegerArithmeticTyped("out", tt.op, tt.a, tt.litA, tt.b, tt.litB)
			if !handled {
				t.Fatalf("expected integer operands to be handled")
			}
			if tt.wantErr {
				if !errors.Is(err, ErrIntegerOverflow) {
					t.Fatalf("expected ErrIntegerOverflow, got attr=%+v err=%v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Type != tt.wantType {
				t.Fatalf("expected type %v, got %v", tt.wantType, got.Type)
			}
			if tt.wantType == AttrTypeInt64 && got.Int64() != tt.wantI {
				t.Fatalf("expected %d, got %d", tt.wantI, got.Int64())
			}
			if tt.wantType == AttrTypeUint64 && got.Uint64() != tt.wantU {
				t.Fatalf("expected %d, got %d", tt.wantU, got.Uint64())
			}
		})
	}
}

func openTypedComputeDB(t *testing.T) *AnchorDB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "typed_compute.anchordb"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.SaveRecordTyped("rec", []AttrEntry{
		NewInt64Attr("big", math.MaxInt64),
		NewInt64Attr("min", math.MinInt64),
		NewFloat32Attr("x", 10),
	})
	if err != nil {
		t.Fatalf("SaveRecordTyped failed: %v", err)
	}
	return db
}

func TestTypedCompute_OverflowReturnsError(t *testing.T) {
	db := openTypedComputeDB(t)

	cases := map[string]ComputeOp{
		"add":          OpAdd{FieldA: "big", FieldB: "1"},
		"multiply":     OpMultiply{FieldA: "big", FieldB: "2"},
		"subtract":     OpSubtract{FieldA: "min", FieldB: "1"},
		"addDays":      OpAddDays{Field: "big", Days: 1},
		"subtractDays": OpSubtractDays{Field: "min", Days: 1},
	}
	for name, op := range cases {
		t.Run("Get/"+name, func(t *testing.T) {
			_, err := db.GetRecordTypedWithCompute("rec", map[string]ComputeOp{"out": op})
			if !errors.Is(err, ErrIntegerOverflow) {
				t.Fatalf("expected ErrIntegerOverflow, got %v", err)
			}
		})
	}

	t.Run("Update", func(t *testing.T) {
		_, err := db.UpdateRecords(UpdateOptions{
			TargetEntities: []string{"rec"},
			Mutations:      map[string]ComputeOp{"big": OpAdd{FieldA: "big", FieldB: "1"}},
		})
		if !errors.Is(err, ErrIntegerOverflow) {
			t.Fatalf("expected ErrIntegerOverflow, got %v", err)
		}
		cr, err := db.GetRecordTyped("rec")
		if err != nil {
			t.Fatalf("GetRecordTyped failed: %v", err)
		}
		if a, _ := cr.GetTyped("big"); a.Int64() != math.MaxInt64 {
			t.Fatalf("expected big to stay MaxInt64 after failed update, got %d", a.Int64())
		}
	})

	t.Run("List", func(t *testing.T) {
		_, _, err := db.ListRecordsWithPaging(QueryOptions{
			Computations: map[string]ComputeOp{"out": OpAdd{FieldA: "big", FieldB: "1"}},
		})
		if !errors.Is(err, ErrIntegerOverflow) {
			t.Fatalf("expected ErrIntegerOverflow from List, got %v", err)
		}
		if err == nil || !strings.Contains(err.Error(), `"rec"`) {
			t.Fatalf("expected List error to identify record rec, got %v", err)
		}
	})
}

func TestTypedCompute_NotFoundSentinel(t *testing.T) {
	db := openTypedComputeDB(t)
	if _, err := db.GetRecordTyped("missing"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound from GetRecordTyped, got %v", err)
	}
	if _, err := db.GetRecordTypedWithCompute("missing", map[string]ComputeOp{"out": OpAdd{FieldA: "x", FieldB: "1"}}); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound from GetRecordTypedWithCompute, got %v", err)
	}
	if _, _, err := db.GetRecordWithOperation("missing"); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound from GetRecordWithOperation, got %v", err)
	}
}

func TestListRejectsAfterClose(t *testing.T) {
	db := openTypedComputeDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, _, err := db.ListRecordsWithPaging(QueryOptions{}); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed from List after Close, got %v", err)
	}
	if _, _, err := db.ListRecordsWithPaging(QueryOptions{Computations: map[string]ComputeOp{"out": plusOneOp{field: "x"}}}); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed from List+compute after Close, got %v", err)
	}
	if _, err := db.CalculateDistance("a", "b"); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed from CalculateDistance after Close, got %v", err)
	}
	if _, err := db.ExecuteGraphChainWithContext(t.Context(), []string{"a"}, []string{"r"}, 5, 1.0); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed from ExecuteGraphChain after Close, got %v", err)
	}
}

type plusOneOp struct{ field string }

func (o plusOneOp) Apply(props map[string]float32) (float32, error) {
	return props[o.field] + 1, nil
}

func TestTypedCompute_CustomOpReceivesRecordAttributes(t *testing.T) {
	db := openTypedComputeDB(t)
	op := plusOneOp{field: "x"}

	legacy, err := db.GetRecordWithCompute("rec", map[string]ComputeOp{"out": op})
	if err != nil {
		t.Fatalf("GetRecordWithCompute failed: %v", err)
	}
	if legacy["out"] != 11 {
		t.Fatalf("expected legacy out=11, got %v", legacy["out"])
	}

	cr, err := db.GetRecordTypedWithCompute("rec", map[string]ComputeOp{"out": op})
	if err != nil {
		t.Fatalf("GetRecordTypedWithCompute failed: %v", err)
	}
	if a, ok := cr.GetTyped("out"); !ok || a.Float32() != 11 {
		t.Fatalf("expected typed Get out=11, got %+v (ok=%v)", a, ok)
	}

	recs, _, err := db.ListRecordsWithPaging(QueryOptions{Computations: map[string]ComputeOp{"out": op}})
	if err != nil {
		t.Fatalf("ListRecordsWithPaging failed: %v", err)
	}
	found := false
	for _, r := range recs {
		for _, a := range r.Attrs {
			if a.Key == "out" {
				found = true
				if a.Float32() != 11 {
					t.Fatalf("expected List out=11, got %v", a.Float32())
				}
			}
		}
	}
	if !found {
		t.Fatalf("expected List result to contain computed field out")
	}

	if _, err := db.UpdateRecords(UpdateOptions{
		TargetEntities: []string{"rec"},
		Mutations:      map[string]ComputeOp{"x": op},
	}); err != nil {
		t.Fatalf("UpdateRecords failed: %v", err)
	}
	updated, err := db.GetRecordTyped("rec")
	if err != nil {
		t.Fatalf("GetRecordTyped failed: %v", err)
	}
	if a, _ := updated.GetTyped("x"); a.Float32() != 11 {
		t.Fatalf("expected updated x=11, got %v", a.Float32())
	}
}
