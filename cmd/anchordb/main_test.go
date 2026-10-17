package main

import (
	"reflect"
	"testing"
)

func TestParseTraversalArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		want      []string
		wantK     int
		wantError bool
	}{
		{name: "default", args: []string{"seed", "causes"}, want: []string{"seed", "causes"}, wantK: 5},
		{name: "interleaved flag", args: []string{"seed", "-k", "3", "causes"}, want: []string{"seed", "causes"}, wantK: 3},
		{name: "equals flag", args: []string{"seed", "causes", "--k=7"}, want: []string{"seed", "causes"}, wantK: 7},
		{name: "zero", args: []string{"seed", "causes", "-k", "0"}, wantError: true},
		{name: "not numeric", args: []string{"seed", "causes", "-k", "many"}, wantError: true},
		{name: "missing value", args: []string{"seed", "causes", "-k"}, wantError: true},
		{name: "unknown flag", args: []string{"seed", "causes", "-unknown"}, wantError: true},
		{name: "extra positional", args: []string{"seed", "causes", "extra"}, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, k, err := parseTraversalArgs(tt.args)
			if (err != nil) != tt.wantError {
				t.Fatalf("parseTraversalArgs(%q) error = %v, wantError = %v", tt.args, err, tt.wantError)
			}
			if tt.wantError {
				return
			}
			if !reflect.DeepEqual(got, tt.want) || k != tt.wantK {
				t.Fatalf("parseTraversalArgs(%q) = (%q, %d), want (%q, %d)", tt.args, got, k, tt.want, tt.wantK)
			}
		})
	}
}
