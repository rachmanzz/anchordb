package driver

import "testing"

// TestDateReturnsError verifies that malformed date input yields an error rather
// than panicking, and that MustDate preserves the panicking literal behaviour.
func TestDateReturnsError(t *testing.T) {
	if _, err := Date("not-a-date"); err == nil {
		t.Fatal("expected error for malformed date, got nil")
	}

	v, err := Date("2026-10-14")
	if err != nil {
		t.Fatalf("unexpected error for valid date: %v", err)
	}
	if v != MustDate("2026-10-14") {
		t.Fatalf("Date and MustDate disagree for valid input: %v vs %v", v, MustDate("2026-10-14"))
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected MustDate to panic on malformed input")
		}
	}()
	_ = MustDate("not-a-date")
}

func TestDateEdgeCasesAndRoundTrip(t *testing.T) {
	testCases := []struct {
		name       string
		dateStr    string
		wantDays   float32
		wantFormat string
	}{
		{"Epoch zero", "1970-01-01", 0, "1970-01-01"},
		{"Pre-epoch 1 day", "1969-12-31", -1, "1969-12-31"},
		{"Pre-epoch 1900", "1900-01-01", -25567, "1900-01-01"},
		{"Leap year 2024", "2024-02-29", 19782, "2024-02-29"},
		{"Century leap year 2000", "2000-02-29", 11016, "2000-02-29"},
		{"Standard date", "2026-10-14", 20740, "2026-10-14"},
		{"Year end", "2025-12-31", 20453, "2025-12-31"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			days, err := Date(tc.dateStr)
			if err != nil {
				t.Fatalf("Date(%q) error = %v", tc.dateStr, err)
			}
			if days != tc.wantDays {
				t.Fatalf("Date(%q) = %v, want %v", tc.dateStr, days, tc.wantDays)
			}
			formatted := FormatDate(days)
			if formatted != tc.wantFormat {
				t.Fatalf("FormatDate(%v) = %q, want %q", days, formatted, tc.wantFormat)
			}
		})
	}
}

func TestFormatDateFractionalRounding(t *testing.T) {
	// FormatDate applies math.Round to nearest integer day
	tests := []struct {
		input float32
		want  string
	}{
		{20740.0, "2026-10-14"},
		{20740.2, "2026-10-14"},
		{20740.49, "2026-10-14"},
		{20740.51, "2026-10-15"},
		{20740.9, "2026-10-15"},
	}

	for _, tc := range tests {
		got := FormatDate(tc.input)
		if got != tc.want {
			t.Errorf("FormatDate(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestDateMathOperatorsDirect(t *testing.T) {
	props := map[string]float32{
		"start_date": MustDate("2026-10-14"), // 20740
		"end_date":   MustDate("2026-11-28"), // 20785 (45 days later)
	}

	// AddDays positive
	addOp := AddDays("start_date", 45)
	res, err := addOp.Apply(props)
	if err != nil {
		t.Fatalf("AddDays.Apply: %v", err)
	}
	if FormatDate(res) != "2026-11-28" {
		t.Errorf("AddDays result = %q, want 2026-11-28", FormatDate(res))
	}

	// AddDays negative (effective subtraction)
	addNegOp := AddDays("end_date", -45)
	resNeg, err := addNegOp.Apply(props)
	if err != nil {
		t.Fatalf("AddDays negative.Apply: %v", err)
	}
	if FormatDate(resNeg) != "2026-10-14" {
		t.Errorf("AddDays negative result = %q, want 2026-10-14", FormatDate(resNeg))
	}

	// SubtractDays positive
	subOp := SubtractDays("end_date", 45)
	resSub, err := subOp.Apply(props)
	if err != nil {
		t.Fatalf("SubtractDays.Apply: %v", err)
	}
	if FormatDate(resSub) != "2026-10-14" {
		t.Errorf("SubtractDays result = %q, want 2026-10-14", FormatDate(resSub))
	}

	// DaysBetween (end - start = 45)
	daysBetweenOp := DaysBetween("end_date", "start_date")
	diff, err := daysBetweenOp.Apply(props)
	if err != nil {
		t.Fatalf("DaysBetween.Apply: %v", err)
	}
	if diff != 45 {
		t.Errorf("DaysBetween = %v, want 45", diff)
	}
}
