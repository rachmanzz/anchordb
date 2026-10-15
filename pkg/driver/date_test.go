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
