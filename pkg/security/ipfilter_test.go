package security

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIPFilter_ExactIP_CIDR_And_Range(t *testing.T) {
	// Rule:
	// - Exact single IP: 127.0.0.1, ::1, 8.8.8.8
	// - CIDR: 192.168.1.0/24, 10.0.0.0/16
	// - IP Range: 172.16.0.10-172.16.0.50
	rule := "127.0.0.1, ::1, 8.8.8.8, 192.168.1.0/24, 10.0.0.0/16, 172.16.0.10-172.16.0.50"
	filter, err := NewIPFilter(rule, true)
	if err != nil {
		t.Fatalf("NewIPFilter failed: %v", err)
	}

	testCases := []struct {
		ip       string
		expected bool
	}{
		// 1. Exact match
		{"127.0.0.1", true},
		{"::1", true},
		{"8.8.8.8", true},
		{"8.8.4.4", false}, // not in whitelist

		// 2. CIDR match
		{"192.168.1.1", true},
		{"192.168.1.254", true},
		{"192.168.2.1", false}, // different subnet
		{"10.0.5.20", true},
		{"10.1.0.1", false}, // outside /16

		// 3. IP Range match [172.16.0.10 - 172.16.0.50]
		{"172.16.0.10", true},  // edge start
		{"172.16.0.25", true},  // inside
		{"172.16.0.50", true},  // edge end
		{"172.16.0.9", false},  // below start
		{"172.16.0.51", false}, // above end
	}

	for _, tc := range testCases {
		parsed := net.ParseIP(tc.ip)
		if parsed == nil {
			t.Fatalf("failed to parse IP %s", tc.ip)
		}
		allowed := filter.IsAllowed(parsed)
		if allowed != tc.expected {
			t.Errorf("IP %s: expected allowed=%v, got=%v", tc.ip, tc.expected, allowed)
		}
	}
}

func TestIPFilter_HTTPMiddleware(t *testing.T) {
	rule := "192.168.1.0/24, 10.0.0.1-10.0.0.10"
	filter, err := NewIPFilter(rule, true)
	if err != nil {
		t.Fatalf("NewIPFilter failed: %v", err)
	}

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mw := filter.Middleware(dummyHandler)

	// Case 1: Allowed IP via RemoteAddr
	req1 := httptest.NewRequest("GET", "/health", nil)
	req1.RemoteAddr = "192.168.1.42:54321"
	rec1 := httptest.NewRecorder()
	mw.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec1.Code)
	}

	// Case 2: Blocked IP
	req2 := httptest.NewRequest("GET", "/health", nil)
	req2.RemoteAddr = "203.0.113.195:12345"
	rec2 := httptest.NewRecorder()
	mw.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden, got %d", rec2.Code)
	}

	// Case 3: Allowed IP via X-Forwarded-For with trustProxy=true
	req3 := httptest.NewRequest("GET", "/health", nil)
	req3.RemoteAddr = "127.0.0.1:8080" // local proxy
	req3.Header.Set("X-Forwarded-For", "10.0.0.5, 127.0.0.1")
	rec3 := httptest.NewRecorder()
	mw.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Errorf("expected 200 OK via X-Forwarded-For, got %d", rec3.Code)
	}
}
