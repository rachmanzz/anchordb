package main

import (
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// rateLimiter is a dependency-free per-key token-bucket limiter. Keys are
// typically client IPs. Idle buckets are swept periodically so memory does not
// grow with the number of distinct clients ever seen.
type rateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*tokenBucket
	rate      float64 // tokens replenished per second
	burst     float64 // bucket capacity
	lastSweep time.Time
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// newRateLimiter builds a limiter allowing ratePerSec requests per second per
// key, with an optional burst capacity (defaults to ratePerSec, minimum 1).
func newRateLimiter(ratePerSec float64, burst int) *rateLimiter {
	if burst <= 0 {
		burst = int(ratePerSec)
	}
	if burst <= 0 {
		burst = 1
	}
	return &rateLimiter{
		buckets:   make(map[string]*tokenBucket),
		rate:      ratePerSec,
		burst:     float64(burst),
		lastSweep: time.Now(),
	}
}

// allow reports whether a request for key may proceed, consuming one token.
func (rl *rateLimiter) allow(key string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Sweep idle buckets at most once a minute.
	if now.Sub(rl.lastSweep) > time.Minute {
		rl.sweepLocked(now)
		rl.lastSweep = now
	}

	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}

	// Refill based on elapsed time, capped at burst.
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * rl.rate
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
		b.last = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// retryAfterSeconds returns a whole-second Retry-After suggestion.
func (rl *rateLimiter) retryAfterSeconds() int {
	if rl.rate <= 0 {
		return 1
	}
	s := int(1/rl.rate + 0.999)
	if s < 1 {
		s = 1
	}
	return s
}

// sweepLocked drops buckets that have been idle long enough to be fully refilled.
func (rl *rateLimiter) sweepLocked(now time.Time) {
	for k, b := range rl.buckets {
		if now.Sub(b.last).Seconds()*rl.rate >= rl.burst {
			delete(rl.buckets, k)
		}
	}
}

// rateLimitMiddleware enforces the limiter, if configured. Health and metrics
// endpoints are exempt so probes and scrapers keep working under load.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	if s.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.limiter.allow(s.clientKey(r)) {
			w.Header().Set("Retry-After", strconv.Itoa(s.limiter.retryAfterSeconds()))
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientKey derives the rate-limit key for a request, honoring trusted proxy
// headers via the IP filter and falling back to the socket peer address.
func (s *Server) clientKey(r *http.Request) string {
	if s.ipFilter != nil {
		if ip := s.ipFilter.ExtractClientIP(r); ip != nil {
			return ip.String()
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
