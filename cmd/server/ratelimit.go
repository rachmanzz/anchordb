package main

import (
	"container/list"
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
	lru       *list.List
	rate      float64 // tokens replenished per second
	burst     float64 // bucket capacity
	lastSweep time.Time
}

type tokenBucket struct {
	key    string
	elem   *list.Element
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
		lru:       list.New(),
		rate:      ratePerSec,
		burst:     float64(burst),
		lastSweep: time.Now(),
	}
}

const maxRateLimiterBuckets = 65536

// Allow exports token-bucket rate limiting for shared TCP/HTTP abuse control.
func (rl *rateLimiter) Allow(key string) bool {
	return rl.allow(key)
}

// allow reports whether a request for key may proceed, consuming one token.
func (rl *rateLimiter) allow(key string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Sweep idle buckets periodically; also sweep before LRU eviction under cap pressure
	// so stale refilled buckets are pruned before active client buckets are evicted.
	if now.Sub(rl.lastSweep) > time.Minute {
		rl.sweepLocked(now)
		rl.lastSweep = now
	}

	b, ok := rl.buckets[key]
	if ok {
		if rl.lru != nil && b.elem != nil {
			rl.lru.MoveToFront(b.elem)
		}
	} else {
		if len(rl.buckets) >= maxRateLimiterBuckets {
			rl.sweepLocked(now)
			rl.lastSweep = now
		}
		for len(rl.buckets) >= maxRateLimiterBuckets {
			if rl.lru != nil && rl.lru.Len() > 0 {
				oldest := rl.lru.Back()
				if ob, valid := oldest.Value.(*tokenBucket); valid {
					delete(rl.buckets, ob.key)
				}
				rl.lru.Remove(oldest)
				continue
			}
			for k := range rl.buckets {
				delete(rl.buckets, k)
				break
			}
		}
		b = &tokenBucket{key: key, tokens: rl.burst, last: now}
		if rl.lru != nil {
			b.elem = rl.lru.PushFront(b)
		}
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
			if rl.lru != nil && b.elem != nil {
				rl.lru.Remove(b.elem)
			}
		}
	}
}

// rateLimitMiddleware enforces the limiter, if configured. Health probes remain
// exempt so liveness/readiness checks keep working under load.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	if s.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.limiter.allow(s.clientKey(r)) {
			w.Header().Set("Retry-After", strconv.Itoa(s.limiter.retryAfterSeconds()))
			writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded")
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
