package security

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// IPRange defines an inclusive IP range [Start, End]
type IPRange struct {
	Start net.IP
	End   net.IP
	IsV4  bool
}

// IPFilter evaluates client IP addresses against single IPs, CIDR blocks, and IP ranges
type IPFilter struct {
	allowedIPs    map[string]bool
	allowedCIDRs  []*net.IPNet
	allowedRanges []IPRange
	trustProxy    bool
	rawRule       string
}

// NewIPFilter parses a comma-separated list of IP rules:
// Examples:
// - Single IP: "127.0.0.1", "::1", "192.168.1.50"
// - CIDR Range: "192.168.1.0/24", "10.0.0.0/8"
// - Explicit Range: "192.168.1.10-192.168.1.50"
func NewIPFilter(rules string, trustProxy bool) (*IPFilter, error) {
	filter := &IPFilter{
		allowedIPs: make(map[string]bool),
		trustProxy: trustProxy,
		rawRule:    strings.TrimSpace(rules),
	}

	if filter.rawRule == "" {
		return filter, nil
	}

	tokens := strings.Split(filter.rawRule, ",")
	for _, rawToken := range tokens {
		token := strings.TrimSpace(rawToken)
		if token == "" {
			continue
		}

		// 1. Check if token is CIDR (e.g. 192.168.1.0/24 or 10.0.0.0/8)
		if strings.Contains(token, "/") {
			_, ipNet, err := net.ParseCIDR(token)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR rule %q: %w", token, err)
			}
			filter.allowedCIDRs = append(filter.allowedCIDRs, ipNet)
			continue
		}

		// 2. Check if token is Range (e.g. 192.168.1.10-192.168.1.50)
		if strings.Contains(token, "-") {
			parts := strings.SplitN(token, "-", 2)
			startStr := strings.TrimSpace(parts[0])
			endStr := strings.TrimSpace(parts[1])

			startIP := net.ParseIP(startStr)
			endIP := net.ParseIP(endStr)

			if startIP == nil || endIP == nil {
				return nil, fmt.Errorf("invalid IP range rule %q", token)
			}

			startV4 := startIP.To4()
			endV4 := endIP.To4()

			if (startV4 != nil && endV4 == nil) || (startV4 == nil && endV4 != nil) {
				return nil, fmt.Errorf("mismatched IPv4/IPv6 in range rule %q", token)
			}

			isV4 := startV4 != nil
			if isV4 {
				if ipToUint32(startV4) > ipToUint32(endV4) {
					return nil, fmt.Errorf("start IP %s is greater than end IP %s", startStr, endStr)
				}
			} else {
				if bytes.Compare(startIP, endIP) > 0 {
					return nil, fmt.Errorf("start IP %s is greater than end IP %s", startStr, endStr)
				}
			}

			filter.allowedRanges = append(filter.allowedRanges, IPRange{
				Start: startIP,
				End:   endIP,
				IsV4:  isV4,
			})
			continue
		}

		// 3. Exact Single IP (e.g. 127.0.0.1 or ::1)
		parsedIP := net.ParseIP(token)
		if parsedIP == nil {
			return nil, fmt.Errorf("invalid IP rule %q", token)
		}
		// Canonical string
		filter.allowedIPs[parsedIP.String()] = true
	}

	return filter, nil
}

// IsEmpty returns true if no whitelist rules were defined
func (f *IPFilter) IsEmpty() bool {
	return len(f.allowedIPs) == 0 && len(f.allowedCIDRs) == 0 && len(f.allowedRanges) == 0
}

// IsAllowed checks if a given IP satisfies any whitelist rule
func (f *IPFilter) IsAllowed(ip net.IP) bool {
	if f.IsEmpty() {
		return true // No whitelist = open access
	}

	if ip == nil {
		return false
	}

	// 1. Exact match check
	if f.allowedIPs[ip.String()] {
		return true
	}
	// For IPv4 representation
	if ipV4 := ip.To4(); ipV4 != nil {
		if f.allowedIPs[ipV4.String()] {
			return true
		}
	}

	// 2. CIDR match check
	for _, cidr := range f.allowedCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}

	// 3. Range match check
	ipV4 := ip.To4()
	for _, r := range f.allowedRanges {
		if r.IsV4 && ipV4 != nil {
			val := ipToUint32(ipV4)
			startVal := ipToUint32(r.Start.To4())
			endVal := ipToUint32(r.End.To4())
			if val >= startVal && val <= endVal {
				return true
			}
		} else if !r.IsV4 && ipV4 == nil {
			if bytes.Compare(ip, r.Start) >= 0 && bytes.Compare(ip, r.End) <= 0 {
				return true
			}
		}
	}

	return false
}

// ExtractClientIP retrieves client IP from request, taking proxy headers into account if configured
func (f *IPFilter) ExtractClientIP(r *http.Request) net.IP {
	if f.trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			clientIP := strings.TrimSpace(parts[0])
			if ip := net.ParseIP(clientIP); ip != nil {
				return ip
			}
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			if ip := net.ParseIP(strings.TrimSpace(xri)); ip != nil {
				return ip
			}
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

// Middleware returns an HTTP middleware verifying incoming IP against whitelist rules
func (f *IPFilter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := f.ExtractClientIP(r)
		if !f.IsAllowed(clientIP) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"Forbidden: client IP %s is not allowed by AnchorDB whitelist"}`, clientIP)))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func ipToUint32(ip net.IP) uint32 {
	v4 := ip.To4()
	if v4 == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v4)
}
