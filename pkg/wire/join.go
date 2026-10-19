package wire

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrPairingWindowClosed is returned when an AUTH_PAIRING request arrives after the window expired or closed.
	ErrPairingWindowClosed = errors.New("pairing window closed")
	// ErrIPNotAllowed is returned when remote IP is not within the allowed CIDR for pairing.
	ErrIPNotAllowed = errors.New("remote ip not allowed for pairing")
	// ErrOTKExpiredOrBurned is returned when an OTK token is expired, not found, or already burned.
	ErrOTKExpiredOrBurned = errors.New("otk token expired or already burned")
	// ErrInvalidSessionKey is returned when session key signature verification fails.
	ErrInvalidSessionKey = errors.New("invalid session key signature")
	// ErrUnauthorizedGateway is returned when a client attempts direct/static access while gateway mode is enforced.
	ErrUnauthorizedGateway = errors.New("unauthorized: direct client access disabled in gateway mode; connection must route through anchor-connector")
	// ErrInvalidToken is returned when authentication token does not match.
	ErrInvalidToken = errors.New("unauthorized: invalid token")
)

// JoinConfig defines configuration options for AnchorWire cluster join and gateway enforcement.
type JoinConfig struct {
	GatewayMode         bool
	JoinMode            string // "pairing", "otk", "static"
	PairingTimeout      time.Duration
	PairingAllowedCIDR  string
	SessionSecret       string
	AuthToken           string
	ClusterID           string
	OTKSecret           string
	CommitmentStore     *CommitmentStore
	CommitmentStorePath string
}

// JoinTokenEntry holds metadata for ephemeral One-Time Join Keys (OTK).
type JoinTokenEntry struct {
	TokenString string
	Role        string
	ClusterID   string
	Epoch       uint64
	ExpiresAt   time.Time
	Burned      bool
	BurnedAt    time.Time
}

// JoinManager coordinates pairing windows, one-time keys, session tickets, and gateway isolation.
type JoinManager struct {
	mu              sync.RWMutex
	gatewayMode     bool
	joinMode        string
	pairingActive   bool
	pairingExpiry   time.Time
	allowedNets     []*net.IPNet
	sessionSecret   []byte
	staticToken     string
	clusterID       string
	otkSecret       []byte
	otkTokens       map[string]*JoinTokenEntry
	commitmentStore *CommitmentStore
}

// NewJoinManager creates and initializes a JoinManager.
func NewJoinManager(cfg JoinConfig) *JoinManager {
	if cfg.ClusterID == "" {
		cfg.ClusterID = "cluster-01"
	}
	if cfg.PairingTimeout <= 0 {
		cfg.PairingTimeout = 120 * time.Second
	}
	if cfg.JoinMode == "" {
		if cfg.GatewayMode {
			cfg.JoinMode = "pairing"
		} else {
			cfg.JoinMode = "otk"
		}
	}

	var sessionSecretBytes []byte
	if cfg.SessionSecret != "" {
		sessionSecretBytes = []byte(cfg.SessionSecret)
	} else {
		sessionSecretBytes = make([]byte, 32)
		if _, err := rand.Read(sessionSecretBytes); err != nil {
			copy(sessionSecretBytes, []byte("anchordb-default-session-secret-key-32b"))
		}
	}

	var otkSecretBytes []byte
	if cfg.OTKSecret != "" {
		otkSecretBytes = []byte(cfg.OTKSecret)
	} else {
		otkSecretBytes = sessionSecretBytes
	}

	var allowedNets []*net.IPNet
	if cfg.PairingAllowedCIDR != "" {
		for _, part := range strings.Split(cfg.PairingAllowedCIDR, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if !strings.Contains(part, "/") {
				if ip := net.ParseIP(part); ip != nil {
					if ip.To4() != nil {
						part += "/32"
					} else {
						part += "/128"
					}
				}
			}
			if _, cidrNet, err := net.ParseCIDR(part); err == nil {
				allowedNets = append(allowedNets, cidrNet)
			}
		}
	}

	var cs *CommitmentStore
	if cfg.CommitmentStore != nil {
		cs = cfg.CommitmentStore
	} else if cfg.CommitmentStorePath != "" {
		opened, err := OpenCommitmentStore(cfg.CommitmentStorePath)
		if err == nil {
			cs = opened
		}
	}

	jm := &JoinManager{
		gatewayMode:     cfg.GatewayMode,
		joinMode:        cfg.JoinMode,
		sessionSecret:   sessionSecretBytes,
		staticToken:     cfg.AuthToken,
		clusterID:       cfg.ClusterID,
		otkSecret:       otkSecretBytes,
		otkTokens:       make(map[string]*JoinTokenEntry),
		allowedNets:     allowedNets,
		commitmentStore: cs,
	}

	if cfg.GatewayMode || cfg.JoinMode == "pairing" {
		jm.pairingActive = true
		jm.pairingExpiry = time.Now().Add(cfg.PairingTimeout)
	}

	return jm
}

// CommitmentStore returns the registered CommitmentStore, if any.
func (jm *JoinManager) CommitmentStore() *CommitmentStore {
	if jm == nil {
		return nil
	}
	jm.mu.RLock()
	defer jm.mu.RUnlock()
	return jm.commitmentStore
}

// SetCommitmentStore assigns or updates the CommitmentStore.
func (jm *JoinManager) SetCommitmentStore(cs *CommitmentStore) {
	if jm == nil {
		return
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.commitmentStore = cs
}

// IsGatewayMode reports whether gateway enforcement is active.
func (jm *JoinManager) IsGatewayMode() bool {
	if jm == nil {
		return false
	}
	jm.mu.RLock()
	defer jm.mu.RUnlock()
	return jm.gatewayMode
}

// IsPairingActive reports whether the pairing window is currently open.
func (jm *JoinManager) IsPairingActive() bool {
	if jm == nil {
		return false
	}
	jm.mu.RLock()
	defer jm.mu.RUnlock()
	return jm.pairingActive && time.Now().Before(jm.pairingExpiry)
}

// ClosePairing explicitly closes the pairing window.
func (jm *JoinManager) ClosePairing() {
	if jm == nil {
		return
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.pairingActive = false
}

// Close gracefully flushes any resources held by JoinManager.
func (jm *JoinManager) Close() error {
	if jm == nil {
		return nil
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if jm.commitmentStore != nil {
		return jm.commitmentStore.Close()
	}
	return nil
}

// GenerateOTK creates an ephemeral one-time join key.
func (jm *JoinManager) GenerateOTK(role string, epoch uint64, ttl time.Duration) (string, error) {
	if jm == nil {
		return "", errors.New("join manager not initialized")
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	if role == "" {
		role = "CONNECTOR"
	}
	role = strings.ToUpper(strings.TrimSpace(role))

	randBytes := make([]byte, 12)
	if _, err := rand.Read(randBytes); err != nil {
		return "", fmt.Errorf("failed to read random entropy: %w", err)
	}
	randomSecret := hex.EncodeToString(randBytes)

	payload := fmt.Sprintf("ANCH.JOIN.%s.%s.%d.%s", role, jm.clusterID, epoch, randomSecret)
	mac := hmac.New(sha256.New, jm.otkSecret)
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))[:32]

	tokenString := fmt.Sprintf("%s.%s", payload, sig)

	jm.mu.Lock()
	defer jm.mu.Unlock()

	// Prune expired or burned entries older than 10 minutes to prevent unbounded memory growth.
	now := time.Now()
	for k, entry := range jm.otkTokens {
		if now.After(entry.ExpiresAt) || (entry.Burned && now.Sub(entry.BurnedAt) > 10*time.Minute) {
			delete(jm.otkTokens, k)
		}
	}

	jm.otkTokens[tokenString] = &JoinTokenEntry{
		TokenString: tokenString,
		Role:        role,
		ClusterID:   jm.clusterID,
		Epoch:       epoch,
		ExpiresAt:   time.Now().Add(ttl),
		Burned:      false,
	}

	return tokenString, nil
}

// PruneExpiredTokens removes expired or stale burned OTK tokens from memory and returns the count removed.
func (jm *JoinManager) PruneExpiredTokens(now time.Time) int {
	if jm == nil {
		return 0
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()

	pruned := 0
	for k, entry := range jm.otkTokens {
		if now.After(entry.ExpiresAt) || (entry.Burned && now.Sub(entry.BurnedAt) > 10*time.Minute) {
			delete(jm.otkTokens, k)
			pruned++
		}
	}
	return pruned
}

// ValidateAndBurnOTK checks and consumes an OTK token.
func (jm *JoinManager) ValidateAndBurnOTK(tokenString string) (role, clusterID string, epoch uint64, err error) {
	if jm == nil {
		return "", "", 0, ErrOTKExpiredOrBurned
	}
	parts := strings.Split(tokenString, ".")
	if len(parts) != 7 || parts[0] != "ANCH" || parts[1] != "JOIN" {
		return "", "", 0, ErrOTKExpiredOrBurned
	}

	role = parts[2]
	clusterID = parts[3]
	epochVal, parseErr := strconv.ParseUint(parts[4], 10, 64)
	if parseErr != nil {
		return "", "", 0, ErrOTKExpiredOrBurned
	}
	epoch = epochVal
	randomSecret := parts[5]
	expectedSig := parts[6]

	payload := fmt.Sprintf("ANCH.JOIN.%s.%s.%d.%s", role, clusterID, epoch, randomSecret)
	mac := hmac.New(sha256.New, jm.otkSecret)
	mac.Write([]byte(payload))
	actualSig := hex.EncodeToString(mac.Sum(nil))[:32]

	if !hmac.Equal([]byte(expectedSig), []byte(actualSig)) {
		return "", "", 0, ErrOTKExpiredOrBurned
	}

	jm.mu.Lock()
	defer jm.mu.Unlock()

	entry, exists := jm.otkTokens[tokenString]
	if !exists {
		return "", "", 0, ErrOTKExpiredOrBurned
	}

	if time.Now().After(entry.ExpiresAt) {
		return "", "", 0, ErrOTKExpiredOrBurned
	}

	if entry.Burned {
		return "", "", 0, ErrOTKExpiredOrBurned
	}

	entry.Burned = true
	entry.BurnedAt = time.Now()
	return role, clusterID, epoch, nil
}

// GenerateSessionKey generates a signed session key for nodeID.
func (jm *JoinManager) GenerateSessionKey(nodeID string) string {
	if nodeID == "" {
		nodeID = "anchor-connector"
	}
	randBytes := make([]byte, 16)
	if _, err := rand.Read(randBytes); err != nil {
		// Fallback to high-resolution timestamp if entropy pool fails
		binary.LittleEndian.PutUint64(randBytes[:8], uint64(time.Now().UnixNano()))
	}
	randHex := hex.EncodeToString(randBytes)

	payload := fmt.Sprintf("SESS.%s.%s.%s", jm.clusterID, nodeID, randHex)
	mac := hmac.New(sha256.New, jm.sessionSecret)
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))[:32]

	return fmt.Sprintf("%s.%s", payload, sig)
}

// ValidateSessionKey verifies that the session key carries a valid HMAC signature.
func (jm *JoinManager) ValidateSessionKey(key string) (nodeID string, err error) {
	if jm == nil {
		return "", ErrInvalidSessionKey
	}
	parts := strings.Split(key, ".")
	if len(parts) != 5 || parts[0] != "SESS" {
		return "", ErrInvalidSessionKey
	}
	payload := fmt.Sprintf("SESS.%s.%s.%s", parts[1], parts[2], parts[3])
	expectedSig := parts[4]

	mac := hmac.New(sha256.New, jm.sessionSecret)
	mac.Write([]byte(payload))
	actualSig := hex.EncodeToString(mac.Sum(nil))[:32]

	if !hmac.Equal([]byte(expectedSig), []byte(actualSig)) {
		return "", ErrInvalidSessionKey
	}
	return parts[2], nil
}

// HandleStructuredAuth processes an incoming structured AuthRequest.
func (jm *JoinManager) HandleStructuredAuth(req AuthRequest, remoteAddr net.Addr) (sessionKey string, errCode uint16, err error) {
	if jm == nil {
		return "", ErrCodeUnauthorized, ErrInvalidToken
	}

	switch req.Type {
	case AuthTypePairing:
		jm.mu.Lock()
		active := jm.pairingActive && time.Now().Before(jm.pairingExpiry)
		if !active {
			jm.pairingActive = false
			jm.mu.Unlock()
			return "", ErrCodePairingClosed, ErrPairingWindowClosed
		}

		if len(jm.allowedNets) > 0 && remoteAddr != nil {
			host, _, sErr := net.SplitHostPort(remoteAddr.String())
			if sErr != nil {
				host = remoteAddr.String()
			}
			ip := net.ParseIP(host)
			if ip != nil {
				matched := false
				for _, allowed := range jm.allowedNets {
					if allowed.Contains(ip) {
						matched = true
						break
					}
				}
				if !matched {
					jm.mu.Unlock()
					return "", ErrCodeIPNotAllowed, ErrIPNotAllowed
				}
			}
		}

		// Lock pairing after successful initial handshake
		jm.pairingActive = false
		jm.mu.Unlock()

		sessKey := jm.GenerateSessionKey(req.NodeID)
		if req.HasCommitment && jm.commitmentStore != nil {
			_ = jm.commitmentStore.RegisterInitial(req.NodeID, req.InitialCommitment)
		}
		return sessKey, 0, nil

	case AuthTypeOTK:
		_, _, _, err := jm.ValidateAndBurnOTK(req.Credential)
		if err != nil {
			return "", ErrCodeOTKExpiredOrBurned, ErrOTKExpiredOrBurned
		}
		sessKey := jm.GenerateSessionKey(req.NodeID)
		if req.HasCommitment && jm.commitmentStore != nil {
			_ = jm.commitmentStore.RegisterInitial(req.NodeID, req.InitialCommitment)
		}
		return sessKey, 0, nil

	case AuthTypeSession:
		_, err := jm.ValidateSessionKey(req.Credential)
		if err != nil {
			return "", ErrCodeInvalidSession, ErrInvalidSessionKey
		}
		// Valid session key from earlier pairing/OTK
		return req.Credential, 0, nil

	case AuthTypeStagedReauth:
		if jm.commitmentStore == nil {
			return "", ErrCodeUnauthorized, ErrUnknownNodeID
		}
		code, aErr := jm.commitmentStore.StageReauth(req.NodeID, req.Validator, req.NextCommitment)
		if aErr != nil {
			return "", code, aErr
		}
		return "", 0, nil

	case AuthTypeStatic:
		if jm.gatewayMode {
			return "", ErrCodeUnauthorized, ErrUnauthorizedGateway
		}
		if jm.staticToken == "" {
			return "", 0, nil
		}
		if subtle.ConstantTimeCompare([]byte(req.Credential), []byte(jm.staticToken)) == 1 {
			return "", 0, nil
		}
		return "", ErrCodeUnauthorized, ErrInvalidToken

	default:
		return "", ErrCodeUnauthorized, ErrInvalidToken
	}
}
