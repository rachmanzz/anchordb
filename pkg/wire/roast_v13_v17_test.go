package wire

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"math/big"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/anchordb/pkg/storage"
)

func TestRoastV13_PreAuthFrameLimitRejectsOversizedBeforeOOM(t *testing.T) {
	backend := &mockBackend{
		dataDir:   t.TempDir(),
		models:    make(map[string]*storage.AnchorDB),
		authToken: "secret-token",
	}
	defer backend.CloseAll()

	srv := NewServer(backend)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()

	conn, err := net.DialTimeout("tcp", l.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send only a 16-byte header claiming a 1 MiB pre-auth payload; server must reject immediately.
	var hdr [HeaderSize]byte
	hdr[0] = Magic
	hdr[1] = Version
	hdr[2] = OpAuth
	binary.LittleEndian.PutUint32(hdr[4:8], 1)
	binary.LittleEndian.PutUint32(hdr[8:12], 1<<20)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write header: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	if _, rErr := conn.Read(buf); rErr == nil {
		t.Fatal("expected server to close connection on oversized pre-auth frame")
	}
}

func TestRoastV13_AuthBypassAndSessionBindingAndOTKOverwrite(t *testing.T) {
	backend := &mockBackend{
		dataDir:   t.TempDir(),
		models:    make(map[string]*storage.AnchorDB),
		authToken: "backend-secret",
	}
	defer backend.CloseAll()

	// JoinManager with JoinMode="" and AuthToken="" must still delegate AuthTypeStatic to backend.Authenticate.
	jm := NewJoinManager(JoinConfig{
		GatewayMode: false,
		ClusterID:   "c-bind",
	})
	srv := NewServerWithJoinManager(backend, jm)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()

	c, err := Connect(l.Addr().String())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	if _, err := c.AuthStructured(AuthRequest{Type: AuthTypeStatic, Credential: ""}); err == nil {
		t.Fatal("expected empty AuthTypeStatic credential to be rejected when backend.RequiresAuth() is true")
	}

	// Explicit JoinMode="otk" with GatewayMode=false must require auth initially.
	backendOpen := &mockBackend{
		dataDir: t.TempDir(),
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backendOpen.CloseAll()
	jmOTK := NewJoinManager(JoinConfig{
		GatewayMode: false,
		JoinMode:    "otk",
		ClusterID:   "c-otk",
	})
	srvOTK := NewServerWithJoinManager(backendOpen, jmOTK)
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 2: %v", err)
	}
	defer l2.Close()
	go func() { _ = srvOTK.Serve(l2) }()
	defer srvOTK.Close()

	c2, err := Connect(l2.Addr().String())
	if err != nil {
		t.Fatalf("Connect 2: %v", err)
	}
	defer c2.Close()
	if err := c2.Auth(""); err == nil {
		t.Fatal("expected legacy OpAuth(\"\") to be rejected when JoinMode=otk")
	}
	c2b, err := Connect(l2.Addr().String())
	if err != nil {
		t.Fatalf("Connect 2b: %v", err)
	}
	defer c2b.Close()
	if _, err := c2b.Save(storage.BatchRecordItem{Name: "x"}); err == nil {
		t.Fatal("expected unauthenticated Save to fail when JoinMode=otk")
	}

	// SessionKey cluster, nodeID, and TTL binding + OTK no-overwrite check.
	store, err := OpenCommitmentStore(filepath.Join(t.TempDir(), "comm.bin"))
	if err != nil {
		t.Fatalf("OpenCommitmentStore: %v", err)
	}
	defer store.Close()

	jmA := NewJoinManager(JoinConfig{
		GatewayMode:     true,
		ClusterID:       "cluster-A",
		SessionSecret:   "shared-secret-32-bytes-minimum!!",
		SessionTTL:      50 * time.Millisecond,
		CommitmentStore: store,
	})
	jmB := NewJoinManager(JoinConfig{
		GatewayMode:   true,
		ClusterID:     "cluster-B",
		SessionSecret: "shared-secret-32-bytes-minimum!!",
	})

	sessA := jmA.GenerateSessionKey("node-1")
	if _, err := jmB.ValidateSessionKey(sessA); !errors.Is(err, ErrInvalidSessionKey) {
		t.Fatalf("expected cluster mismatch to fail ValidateSessionKey, got %v", err)
	}
	if _, code, err := jmA.HandleStructuredAuth(AuthRequest{
		Type:       AuthTypeSession,
		NodeID:     "node-2",
		Credential: sessA,
	}, nil); err == nil || code != ErrCodeInvalidSession {
		t.Fatalf("expected nodeID mismatch to fail session auth, got code=%d err=%v", code, err)
	}

	otk, err := jmA.GenerateOTK("CONNECTOR", 1, time.Minute)
	if err != nil {
		t.Fatalf("GenerateOTK: %v", err)
	}
	c1 := sha256.Sum256([]byte("secret-1"))
	if _, _, err := jmA.HandleStructuredAuth(AuthRequest{
		Type:              AuthTypeOTK,
		NodeID:            "enrolled-node",
		Credential:        otk,
		InitialCommitment: c1,
		HasCommitment:     true,
	}, nil); err != nil {
		t.Fatalf("first OTK enroll failed: %v", err)
	}
	otk2, err := jmA.GenerateOTK("CONNECTOR", 2, time.Minute)
	if err != nil {
		t.Fatalf("GenerateOTK 2: %v", err)
	}
	c2Hash := sha256.Sum256([]byte("hijack"))
	if _, code, err := jmA.HandleStructuredAuth(AuthRequest{
		Type:              AuthTypeOTK,
		NodeID:            "enrolled-node",
		Credential:        otk2,
		InitialCommitment: c2Hash,
		HasCommitment:     true,
	}, nil); err == nil || code != ErrCodeUnauthorized {
		t.Fatalf("expected OTK overwrite of enrolled-node to fail, got code=%d err=%v", code, err)
	}

	time.Sleep(1100 * time.Millisecond)
	if _, err := jmA.ValidateSessionKey(sessA); !errors.Is(err, ErrInvalidSessionKey) {
		t.Fatalf("expected expired session key to fail ValidateSessionKey, got %v", err)
	}
}

func TestRoastV17_NoBuf1CollisionAndSeqMismatchClosesAndTLS(t *testing.T) {
	// 1. Construct a sessionKey whose length is 257 (257 % 256 == 1 == SubCodeRotationStaged).
	longNodeID := strings.Repeat("n", 180)
	jm := NewJoinManager(JoinConfig{
		GatewayMode: true,
		ClusterID:   "c1",
	})
	sess := jm.GenerateSessionKey(longNodeID)
	// Adjust nodeID length until len(sess) % 256 == 1.
	for len(sess)%256 != 1 {
		longNodeID += "x"
		sess = jm.GenerateSessionKey(longNodeID)
	}
	payload, err := EncodeAuthResponse(sess)
	if err != nil {
		t.Fatalf("EncodeAuthResponse failed: %v", err)
	}
	if payload[0] != StructuredRespMagic || payload[1] != 0x01 {
		t.Fatalf("expected payload[0]=0xAB and payload[1]=0x01 for len=%d", len(sess))
	}
	got, err := DecodeAuthResponse(payload)
	if err != nil || got != sess {
		t.Fatalf("DecodeAuthResponse failed for len=%d: got len=%d err=%v", len(sess), len(got), err)
	}
	rotPayload, err := EncodeRotationStagedResponse("ok")
	if err != nil {
		t.Fatalf("EncodeRotationStagedResponse failed: %v", err)
	}
	if rotPayload[0] != StructuredRotationRespMagic {
		t.Fatalf("expected StructuredRotationRespMagic, got 0x%02X", rotPayload[0])
	}

	// 2. TLS round-trip + AuthStructured over wire with 257-byte session key.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  key,
	}
	pool := x509.NewCertPool()
	parsedCert, _ := x509.ParseCertificate(derBytes)
	pool.AddCert(parsedCert)

	backend := &mockBackend{
		dataDir: t.TempDir(),
		models:  make(map[string]*storage.AnchorDB),
	}
	defer backend.CloseAll()

	jmTLS := NewJoinManager(JoinConfig{
		GatewayMode: true,
		JoinMode:    "pairing",
		ClusterID:   "c1",
	})
	srv := NewServerWithJoinManager(backend, jmTLS)
	srv.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TLS: %v", err)
	}
	defer l.Close()
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()

	c, err := Connect(l.Addr().String(), WithClientTLS(&tls.Config{RootCAs: pool}))
	if err != nil {
		t.Fatalf("Connect TLS: %v", err)
	}
	defer c.Close()

	gotSess, err := c.AuthPairing(longNodeID)
	if err != nil {
		t.Fatalf("AuthPairing with 257-mod-256==1 session key over TLS failed: %v", err)
	}
	if len(gotSess)%256 != 1 {
		t.Fatalf("expected len(gotSess)%%256 == 1, got %d", len(gotSess))
	}

	// 3. Response sequence mismatch must close the client connection.
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go func() {
		req, rErr := ReadFrame(serverConn)
		if rErr != nil {
			return
		}
		_ = WriteFrame(serverConn, Frame{
			Magic:   Magic,
			Version: Version,
			OpCode:  OpPong,
			Seq:     req.Seq + 99,
			Payload: []byte("PONG"),
		})
	}()
	cPipe := &Client{
		conn:    clientConn,
		timeout: time.Second,
		reader:  bufio.NewReader(clientConn),
		writer:  bufio.NewWriter(clientConn),
	}
	if err := cPipe.Ping(); err == nil || !strings.Contains(err.Error(), "sequence mismatch") {
		t.Fatalf("expected sequence mismatch error, got %v", err)
	}
	if err := cPipe.Ping(); err == nil {
		t.Fatal("expected subsequent call on closed desynced client to fail")
	}
}
