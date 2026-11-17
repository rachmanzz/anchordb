package wire

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/system"
)

// wireErrorCode maps storage failures to wire error codes so a missing record
// never shares a code with a failed computation or a closed database.
func wireErrorCode(err error) uint16 {
	switch {
	case errors.Is(err, storage.ErrRecordNotFound):
		return 404
	case errors.Is(err, storage.ErrComputeFailed):
		return 422
	case errors.Is(err, storage.ErrDatabaseClosed):
		return 503
	default:
		return 500
	}
}

// Backend defines the storage interface wire.Server requires from AnchorDB.
type Backend interface {
	GetModel(name string) (*storage.AnchorDB, error)
	Authenticate(token string) bool
	RequiresAuth() bool
}

// LeaseBackend is an optional extension of Backend that pins an open model
// database across an operation so concurrent eviction cannot Close/Munmap it
// while in flight.
type LeaseBackend interface {
	AcquireModel(name string) (db *storage.AnchorDB, release func(), err error)
}

// TieredBackend defines cold storage tiered fallback for wire queries.
type TieredBackend interface {
	GetRecordTiered(model, name string) (props map[string]float32, op uint8, err error)
	GetRecordWithComputeTiered(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (props map[string]float32, op uint8, err error)
}

// TypedTieredBackend defines typed cold storage tiered fallback for wire queries.
type TypedTieredBackend interface {
	GetRecordTypedWithComputeTiered(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error)
}

// ColdTierBackend is an optional extension consulted only after the caller's
// own hot lookup already failed with storage.ErrRecordNotFound. Backends
// implementing it let callers skip the redundant second hot read that the
// full tiered fallback would otherwise repeat before going cold.
type ColdTierBackend interface {
	GetColdRecordTyped(model, name string, computations map[string]storage.ComputeOp, selectFields []string) (storage.CompactRecord, error)
}

// PartitionBackend defines AnchorWire handler interfaces for catalog and direct inspection.
type PartitionBackend interface {
	GetPartitionCatalog(model string) ([]PartitionCatalogItemDTO, error)
	InspectPartition(fileOrLoc, recordName string, limit int) (PartitionInspectWireResponse, error)
}

type saveJob struct {
	model     string
	item      storage.BatchRecordItem
	savedName string
	err       error
	done      chan struct{}
}

type edgeJob struct {
	model string
	item  storage.EdgeRecord
	err   error
	done  chan struct{}
}

const (
	defaultWireMaxConns     = 1024
	defaultWireReadTimeout  = 60 * time.Second
	defaultWireWriteTimeout = 30 * time.Second
	defaultWireCloseTimeout = 5 * time.Second
)

// Server is the AnchorWire binary TCP protocol daemon.
type Server struct {
	backend           Backend
	joinManager       *JoinManager
	gatewayMode       bool
	tlsConfig         *tls.Config
	l                 net.Listener
	mu                sync.Mutex
	conns             map[net.Conn]struct{}
	connSem           chan struct{}
	readTimeout       time.Duration
	writeTimeout      time.Duration
	closed            bool
	wg                sync.WaitGroup
	saveJobs          chan *saveJob
	edgeJobs          chan *edgeJob
	stopChan          chan struct{}
	saveCommitterDone chan struct{}
	edgeCommitterDone chan struct{}
}

// NewServer constructs an AnchorWire server instance.
func NewServer(backend Backend) *Server {
	return NewServerWithJoinManager(backend, nil)
}

// NewServerWithJoinManager constructs an AnchorWire server instance with an optional JoinManager.
func NewServerWithJoinManager(backend Backend, jm *JoinManager) *Server {
	gatewayMode := false
	if jm != nil {
		gatewayMode = jm.IsGatewayMode()
	}
	s := &Server{
		backend:           backend,
		joinManager:       jm,
		gatewayMode:       gatewayMode,
		conns:             make(map[net.Conn]struct{}),
		connSem:           make(chan struct{}, defaultWireMaxConns),
		readTimeout:       defaultWireReadTimeout,
		writeTimeout:      defaultWireWriteTimeout,
		saveJobs:          make(chan *saveJob, 2048),
		edgeJobs:          make(chan *edgeJob, 2048),
		stopChan:          make(chan struct{}),
		saveCommitterDone: make(chan struct{}),
		edgeCommitterDone: make(chan struct{}),
	}
	s.wg.Add(2)
	go s.runGroupCommitter()
	go s.runEdgeGroupCommitter()
	return s
}

// SetTimeouts configures per-frame read and write socket deadlines for new connections.
func (s *Server) SetTimeouts(readTimeout, writeTimeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readTimeout = readTimeout
	s.writeTimeout = writeTimeout
}

// SetMaxConnections updates the maximum concurrent TCP connections accepted by Serve.
func (s *Server) SetMaxConnections(maxConns int) {
	if maxConns <= 0 {
		maxConns = defaultWireMaxConns
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connSem = make(chan struct{}, maxConns)
}

// SetJoinManager assigns the JoinManager.
func (s *Server) SetJoinManager(jm *JoinManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinManager = jm
	if jm != nil {
		s.gatewayMode = jm.IsGatewayMode()
	}
}

// SetTLSConfig configures TLS for subsequent Serve calls.
func (s *Server) SetTLSConfig(cfg *tls.Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg != nil {
		s.tlsConfig = cfg.Clone()
	} else {
		s.tlsConfig = nil
	}
}

// Serve accepts incoming AnchorWire TCP (or TLS if configured) connections.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("server already closed")
	}
	if s.tlsConfig != nil {
		l = tls.NewListener(l, s.tlsConfig.Clone())
	}
	s.l = l
	s.mu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}

		s.mu.Lock()
		if s.closed {
			_ = conn.Close()
			s.mu.Unlock()
			return nil
		}
		sem := s.connSem
		readTimeout := s.readTimeout
		writeTimeout := s.writeTimeout
		select {
		case sem <- struct{}{}:
		default:
			// Connection limit reached; reject immediately so goroutines stay bounded.
			_ = conn.Close()
			s.mu.Unlock()
			continue
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go func(c net.Conn, slot chan struct{}, rt, wt time.Duration) {
			defer func() { <-slot }()
			s.handleConn(c, rt, wt)
		}(conn, sem, readTimeout, writeTimeout)
	}
}

// Close gracefully terminates the server and closes all active connections within the default timeout.
func (s *Server) Close() error {
	return s.CloseWithTimeout(defaultWireCloseTimeout)
}

// CloseWithTimeout terminates the server and waits up to timeout for active handlers to finish.
func (s *Server) CloseWithTimeout(timeout time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stopChan)
	var err error
	if s.l != nil {
		err = s.l.Close()
	}
	for conn := range s.conns {
		_ = conn.Close()
	}
	jm := s.joinManager
	s.mu.Unlock()

	if timeout <= 0 {
		s.wg.Wait()
	} else {
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			err = errors.Join(err, fmt.Errorf("wire server close timed out after %v", timeout))
		}
	}

	if jm != nil {
		if jmErr := jm.Close(); jmErr != nil {
			err = errors.Join(err, jmErr)
		}
	}
	return err
}

func (s *Server) runGroupCommitter() {
	defer close(s.saveCommitterDone)
	defer s.wg.Done()
	const maxBatch = 128

	drainClosing := func() {
		for {
			select {
			case j := <-s.saveJobs:
				j.err = errors.New("server closing")
				close(j.done)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-s.stopChan:
			drainClosing()
			return
		default:
		}

		var firstJob *saveJob
		select {
		case <-s.stopChan:
			drainClosing()
			return
		case firstJob = <-s.saveJobs:
		}

		batch := []*saveJob{firstJob}
	drain:
		for len(batch) < maxBatch {
			select {
			case <-s.stopChan:
				break drain
			case j := <-s.saveJobs:
				batch = append(batch, j)
			default:
				break drain
			}
		}

		s.processSaveBatch(batch)
	}
}

func saveSingleGroupJob(db *storage.AnchorDB, j *saveJob) {
	opStatus := j.item.Operation
	if opStatus == 0 {
		opStatus = storage.OpStatusActive
	}
	if len(j.item.Attrs) > 0 {
		if j.item.Coord != nil || j.item.Roles != nil {
			coord := spatial.Point2D{}
			if j.item.Coord != nil {
				coord = *j.item.Coord
			}
			var roles [4]float32
			if j.item.Roles != nil {
				roles = *j.item.Roles
			}
			j.savedName, j.err = db.SaveRecordTypedWithNode(j.item.Name, opStatus, j.item.Attrs, coord, roles)
		} else {
			j.savedName, j.err = db.SaveRecordTypedWithIDAndOp(j.item.Name, opStatus, j.item.Attrs)
		}
	} else if j.item.Coord != nil || j.item.Roles != nil {
		coord := spatial.Point2D{}
		if j.item.Coord != nil {
			coord = *j.item.Coord
		}
		var roles [4]float32
		if j.item.Roles != nil {
			roles = *j.item.Roles
		}
		j.savedName, j.err = db.SaveRecordWithNode(j.item.Name, opStatus, j.item.Props, coord, roles)
	} else {
		j.savedName, j.err = db.SaveRecordWithIDAndOp(j.item.Name, opStatus, j.item.Props)
	}
	close(j.done)
	if j.err == nil {
		system.RecordMutation(1)
	}
}

func (s *Server) acquireModel(name string) (*storage.AnchorDB, func(), error) {
	if lb, ok := s.backend.(LeaseBackend); ok {
		return lb.AcquireModel(name)
	}
	db, err := s.backend.GetModel(name)
	if err != nil {
		return nil, nil, err
	}
	return db, func() {}, nil
}

func (s *Server) processSaveBatch(batch []*saveJob) {
	if len(batch) == 0 {
		return
	}

	headroom := system.GetMemoryHeadroom()
	if headroom.UsedPercent >= 93 {
		err := fmt.Errorf("storage admission control: container memory pressure %d%%", headroom.UsedPercent)
		for _, j := range batch {
			j.err = err
			close(j.done)
		}
		return
	}

	byModel := make(map[string][]*saveJob)
	for _, j := range batch {
		byModel[j.model] = append(byModel[j.model], j)
	}

	for model, jobs := range byModel {
		db, release, err := s.acquireModel(model)
		if err != nil {
			for _, j := range jobs {
				j.err = err
				close(j.done)
			}
			continue
		}

		func() {
			defer release()
			if headroom.UsedPercent >= 88 {
				db.TriggerAutoSync()
			}

			if len(jobs) == 1 {
				saveSingleGroupJob(db, jobs[0])
				return
			}

			items := make([]storage.BatchRecordItem, len(jobs))
			for i, j := range jobs {
				items[i] = j.item
			}

			names, bErr := db.SaveRecordsBatch(items)
			if bErr != nil {
				if errors.Is(bErr, storage.ErrDatabaseClosed) {
					for _, j := range jobs {
						j.err = bErr
						close(j.done)
					}
					return
				}
				for _, j := range jobs {
					saveSingleGroupJob(db, j)
				}
			} else {
				for i, j := range jobs {
					j.savedName = names[i]
					close(j.done)
				}
				system.RecordMutation(uint64(len(jobs)))
			}
		}()
	}
}

func (s *Server) runEdgeGroupCommitter() {
	defer close(s.edgeCommitterDone)
	defer s.wg.Done()
	const maxBatch = 128

	drainClosing := func() {
		for {
			select {
			case j := <-s.edgeJobs:
				j.err = errors.New("server closing")
				close(j.done)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-s.stopChan:
			drainClosing()
			return
		default:
		}

		var firstJob *edgeJob
		select {
		case <-s.stopChan:
			drainClosing()
			return
		case firstJob = <-s.edgeJobs:
		}

		batch := []*edgeJob{firstJob}
	drain:
		for len(batch) < maxBatch {
			select {
			case <-s.stopChan:
				break drain
			case j := <-s.edgeJobs:
				batch = append(batch, j)
			default:
				break drain
			}
		}

		s.processEdgeBatch(batch)
	}
}

func (s *Server) processEdgeBatch(batch []*edgeJob) {
	if len(batch) == 0 {
		return
	}

	byModel := make(map[string][]*edgeJob)
	for _, j := range batch {
		byModel[j.model] = append(byModel[j.model], j)
	}

	for model, jobs := range byModel {
		db, release, err := s.acquireModel(model)
		if err != nil {
			for _, j := range jobs {
				j.err = err
				close(j.done)
			}
			continue
		}

		func() {
			defer release()
			if len(jobs) == 1 {
				j := jobs[0]
				j.err = db.AddEdge(j.item.Source, j.item.Relation, j.item.Target, j.item.Weight)
				close(j.done)
				if j.err == nil {
					system.RecordMutation(1)
				}
				return
			}

			items := make([]storage.EdgeRecord, len(jobs))
			for i, j := range jobs {
				items[i] = j.item
			}

			bErr := db.AddEdgesBatch(items)
			if bErr != nil {
				if errors.Is(bErr, storage.ErrDatabaseClosed) {
					for _, j := range jobs {
						j.err = bErr
						close(j.done)
					}
					return
				}
				for _, j := range jobs {
					j.err = db.AddEdge(j.item.Source, j.item.Relation, j.item.Target, j.item.Weight)
					close(j.done)
					if j.err == nil {
						system.RecordMutation(1)
					}
				}
			} else {
				for _, j := range jobs {
					close(j.done)
				}
				system.RecordMutation(uint64(len(jobs)))
			}
		}()
	}
}

func (s *Server) removeConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	s.wg.Done()
}

func (s *Server) handleConn(conn net.Conn, readTimeout, writeTimeout time.Duration) {
	defer func() {
		_ = conn.Close()
		s.removeConn(conn)
	}()

	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()
	go func() {
		select {
		case <-s.stopChan:
			cancelConn()
		case <-connCtx.Done():
		}
	}()

	br := bufio.NewReaderSize(conn, 16*1024)
	bw := bufio.NewWriterSize(conn, 16*1024)
	writeResp := func(f Frame) error {
		if writeTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		}
		if err := WriteFrame(bw, f); err != nil {
			return err
		}
		return bw.Flush()
	}

	currentModel := "default"
	authenticated := !s.gatewayMode && !s.backend.RequiresAuth() && !s.joinManager.RequiresAuth()
	var authenticatedNodeID string
	var stagedPendingConfirm bool
	var stagedCommitment [32]byte

	for {
		if readTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		}
		maxFrame := uint32(MaxPayloadSize)
		if !authenticated {
			maxFrame = MaxPreAuthPayloadSize
		}
		req, err := ReadFrameMax(br, maxFrame)
		if err != nil {
			return
		}

		// Piggyback Pruning: confirm operational state upon receiving the first operational frame after reauth.
		if stagedPendingConfirm && req.OpCode != OpAuth {
			if cs := s.joinManager.CommitmentStore(); cs != nil {
				if cErr := cs.ConfirmOperationalExpected(authenticatedNodeID, stagedCommitment); cErr != nil {
					_ = writeResp(Frame{
						Magic:   Magic,
						Version: Version,
						Seq:     req.Seq,
						OpCode:  OpResponseError,
						Payload: EncodeErrorPayload(500, cErr.Error()),
					})
					return
				}
			}
			stagedPendingConfirm = false
			stagedCommitment = [32]byte{}
		}

		resp := Frame{
			Magic:   Magic,
			Version: Version,
			Seq:     req.Seq,
		}

		// 1. Handle Ping
		if req.OpCode == OpPing {
			resp.OpCode = OpPong
			resp.Payload = []byte("PONG")
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 2. Handle Auth
		if req.OpCode == OpAuth {
			// Check if payload is structured (anchor-connector handshake)
			if len(req.Payload) > 0 && req.Payload[0] == StructuredAuthMagic {
				authReq, dErr := DecodeStructuredAuth(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, "invalid structured auth payload")
					if wErr := writeResp(resp); wErr != nil {
						return
					}
					continue
				}

				if s.joinManager != nil {
					sessKey, errCode, aErr := s.joinManager.HandleStructuredAuth(authReq, conn.RemoteAddr())
					if aErr != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(errCode, aErr.Error())
						_ = writeResp(resp)
						return
					}
					if authReq.Type == AuthTypeStatic && !s.joinManager.HasStaticToken() && !s.backend.Authenticate(authReq.Credential) {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: invalid credentials")
						_ = writeResp(resp)
						return
					}
					authenticated = true
					resp.OpCode = OpResponseOK
					if authReq.Type == AuthTypeStagedReauth {
						authenticatedNodeID = authReq.NodeID
						stagedPendingConfirm = true
						stagedCommitment = authReq.NextCommitment
						resp.Payload = EncodeRotationStagedResponse("Commitment staged successfully")
					} else if sessKey != "" {
						resp.Payload = EncodeAuthResponse(sessKey)
					} else {
						resp.Payload = EncodeOKPayload("authenticated")
					}
				} else {
					if authReq.Type != AuthTypeStatic {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(501, "structured auth mode requires JoinManager")
						_ = writeResp(resp)
						return
					}
					if s.backend.Authenticate(authReq.Credential) {
						authenticated = true
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeOKPayload("authenticated")
					} else {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: invalid credentials")
						_ = writeResp(resp)
						return
					}
				}
				if err := writeResp(resp); err != nil {
					return
				}
				continue
			}

			// Legacy auth payload (plain string token)
			if s.gatewayMode {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: direct client access disabled in gateway mode; connection must route through anchor-connector")
				_ = writeResp(resp)
				return
			}

			token, dErr := DecodeAuthPayload(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, "invalid auth payload")
				if wErr := writeResp(resp); wErr != nil {
					return
				}
				continue
			}
			if s.joinManager != nil {
				_, errCode, aErr := s.joinManager.HandleStructuredAuth(AuthRequest{
					Type:       AuthTypeStatic,
					Credential: token,
				}, conn.RemoteAddr())
				if aErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(errCode, aErr.Error())
					_ = writeResp(resp)
					return
				}
				if !s.joinManager.HasStaticToken() && !s.backend.Authenticate(token) {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: invalid token")
					_ = writeResp(resp)
					return
				}
				authenticated = true
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload("authenticated")
			} else if s.backend.Authenticate(token) {
				authenticated = true
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload("authenticated")
			} else {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(401, "unauthorized: invalid token")
				_ = writeResp(resp)
				return
			}
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// Enforce authentication
		if !authenticated {
			resp.OpCode = OpResponseError
			resp.Payload = EncodeErrorPayload(401, "authentication required: send OpAuth first")
			_ = writeResp(resp)
			return
		}

		// 3. Handle SelectModel
		if req.OpCode == OpSelectModel {
			model, dErr := DecodeSelectModelPayload(req.Payload)
			if dErr != nil || model == "" {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, "invalid model name")
			} else {
				currentModel = model
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload("model selected: " + currentModel)
			}
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 3.5. Handle OpNegotiateMemory
		if req.OpCode == OpNegotiateMemory {
			negoReq, dErr := DecodeMemoryNegotiationRequest(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, "invalid memory negotiation request: "+dErr.Error())
				if wErr := writeResp(resp); wErr != nil {
					return
				}
				continue
			}

			headroom := system.GetMemoryHeadroom()
			var status uint8
			var releaseIntervalMs uint32
			allocatedBatch := negoReq.ClientBatchSize
			if allocatedBatch == 0 {
				allocatedBatch = 1000
			}

			usedPct := headroom.UsedPercent
			if usedPct < 75 {
				status = MemoryStatusGranted
				if allocatedBatch > 2000 {
					allocatedBatch = 2000
				}
				releaseIntervalMs = 0
			} else if usedPct < 88 {
				// Moderate pressure
				status = MemoryStatusThrottled
				if allocatedBatch > 1000 {
					allocatedBatch = 1000
				}
				releaseIntervalMs = 5 // gentle pacing
			} else {
				// High pressure (>88% non-reclaimable RAM consumed)
				status = MemoryStatusHold
				if allocatedBatch > 500 {
					allocatedBatch = 500
				}
				releaseIntervalMs = 20
			}

			resp.OpCode = OpResponseOK
			if status != MemoryStatusGranted {
				resp.Flags |= FlagMemoryPressure
			}
			resp.Payload = EncodeMemoryNegotiationResponse(MemoryNegotiationResponse{
				Status:             status,
				HostUsedPct:        usedPct,
				HeadroomBytes:      headroom.HeadroomBytes,
				AllocatedBatchSize: allocatedBatch,
				ReleaseIntervalMs:  releaseIntervalMs,
			})
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 3.6. Handle OpPartitionCatalog
		if req.OpCode == OpPartitionCatalog {
			model, _ := DecodePartitionCatalogRequest(req.Payload)
			if model == "" {
				model = currentModel
			}
			pb, ok := s.backend.(PartitionBackend)
			if !ok {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(501, "partition catalog backend not supported")
			} else {
				items, cErr := pb.GetPartitionCatalog(model)
				if cErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(500, cErr.Error())
				} else {
					payload, encErr := EncodePartitionCatalogResponse(items)
					if encErr != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(500, encErr.Error())
					} else {
						resp.OpCode = OpResponseOK
						resp.Payload = payload
					}
				}
			}
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 3.7. Handle OpPartitionInspect
		if req.OpCode == OpPartitionInspect {
			inspectReq, dErr := DecodePartitionInspectRequest(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, dErr.Error())
			} else {
				pb, ok := s.backend.(PartitionBackend)
				if !ok {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(501, "partition inspect backend not supported")
				} else {
					inspectResp, iErr := pb.InspectPartition(inspectReq.PartitionFile, inspectReq.RecordName, inspectReq.Limit)
					if iErr != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(500, iErr.Error())
					} else {
						payload, encErr := EncodePartitionInspectResponse(inspectResp)
						if encErr != nil {
							resp.OpCode = OpResponseError
							resp.Payload = EncodeErrorPayload(500, encErr.Error())
						} else {
							resp.OpCode = OpResponseOK
							resp.Payload = payload
						}
					}
				}
			}
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 4. Resolve Model Database
		db, release, err := s.acquireModel(currentModel)
		if err != nil {
			resp.OpCode = OpResponseError
			resp.Payload = EncodeErrorPayload(500, fmt.Sprintf("failed to get model %q: %v", currentModel, err))
			if err := writeResp(resp); err != nil {
				return
			}
			continue
		}

		// 5. Dispatch Command
		func() {
			defer release()
			switch req.OpCode {
			case OpSave:
				item, dErr := DecodeSavePayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}

				job := &saveJob{
					model: currentModel,
					item:  item,
					done:  make(chan struct{}),
				}

				select {
				case s.saveJobs <- job:
					select {
					case <-job.done:
						if job.err != nil {
							if strings.Contains(job.err.Error(), "admission control") {
								resp.OpCode = OpResponseError
								resp.Payload = EncodeErrorPayload(507, job.err.Error())
							} else {
								resp.OpCode = OpResponseError
								resp.Payload = EncodeErrorPayload(wireErrorCode(job.err), job.err.Error())
							}
						} else {
							resp.OpCode = OpResponseOK
							resp.Payload = EncodeOKPayload(job.savedName)
						}
					case <-s.saveCommitterDone:
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(503, "server stopping")
					}
				case <-s.stopChan:
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(503, "server stopping")
				case <-s.saveCommitterDone:
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(503, "server stopping")
				}

				headroom := system.GetMemoryHeadroom()
				if headroom.UsedPercent >= 85 {
					resp.Flags |= FlagMemoryPressure
				}

			case OpSaveBatch:
				items, dErr := DecodeSaveBatchPayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				headroom := system.GetMemoryHeadroom()
				if headroom.UsedPercent >= 85 {
					resp.Flags |= FlagMemoryPressure
				}
				if headroom.UsedPercent >= 88 {
					db.TriggerAutoSync()
				}
				if headroom.UsedPercent >= 93 {
					resp.OpCode = OpResponseError
					resp.Flags |= FlagMemoryPressure
					resp.Payload = EncodeErrorPayload(507, fmt.Sprintf("storage admission control: container memory pressure %d%%", headroom.UsedPercent))
					break
				}
				names, err := db.SaveRecordsBatch(items)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					system.RecordMutation(uint64(len(items)))
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload(fmt.Sprintf("%d", len(names)))
				}

			case OpGet:
				getReq, dErr := DecodeGetRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				var props map[string]float32
				var op uint8
				var attrs []storage.AttrEntry
				isTyped := false

				if len(getReq.Computations) > 0 || len(getReq.Select) > 0 {
					cr, cErr := db.GetRecordTypedWithCompute(getReq.Name, getReq.Computations, getReq.Select...)
					if cErr == nil {
						op = cr.Operation
						attrs = cr.ExpandAttrs()
						isTyped = true
					} else {
						err = cErr
					}
				} else {
					cr, cErr := db.GetRecordTyped(getReq.Name)
					if cErr == nil {
						op = cr.Operation
						attrs = cr.ExpandAttrs()
						isTyped = true
					} else {
						err = cErr
					}
				}
				if err != nil && errors.Is(err, storage.ErrRecordNotFound) {
					// Fallback to Cold Storage Tier only for missing records; preserve
					// real non-not-found cold errors (I/O, closed DB, compute, S3)
					// instead of masking them as 404.
					if cb, ok := s.backend.(ColdTierBackend); ok {
						cr, coldErr := cb.GetColdRecordTyped(currentModel, getReq.Name, getReq.Computations, getReq.Select)
						if coldErr == nil {
							err = nil
							op = cr.Operation
							attrs = cr.ExpandAttrs()
							isTyped = true
							resp.Flags |= FlagColdTier
						} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
							err = coldErr
						}
					} else if ttb, ok := s.backend.(TypedTieredBackend); ok {
						cr, coldErr := ttb.GetRecordTypedWithComputeTiered(currentModel, getReq.Name, getReq.Computations, getReq.Select)
						if coldErr == nil {
							err = nil
							op = cr.Operation
							attrs = cr.ExpandAttrs()
							isTyped = true
							resp.Flags |= FlagColdTier
						} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
							err = coldErr
						}
					} else if tb, ok := s.backend.(TieredBackend); ok {
						var coldErr error
						if len(getReq.Computations) > 0 || len(getReq.Select) > 0 {
							props, op, coldErr = tb.GetRecordWithComputeTiered(currentModel, getReq.Name, getReq.Computations, getReq.Select)
						} else {
							props, op, coldErr = tb.GetRecordTiered(currentModel, getReq.Name)
						}
						if coldErr == nil {
							err = nil
							isTyped = false
							resp.Flags |= FlagColdTier
						} else if !errors.Is(coldErr, storage.ErrRecordNotFound) {
							err = coldErr
						}
					}
				}
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					if isTyped {
						resp.Payload = EncodeGetResultTypedPayload(op, attrs)
					} else {
						resp.Payload = EncodeGetResultPayload(op, props)
					}
				}

			case OpEdge:
				action, src, rel, tgt, weight, direction, dErr := DecodeEdgePayloadWithDirection(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				switch action {
				case "remove":
					err = db.RemoveEdge(src, rel, tgt)
					if err == nil {
						system.RecordMutation(1)
					}
				case "set_relation":
					if direction == nil {
						err = errors.New("direction is required for set_relation")
					} else {
						err = db.SetRelation(rel, *direction)
						if err == nil {
							system.RecordMutation(1)
						}
					}
				default:
					job := &edgeJob{
						model: currentModel,
						item: storage.EdgeRecord{
							Source:   src,
							Relation: rel,
							Target:   tgt,
							Weight:   weight,
						},
						done: make(chan struct{}),
					}
					select {
					case s.edgeJobs <- job:
						select {
						case <-job.done:
							err = job.err
						case <-s.edgeCommitterDone:
							err = errors.New("server stopping")
						}
					case <-s.stopChan:
						err = errors.New("server stopping")
					case <-s.edgeCommitterDone:
						err = errors.New("server stopping")
					}
				}
				if err != nil {
					code := wireErrorCode(err)
					if code == 500 {
						code = 400
					}
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(code, err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload(fmt.Sprintf("%d", db.EdgeCount()))
				}

			case OpCausal:
				seeds, rel, limitK, dErr := DecodeCausalPayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				results, err := db.ExecuteCausalStep(connCtx, seeds, rel, limitK)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeCausalResultPayload(results)
				}

			case OpCausalChain:
				seeds, relations, limitK, dErr := DecodeCausalChainPayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				results, err := db.ExecuteCausalChain(connCtx, seeds, relations, limitK)
				if err != nil {
					code := wireErrorCode(err)
					if code == 500 {
						code = 400
					}
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(code, err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeCausalResultPayload(results)
				}

			case OpSync:
				if err := db.Sync(); err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload("synced")
				}

			case OpList:
				listReq, dErr := DecodeListRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				opts := storage.QueryOptions{
					Context:              connCtx,
					Limit:                listReq.Limit,
					Offset:               listReq.Offset,
					IncludeAllOperations: listReq.IncludeAllOperations,
					OperationFilter:      listReq.OperationFilter,
					AllowedOperations:    listReq.AllowedOperations,
					Select:               listReq.Select,
					Filters:              listReq.Filters,
					Computations:         listReq.Computations,
				}
				items, hasMore, err := db.ListRecordsWithPaging(opts)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeListResponse(ListResponse{
						TotalCount: len(items),
						HasMore:    hasMore,
						Items:      items,
					})
				}

			case OpUpdate:
				upReq, dErr := DecodeUpdateRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				opts := storage.UpdateOptions{
					Context:        connCtx,
					Limit:          upReq.Limit,
					Offset:         upReq.Offset,
					TargetEntities: upReq.TargetEntities,
					Filters:        upReq.Filters,
					Mutations:      upReq.Mutations,
				}
				updatedCount, err := db.UpdateRecords(opts)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					if updatedCount > 0 {
						system.RecordMutation(uint64(updatedCount))
					}
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeUpdateResponse(updatedCount)
				}

			case OpGraph:
				gReq, dErr := DecodeGraphRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				results, anchor, err := db.ExecuteGraphStepWithOptions(gReq.Inputs, gReq.Relation, gReq.LimitK, storage.OrderByMode(gReq.OrderMode), gReq.BeamWidth)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeGraphResponsePayload(GraphResponsePayload{
						Anchor:  anchor,
						Results: results,
					})
				}

			case OpGraphChain:
				cReq, dErr := DecodeGraphChainRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				results, err := db.ExecuteGraphChainWithOptions(cReq.Inputs, cReq.RelationChain, cReq.LimitK, cReq.BeamWidth)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeGraphResponsePayload(GraphResponsePayload{
						Results: results,
					})
				}

			case OpAttachSchema:
				schemaRaw, dErr := DecodeAttachSchemaPayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				var ms storage.ModelSchema
				if err := json.Unmarshal([]byte(schemaRaw), &ms); err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, fmt.Sprintf("invalid schema JSON: %v", err))
					break
				}
				if err := db.AttachSchema(ms); err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload("schema attached")
				}

			case OpGetSchema:
				ms, ok := db.GetSchema()
				if !ok {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(404, "no schema attached to model")
				} else if data, mErr := json.Marshal(ms); mErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(500, fmt.Sprintf("failed to marshal schema: %v", mErr))
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeGetSchemaPayload(string(data))
				}

			case OpMath:
				mReq, dErr := DecodeMathRequest(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				mathErrCode := func(err error) uint16 {
					code := wireErrorCode(err)
					if code == 500 {
						return 404
					}
					return code
				}
				switch mReq.Op {
				case "distance":
					if len(mReq.Words) < 2 {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(400, "distance requires 2 words")
						break
					}
					dist, err := db.CalculateDistance(mReq.Words[0], mReq.Words[1])
					if err != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(mathErrCode(err), err.Error())
					} else {
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeMathResponse(MathResponse{Value: dist})
					}
				case "centroid":
					coord, roles, err := db.CalculateCentroid(mReq.Words)
					if err != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(mathErrCode(err), err.Error())
					} else {
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeMathResponse(MathResponse{Coord: coord, Roles: roles})
					}
				case "cosine_similarity":
					if len(mReq.Words) < 2 {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(400, "cosine_similarity requires 2 words")
						break
					}
					sim, err := db.CalculateCosineSimilarity(mReq.Words[0], mReq.Words[1])
					if err != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(mathErrCode(err), err.Error())
					} else {
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeMathResponse(MathResponse{Value: sim})
					}
				case "nearest":
					if len(mReq.Words) < 1 {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(400, "nearest requires 1 word")
						break
					}
					neighbors, err := db.FindNearestNeighbors(mReq.Words[0], mReq.LimitK)
					if err != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(mathErrCode(err), err.Error())
					} else {
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeMathResponse(MathResponse{Neighbors: neighbors})
					}
				default:
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, "unsupported math op: "+mReq.Op)
				}

			case OpResetModel:
				confirm, dErr := DecodeResetModelPayload(req.Payload)
				if dErr != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, dErr.Error())
					break
				}
				if !confirm {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(400, "confirm must be true to reset model schema")
					break
				}
				if err := db.ResetSchema(); err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(wireErrorCode(err), err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload("schema reset")
				}

			default:
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, fmt.Sprintf("unsupported wire opcode: 0x%02X", req.OpCode))
			}
		}()

		if (req.Flags & FlagOneway) == 0 {
			if err := writeResp(resp); err != nil {
				return
			}
		} else if resp.OpCode == OpResponseError {
			code, msg, _ := DecodeErrorPayload(resp.Payload)
			log.Printf("[WIRE] Oneway op 0x%02X failed on model %q (code %d): %s", req.OpCode, currentModel, code, msg)
		}
	}
}
