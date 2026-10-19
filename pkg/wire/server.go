package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
	"github.com/rachmanzz/anchordb/pkg/system"
)

// Backend defines the storage interface wire.Server requires from AnchorDB.
type Backend interface {
	GetModel(name string) (*storage.AnchorDB, error)
	Authenticate(token string) bool
	RequiresAuth() bool
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

// Server is the AnchorWire binary TCP protocol daemon.
type Server struct {
	backend     Backend
	joinManager *JoinManager
	gatewayMode bool
	l           net.Listener
	mu          sync.Mutex
	conns       map[net.Conn]struct{}
	closed      bool
	wg          sync.WaitGroup
	saveJobs    chan *saveJob
	edgeJobs    chan *edgeJob
	stopChan    chan struct{}
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
		backend:     backend,
		joinManager: jm,
		gatewayMode: gatewayMode,
		conns:       make(map[net.Conn]struct{}),
		saveJobs:    make(chan *saveJob, 2048),
		edgeJobs:    make(chan *edgeJob, 2048),
		stopChan:    make(chan struct{}),
	}
	s.wg.Add(2)
	go s.runGroupCommitter()
	go s.runEdgeGroupCommitter()
	return s
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

// Serve accepts incoming AnchorWire TCP connections.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("server already closed")
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
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

// Close gracefully terminates the server and closes all active connections.
func (s *Server) Close() error {
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
	s.mu.Unlock()

	s.wg.Wait()
	return err
}

func (s *Server) runGroupCommitter() {
	defer s.wg.Done()
	const maxBatch = 128

	for {
		var firstJob *saveJob
		select {
		case <-s.stopChan:
			for {
				select {
				case j := <-s.saveJobs:
					j.err = errors.New("server closing")
					close(j.done)
				default:
					return
				}
			}
		case firstJob = <-s.saveJobs:
		}

		batch := []*saveJob{firstJob}
	drain:
		for len(batch) < maxBatch {
			select {
			case j := <-s.saveJobs:
				batch = append(batch, j)
			default:
				break drain
			}
		}

		s.processSaveBatch(batch)
	}
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
		db, err := s.backend.GetModel(model)
		if err != nil {
			for _, j := range jobs {
				j.err = err
				close(j.done)
			}
			continue
		}

		if headroom.UsedPercent >= 88 {
			db.TriggerAutoSync()
		}

		if len(jobs) == 1 {
			j := jobs[0]
			opStatus := j.item.Operation
			if opStatus == 0 {
				opStatus = storage.OpStatusActive
			}
			if j.item.Coord != nil || j.item.Roles != nil {
				coord := spatial.Point2D{}
				if j.item.Coord != nil {
					coord = *j.item.Coord
				}
				var roles [4]float32
				if j.item.Roles != nil {
					roles = *j.item.Roles
				}
				if len(j.item.Props) > 0 {
					j.savedName, j.err = db.SaveRecordWithNode(j.item.Name, opStatus, j.item.Props, coord, roles)
				} else {
					j.savedName = j.item.Name
					_, j.err = db.AppendNode(j.savedName, coord, roles, 1)
				}
			} else {
				j.savedName, j.err = db.SaveRecordWithIDAndOp(j.item.Name, opStatus, j.item.Props)
			}
			close(j.done)
			if j.err == nil {
				system.RecordMutation(1)
			}
			continue
		}

		items := make([]storage.BatchRecordItem, len(jobs))
		for i, j := range jobs {
			items[i] = j.item
		}

		names, bErr := db.SaveRecordsBatch(items)
		if bErr != nil {
			for _, j := range jobs {
				opStatus := j.item.Operation
				if opStatus == 0 {
					opStatus = storage.OpStatusActive
				}
				if j.item.Coord != nil || j.item.Roles != nil {
					coord := spatial.Point2D{}
					if j.item.Coord != nil {
						coord = *j.item.Coord
					}
					var roles [4]float32
					if j.item.Roles != nil {
						roles = *j.item.Roles
					}
					if len(j.item.Props) > 0 {
						j.savedName, j.err = db.SaveRecordWithNode(j.item.Name, opStatus, j.item.Props, coord, roles)
					} else {
						j.savedName = j.item.Name
						_, j.err = db.AppendNode(j.savedName, coord, roles, 1)
					}
				} else {
					j.savedName, j.err = db.SaveRecordWithIDAndOp(j.item.Name, opStatus, j.item.Props)
				}
				close(j.done)
				if j.err == nil {
					system.RecordMutation(1)
				}
			}
		} else {
			for i, j := range jobs {
				j.savedName = names[i]
				close(j.done)
			}
			system.RecordMutation(uint64(len(jobs)))
		}
	}
}

func (s *Server) runEdgeGroupCommitter() {
	defer s.wg.Done()
	const maxBatch = 128

	for {
		var firstJob *edgeJob
		select {
		case <-s.stopChan:
			for {
				select {
				case j := <-s.edgeJobs:
					j.err = errors.New("server closing")
					close(j.done)
				default:
					return
				}
			}
		case firstJob = <-s.edgeJobs:
		}

		batch := []*edgeJob{firstJob}
	drain:
		for len(batch) < maxBatch {
			select {
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
		db, err := s.backend.GetModel(model)
		if err != nil {
			for _, j := range jobs {
				j.err = err
				close(j.done)
			}
			continue
		}

		if len(jobs) == 1 {
			j := jobs[0]
			j.err = db.AddEdge(j.item.Source, j.item.Relation, j.item.Target, j.item.Weight)
			close(j.done)
			if j.err == nil {
				system.RecordMutation(1)
			}
			continue
		}

		items := make([]storage.EdgeRecord, len(jobs))
		for i, j := range jobs {
			items[i] = j.item
		}

		bErr := db.AddEdgesBatch(items)
		if bErr != nil {
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
	}
}

func (s *Server) removeConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	s.wg.Done()
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.removeConn(conn)
	}()

	currentModel := "default"
	authenticated := !s.gatewayMode && !s.backend.RequiresAuth()
	var authenticatedNodeID string
	var stagedPendingConfirm bool

	for {
		req, err := ReadFrame(conn)
		if err != nil {
			return
		}

		// Piggyback Pruning: confirm operational state upon receiving the first operational frame after reauth
		if stagedPendingConfirm && req.OpCode != OpAuth {
			if s.joinManager != nil && s.joinManager.CommitmentStore() != nil {
				_ = s.joinManager.CommitmentStore().ConfirmOperational(authenticatedNodeID)
			}
			stagedPendingConfirm = false
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
			if err := WriteFrame(conn, resp); err != nil {
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
					_ = WriteFrame(conn, resp)
					continue
				}

				if s.joinManager != nil {
					sessKey, errCode, aErr := s.joinManager.HandleStructuredAuth(authReq, conn.RemoteAddr())
					if aErr != nil {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(errCode, aErr.Error())
						_ = WriteFrame(conn, resp)
						return
					}
					authenticated = true
					resp.OpCode = OpResponseOK
					if authReq.Type == AuthTypeStagedReauth {
						authenticatedNodeID = authReq.NodeID
						stagedPendingConfirm = true
						resp.Payload = EncodeRotationStagedResponse("Commitment staged successfully")
					} else if sessKey != "" {
						resp.Payload = EncodeAuthResponse(sessKey)
					} else {
						resp.Payload = EncodeOKPayload("authenticated")
					}
				} else {
					// Fallback when joinManager is nil: evaluate static token
					if authReq.Type == AuthTypeStatic && s.backend.Authenticate(authReq.Credential) {
						authenticated = true
						resp.OpCode = OpResponseOK
						resp.Payload = EncodeAuthResponse("SESS.default." + authReq.NodeID + ".static")
					} else {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: invalid credentials")
						_ = WriteFrame(conn, resp)
						return
					}
				}
				if err := WriteFrame(conn, resp); err != nil {
					return
				}
				continue
			}

			// Legacy auth payload (plain string token)
			if s.gatewayMode {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(ErrCodeUnauthorized, "unauthorized: direct client access disabled in gateway mode; connection must route through anchor-connector")
				_ = WriteFrame(conn, resp)
				return
			}

			token, dErr := DecodeAuthPayload(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, "invalid auth payload")
				_ = WriteFrame(conn, resp)
				continue
			}
			if s.backend.Authenticate(token) {
				authenticated = true
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload("authenticated")
			} else {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(401, "unauthorized: invalid token")
				_ = WriteFrame(conn, resp)
				return
			}
			if err := WriteFrame(conn, resp); err != nil {
				return
			}
			continue
		}

		// Enforce authentication
		if !authenticated {
			resp.OpCode = OpResponseError
			resp.Payload = EncodeErrorPayload(401, "authentication required: send OpAuth first")
			_ = WriteFrame(conn, resp)
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
			if err := WriteFrame(conn, resp); err != nil {
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
				_ = WriteFrame(conn, resp)
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
			if err := WriteFrame(conn, resp); err != nil {
				return
			}
			continue
		}

		// 4. Resolve Model Database
		db, err := s.backend.GetModel(currentModel)
		if err != nil {
			resp.OpCode = OpResponseError
			resp.Payload = EncodeErrorPayload(500, fmt.Sprintf("failed to get model %q: %v", currentModel, err))
			if err := WriteFrame(conn, resp); err != nil {
				return
			}
			continue
		}

		// 5. Dispatch Command
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
				<-job.done
				if job.err != nil {
					if strings.Contains(job.err.Error(), "admission control") {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(507, job.err.Error())
					} else {
						resp.OpCode = OpResponseError
						resp.Payload = EncodeErrorPayload(500, job.err.Error())
					}
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeOKPayload(job.savedName)
				}
			case <-s.stopChan:
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
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
			if len(getReq.Computations) > 0 || len(getReq.Select) > 0 {
				props, err = db.GetRecordWithCompute(getReq.Name, getReq.Computations, getReq.Select...)
				if err == nil {
					_, op, _ = db.GetRecordWithOperation(getReq.Name)
				}
			} else {
				props, op, err = db.GetRecordWithOperation(getReq.Name)
			}
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(404, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeGetResultPayload(op, props)
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
					<-job.done
					err = job.err
				case <-s.stopChan:
					err = errors.New("server stopping")
				}
			}
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, err.Error())
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
			results, err := db.ExecuteCausalStep(context.Background(), seeds, rel, limitK)
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeCausalResultPayload(results)
			}

		case OpSync:
			if err := db.Sync(); err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
				Context:              context.Background(),
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
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
				Context:        context.Background(),
				Limit:          upReq.Limit,
				Offset:         upReq.Offset,
				TargetEntities: upReq.TargetEntities,
				Filters:        upReq.Filters,
				Mutations:      upReq.Mutations,
			}
			updatedCount, err := db.UpdateRecords(opts)
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
				resp.Payload = EncodeErrorPayload(500, err.Error())
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
			} else {
				data, _ := json.Marshal(ms)
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
					resp.Payload = EncodeErrorPayload(404, err.Error())
				} else {
					resp.OpCode = OpResponseOK
					resp.Payload = EncodeMathResponse(MathResponse{Value: dist})
				}
			case "centroid":
				coord, roles, err := db.CalculateCentroid(mReq.Words)
				if err != nil {
					resp.OpCode = OpResponseError
					resp.Payload = EncodeErrorPayload(404, err.Error())
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
					resp.Payload = EncodeErrorPayload(404, err.Error())
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
					resp.Payload = EncodeErrorPayload(404, err.Error())
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
				resp.Payload = EncodeErrorPayload(500, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload("schema reset")
			}

		default:
			resp.OpCode = OpResponseError
			resp.Payload = EncodeErrorPayload(400, fmt.Sprintf("unsupported wire opcode: 0x%02X", req.OpCode))
		}

		if (req.Flags & FlagOneway) == 0 {
			if err := WriteFrame(conn, resp); err != nil {
				return
			}
		}
	}
}
