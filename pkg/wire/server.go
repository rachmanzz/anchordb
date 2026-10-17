package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

// Server is the AnchorWire binary TCP protocol daemon.
type Server struct {
	backend Backend
	l       net.Listener
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closed  bool
	wg      sync.WaitGroup
}

// NewServer constructs an AnchorWire server instance.
func NewServer(backend Backend) *Server {
	return &Server{
		backend: backend,
		conns:   make(map[net.Conn]struct{}),
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
	authenticated := !s.backend.RequiresAuth()

	for {
		req, err := ReadFrame(conn)
		if err != nil {
			return
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
			if usedPct < 60 {
				status = MemoryStatusGranted
				if allocatedBatch > 2000 {
					allocatedBatch = 2000
				}
				releaseIntervalMs = 0
			} else if usedPct < 80 {
				// Moderate pressure (e.g. Neo4j consumes 60% host RAM)
				status = MemoryStatusThrottled
				if allocatedBatch > 500 {
					allocatedBatch = 500
				}
				releaseIntervalMs = 15 // pace releases with 15ms delay per batch
			} else {
				// High pressure (>80% RAM consumed)
				status = MemoryStatusHold
				if allocatedBatch > 100 {
					allocatedBatch = 100
				}
				releaseIntervalMs = 100 // hold releases
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
			var savedName string
			opStatus := item.Operation
			if opStatus == 0 {
				opStatus = storage.OpStatusActive
			}
			if item.Coord != nil || item.Roles != nil {
				coord := spatial.Point2D{}
				if item.Coord != nil {
					coord = *item.Coord
				}
				var roles [4]float32
				if item.Roles != nil {
					roles = *item.Roles
				}
				savedName, err = db.SaveRecordWithNode(item.Name, opStatus, item.Props, coord, roles)
			} else {
				savedName, err = db.SaveRecordWithIDAndOp(item.Name, opStatus, item.Props)
			}
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload(savedName)
			}

		case OpSaveBatch:
			items, dErr := DecodeSaveBatchPayload(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, dErr.Error())
				break
			}
			headroom := system.GetMemoryHeadroom()
			if headroom.UsedPercent >= 80 {
				resp.Flags |= FlagMemoryPressure
			}
			names, err := db.SaveRecordsBatch(items)
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
			} else {
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
			case "set_relation":
				if direction == nil {
					err = errors.New("direction is required for set_relation")
				} else {
					err = db.SetRelation(rel, *direction)
				}
			default:
				err = db.AddEdge(src, rel, tgt, weight)
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
