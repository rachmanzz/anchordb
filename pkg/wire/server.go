package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
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
			names, err := db.SaveRecordsBatch(items)
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(500, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeOKPayload(fmt.Sprintf("%d", len(names)))
			}

		case OpGet:
			name, dErr := DecodeGetPayload(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, dErr.Error())
				break
			}
			props, op, err := db.GetRecordWithOperation(name)
			if err != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(404, err.Error())
			} else {
				resp.OpCode = OpResponseOK
				resp.Payload = EncodeGetResultPayload(op, props)
			}

		case OpEdge:
			action, src, rel, tgt, weight, dErr := DecodeEdgePayload(req.Payload)
			if dErr != nil {
				resp.OpCode = OpResponseError
				resp.Payload = EncodeErrorPayload(400, dErr.Error())
				break
			}
			if action == "remove" {
				err = db.RemoveEdge(src, rel, tgt)
			} else {
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
