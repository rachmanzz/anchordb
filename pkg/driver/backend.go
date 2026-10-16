package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rachmanzz/anchordb/pkg/spatial"
	"github.com/rachmanzz/anchordb/pkg/storage"
)

// backend defines the internal execution contract shared by local and remote engine drivers.
type backend interface {
	Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error)
	Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error)
	List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error)
	Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error)
	Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error)
	GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error)
	Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error
	Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error)
	Distance(ctx context.Context, model, wordA, wordB string) (float32, error)
	Centroid(ctx context.Context, model string, words []string) (CentroidResult, error)
	CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error)
	Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error)
	AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error
	SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error
	Sync(ctx context.Context, model string) error
	Close() error
	DB() *storage.AnchorDB
	IsRemote() bool
}

// -----------------------------------------------------------------------------
// Local Backend (Embedded / In-Memory / File)
// -----------------------------------------------------------------------------

type localBackend struct {
	db *storage.AnchorDB
}

func newLocalBackend(filePath string) (*localBackend, error) {
	db, err := storage.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local AnchorDB at %s: %w", filePath, err)
	}
	return &localBackend{db: db}, nil
}

func (b *localBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	opStatus := storage.OpStatusActive
	if op != nil {
		opStatus = *op
	}
	return b.db.SaveRecordWithIDAndOp(name, opStatus, props)
}

func (b *localBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var props map[string]float32
	var err error
	if len(computations) == 0 {
		props, err = b.db.GetRecord(name, selectFields...)
	} else {
		props, err = b.db.GetRecordWithCompute(name, computations, selectFields...)
	}
	if err != nil {
		return nil, err
	}
	return &EntityResult{Name: name, props: props}, nil
}

func (b *localBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts.Context = ctx
	records, hasMore, err := b.db.ListRecordsWithPaging(opts)
	if err != nil {
		return nil, err
	}
	items := make([]EntityResult, len(records))
	for i, r := range records {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: hasMore,
	}, nil
}

func (b *localBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	opts.Context = ctx
	return b.db.UpdateRecords(opts)
}

func (b *localBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results, anchor, err := b.db.ExecuteGraphStepWithContext(ctx, inputs, relation, limitK, orderMode, beamWidth)
	if err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: anchor, Results: results}, nil
}

func (b *localBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	steps, err := b.db.ExecuteGraphChainWithContext(ctx, inputs, relations, limitK, beamWidth)
	if err != nil {
		return nil, err
	}
	seq := make([]string, len(steps))
	for i, step := range steps {
		seq[i] = step.Word
	}
	return &ChainResponse{
		Sequence: seq,
		Steps:    steps,
	}, nil
}

func (b *localBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch strings.ToLower(action) {
	case "remove", "delete", "unlink":
		return b.db.RemoveEdge(source, relation, target)
	default:
		return b.db.AddEdge(source, relation, target, weight)
	}
}

func (b *localBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(relations) > 0 {
		return b.db.ExecuteCausalChain(ctx, seeds, relations, limitK)
	}
	return b.db.ExecuteCausalStep(ctx, seeds, relation, limitK)
}

func (b *localBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	return b.db.CalculateDistance(wordA, wordB)
}

func (b *localBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	coord, roles, err := b.db.CalculateCentroid(words)
	return CentroidResult{Coord: coord, Roles: roles}, err
}

func (b *localBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	return b.db.CalculateCosineSimilarity(wordA, wordB)
}

func (b *localBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	return b.db.FindNearestNeighbors(word, limitK)
}

func (b *localBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := b.db.AppendNode(word, coord, roles, 1)
	return err
}

func (b *localBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.db.SetRelation(name, direction)
}

func (b *localBackend) Sync(ctx context.Context, model string) error {
	return b.db.Sync()
}

func (b *localBackend) Close() error {
	return b.db.Close()
}

func (b *localBackend) DB() *storage.AnchorDB {
	return b.db
}

func (b *localBackend) IsRemote() bool {
	return false
}

// -----------------------------------------------------------------------------
// Remote Backend (HTTP Engine Client)
// -----------------------------------------------------------------------------

type remoteBackend struct {
	baseURL    string
	cfg        clientConfig
	httpClient *http.Client
}

func newRemoteBackend(baseURL string, cfg clientConfig) (*remoteBackend, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	client := cfg.httpClient
	if client == nil {
		client = &http.Client{
			Timeout: cfg.timeout,
		}
	}
	return &remoteBackend{
		baseURL:    baseURL,
		cfg:        cfg,
		httpClient: client,
	}, nil
}

func (b *remoteBackend) doJSON(ctx context.Context, method, endpoint string, reqPayload, respPayload any) error {
	urlStr := b.baseURL + endpoint

	var bodyReader io.Reader
	if reqPayload != nil {
		data, err := json.Marshal(reqPayload)
		if err != nil {
			return fmt.Errorf("failed to marshal request payload: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	if reqPayload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.cfg.token)
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("remote engine request failed (%s %s): %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(bodyBytes, &errResp); jsonErr == nil && errResp.Error != "" {
			return fmt.Errorf("engine error (status %d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("engine returned HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if respPayload != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, respPayload); err != nil {
			return fmt.Errorf("failed to decode response payload: %w", err)
		}
	}

	return nil
}

func (b *remoteBackend) Save(ctx context.Context, model, name string, op *uint8, props map[string]float32) (string, error) {
	req := map[string]interface{}{
		"model": model,
		"name":  name,
		"props": props,
	}
	if op != nil {
		req["operation"] = *op
	}

	var resp struct {
		Success bool   `json:"success"`
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/save", req, &resp); err != nil {
		return "", err
	}
	return resp.Name, nil
}

func (b *remoteBackend) Get(ctx context.Context, model, name string, selectFields []string, computations map[string]storage.ComputeOp) (*EntityResult, error) {
	req := map[string]interface{}{
		"model":  model,
		"name":   name,
		"select": selectFields,
	}
	if len(computations) > 0 {
		compMap := make(map[string]string, len(computations))
		for k, v := range computations {
			compMap[k] = serializeComputeOp(v)
		}
		req["computations"] = compMap
	}

	var resp struct {
		Success   bool               `json:"success"`
		Name      string             `json:"name"`
		Operation uint8              `json:"operation"`
		Props     map[string]float32 `json:"props"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/get", req, &resp); err != nil {
		return nil, err
	}
	return &EntityResult{Name: resp.Name, props: resp.Props}, nil
}

func (b *remoteBackend) List(ctx context.Context, model string, opts storage.QueryOptions) (*PagedListResult, error) {
	req := map[string]interface{}{
		"model":                  model,
		"select":                 opts.Select,
		"limit":                  opts.Limit,
		"offset":                 opts.Offset,
		"include_all_operations": opts.IncludeAllOperations,
	}
	if opts.OperationFilter != nil {
		req["operation"] = *opts.OperationFilter
	}
	if len(opts.AllowedOperations) > 0 {
		req["allowed_operations"] = opts.AllowedOperations
	}
	if len(opts.Filters) > 0 {
		req["filters"] = serializeFilters(opts.Filters)
	}
	if len(opts.Computations) > 0 {
		compMap := make(map[string]string, len(opts.Computations))
		for k, v := range opts.Computations {
			compMap[k] = serializeComputeOp(v)
		}
		req["computations"] = compMap
	}

	var resp struct {
		Success bool `json:"success"`
		Items   []struct {
			Name      string             `json:"name"`
			Operation uint8              `json:"operation"`
			Props     map[string]float32 `json:"props"`
		} `json:"items"`
		HasMore bool `json:"has_more"`
		Count   int  `json:"count"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/list", req, &resp); err != nil {
		return nil, err
	}

	items := make([]EntityResult, len(resp.Items))
	for i, r := range resp.Items {
		items[i] = EntityResult{
			Name:  r.Name,
			props: r.Props,
		}
	}
	return &PagedListResult{
		Items:   items,
		HasMore: resp.HasMore,
	}, nil
}

func (b *remoteBackend) Update(ctx context.Context, model string, opts storage.UpdateOptions) (int, error) {
	req := map[string]interface{}{
		"model": model,
		"in":    opts.TargetEntities,
		"limit": opts.Limit,
	}
	if len(opts.Filters) > 0 {
		req["filters"] = serializeFilters(opts.Filters)
	}
	if len(opts.Mutations) > 0 {
		mutMap := make(map[string]string, len(opts.Mutations))
		for k, v := range opts.Mutations {
			mutMap[k] = serializeComputeOp(v)
		}
		req["mutations"] = mutMap
	}

	var resp struct {
		Success      bool `json:"success"`
		UpdatedCount int  `json:"updated_count"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/update", req, &resp); err != nil {
		return 0, err
	}
	return resp.UpdatedCount, nil
}

func (b *remoteBackend) Graph(ctx context.Context, model string, inputs []string, relation string, limitK int, orderMode storage.OrderByMode, beamWidth float32) (*GraphResponse, error) {
	req := map[string]interface{}{
		"model":      model,
		"inputs":     inputs,
		"relation":   relation,
		"limit_k":    limitK,
		"order_by":   orderMode,
		"beam_width": beamWidth,
	}
	var resp struct {
		Success bool                       `json:"success"`
		Anchor  spatial.Point2D            `json:"anchor"`
		Results []storage.GraphQueryResult `json:"results"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/graph", req, &resp); err != nil {
		return nil, err
	}
	return &GraphResponse{Anchor: resp.Anchor, Results: resp.Results}, nil
}

func (b *remoteBackend) GraphChain(ctx context.Context, model string, inputs []string, relations []string, limitK int, beamWidth float32) (*ChainResponse, error) {
	req := map[string]interface{}{
		"model":      model,
		"inputs":     inputs,
		"relations":  relations,
		"beam_width": beamWidth,
		"limit_k":    limitK,
	}
	var resp struct {
		Success  bool                       `json:"success"`
		Sequence []string                   `json:"sequence"`
		Steps    []storage.GraphQueryResult `json:"steps"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/graph", req, &resp); err != nil {
		return nil, err
	}
	return &ChainResponse{Sequence: resp.Sequence, Steps: resp.Steps}, nil
}

func (b *remoteBackend) Edge(ctx context.Context, model, action, source, relation, target string, weight float32) error {
	req := map[string]interface{}{
		"model":    model,
		"action":   action,
		"source":   source,
		"relation": relation,
		"target":   target,
		"weight":   weight,
	}
	var resp struct {
		Success   bool `json:"success"`
		EdgeCount int  `json:"edge_count"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/edge", req, &resp)
}

func (b *remoteBackend) Causal(ctx context.Context, model string, seeds []string, relation string, relations []string, limitK int) ([]storage.CausalResult, error) {
	req := map[string]interface{}{
		"model":     model,
		"seeds":     seeds,
		"relation":  relation,
		"relations": relations,
		"limit_k":   limitK,
	}
	var resp struct {
		Success bool                   `json:"success"`
		Results []storage.CausalResult `json:"results"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/causal", req, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

func (b *remoteBackend) Distance(ctx context.Context, model, wordA, wordB string) (float32, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "distance",
		"words": []string{wordA, wordB},
	}
	var resp struct {
		Success  bool    `json:"success"`
		Distance float32 `json:"distance"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return 0, err
	}
	return resp.Distance, nil
}

func (b *remoteBackend) Centroid(ctx context.Context, model string, words []string) (CentroidResult, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "centroid",
		"words": words,
	}
	var resp struct {
		Success bool            `json:"success"`
		Coord   spatial.Point2D `json:"coord"`
		Roles   [4]float32      `json:"roles"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return CentroidResult{}, err
	}
	return CentroidResult{Coord: resp.Coord, Roles: resp.Roles}, nil
}

func (b *remoteBackend) CosineSimilarity(ctx context.Context, model, wordA, wordB string) (float32, error) {
	req := map[string]interface{}{
		"model": model,
		"op":    "cosine_similarity",
		"words": []string{wordA, wordB},
	}
	var resp struct {
		Success    bool    `json:"success"`
		Similarity float32 `json:"similarity"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return 0, err
	}
	return resp.Similarity, nil
}

func (b *remoteBackend) Nearest(ctx context.Context, model, word string, limitK int) ([]storage.GraphQueryResult, error) {
	req := map[string]interface{}{
		"model":   model,
		"op":      "nearest",
		"words":   []string{word},
		"limit_k": limitK,
	}
	var resp struct {
		Success   bool                       `json:"success"`
		Neighbors []storage.GraphQueryResult `json:"neighbors"`
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/math", req, &resp); err != nil {
		return nil, err
	}
	return resp.Neighbors, nil
}

func (b *remoteBackend) AppendNode(ctx context.Context, model, word string, coord spatial.Point2D, roles [4]float32) error {
	req := map[string]interface{}{
		"model": model,
		"name":  word,
		"coord": coord,
		"roles": roles,
	}
	var resp struct {
		Success bool `json:"success"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/save", req, &resp)
}

func (b *remoteBackend) SetRelation(ctx context.Context, model, name string, direction spatial.Point2D) error {
	req := map[string]interface{}{
		"model":     model,
		"action":    "set_relation",
		"relation":  name,
		"direction": direction,
	}
	var resp struct {
		Success bool `json:"success"`
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/edge", req, &resp)
}

func (b *remoteBackend) Sync(ctx context.Context, model string) error {
	endpoint := "/v1/sync"
	if model != "" {
		endpoint += "?model=" + url.QueryEscape(model)
	}
	var resp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	return b.doJSON(ctx, http.MethodPost, endpoint, nil, &resp)
}

func (b *remoteBackend) Close() error {
	if b.httpClient != nil {
		b.httpClient.CloseIdleConnections()
	}
	return nil
}

func (b *remoteBackend) DB() *storage.AnchorDB {
	return nil
}

func (b *remoteBackend) IsRemote() bool {
	return true
}

// -----------------------------------------------------------------------------
// Serialization Helpers
// -----------------------------------------------------------------------------

func serializeComputeOp(op storage.ComputeOp) string {
	switch o := op.(type) {
	case storage.OpDivide:
		return fmt.Sprintf("%s / %s", o.FieldA, o.FieldB)
	case storage.OpMultiply:
		return fmt.Sprintf("%s * %s", o.FieldA, o.FieldB)
	case storage.OpAdd:
		return fmt.Sprintf("%s + %s", o.FieldA, o.FieldB)
	case storage.OpSubtract:
		return fmt.Sprintf("%s - %s", o.FieldA, o.FieldB)
	case storage.OpAssign:
		return o.Value
	case storage.OpAddDays:
		return fmt.Sprintf("%s + %g", o.Field, o.Days)
	case storage.OpSubtractDays:
		return fmt.Sprintf("%s - %g", o.Field, o.Days)
	case storage.OpDaysBetween:
		return fmt.Sprintf("%s - %s", o.FieldA, o.FieldB)
	default:
		return fmt.Sprintf("%v", op)
	}
}

func serializeFilters(filters []storage.FilterCondition) []map[string]string {
	out := make([]map[string]string, len(filters))
	for i, f := range filters {
		m := map[string]string{
			"field":    f.Field,
			"operator": f.Operator,
			"value":    fmt.Sprintf("%g", f.Value),
		}
		if f.Operator == "BETWEEN" {
			m["value_high"] = fmt.Sprintf("%g", f.ValueHigh)
		}
		out[i] = m
	}
	return out
}
