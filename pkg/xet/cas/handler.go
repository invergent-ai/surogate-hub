package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/kv"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type Handler struct {
	registry     *xetstore.Registry
	xorbs        *XorbStore
	verifyTokens chan struct{}
	verifyXorb   func(expectedHash string, data []byte) error
}

type HandlerOption func(*Handler)

func WithXorbStore(store *XorbStore) HandlerOption {
	return func(h *Handler) {
		h.xorbs = store
	}
}

func WithVerifyMaxConcurrent(maxConcurrent int) HandlerOption {
	return func(h *Handler) {
		h.verifyTokens = make(chan struct{}, normalizeVerifyMaxConcurrent(maxConcurrent))
	}
}

func withXorbVerifier(verify func(expectedHash string, data []byte) error) HandlerOption {
	return func(h *Handler) {
		h.verifyXorb = verify
	}
}

type registerShardRequest struct {
	FileHash string   `json:"file_hash"`
	Shard    string   `json:"shard"`
	ChunkIDs []string `json:"chunk_ids"`
	XorbIDs  []string `json:"xorb_ids"`
}

type registerShardResponse struct {
	FileHash    string `json:"file_hash"`
	WasInserted bool   `json:"was_inserted"`
}

type putXorbResponse struct {
	WasInserted bool `json:"was_inserted"`
}

type uploadShardResponse struct {
	Result int `json:"result"`
}

func NewHandler(registry *xetstore.Registry, opts ...HandlerOption) http.Handler {
	h := &Handler{
		registry:     registry,
		verifyTokens: make(chan struct{}, normalizeVerifyMaxConcurrent(0)),
		verifyXorb:   validateSerializedXorb,
	}
	for _, opt := range opts {
		opt(h)
	}
	r := chi.NewRouter()
	r.Get("/v1/chunks/{prefix}/{hash}", h.getChunk)
	r.Post("/v1/shards", h.postShard)
	r.Post("/v1/xorbs/{prefix}/{hash}", h.postXorb)
	return r
}

func (h *Handler) postXorb(w http.ResponseWriter, r *http.Request) {
	if h.xorbs == nil {
		http.NotFound(w, r)
		return
	}
	hash := chi.URLParam(r, "hash")
	body := r.Body
	size := r.ContentLength
	if isXETHash(hash) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := h.withXorbVerificationSlot(r, func() error {
			return h.verifyXorb(hash, data)
		}); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body = io.NopCloser(bytes.NewReader(data))
		size = int64(len(data))
	}
	result, err := h.xorbs.Put(r.Context(), chi.URLParam(r, "prefix"), hash, size, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(putXorbResponse{WasInserted: result.WasInserted})
}

func (h *Handler) postShard(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		h.postBinaryShard(w, r)
		return
	}

	var req registerShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.FileHash == "" || req.Shard == "" {
		http.Error(w, "file_hash and shard are required", http.StatusBadRequest)
		return
	}
	if req.FileHash != computedShimFileHash(req.Shard) {
		http.Error(w, "file_hash does not match shard", http.StatusBadRequest)
		return
	}
	if err := h.validateXorbs(r, req.XorbIDs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.registry.RegisterShard(r.Context(), xetstore.RegisterShardParams{
		FileHash: req.FileHash,
		Shard:    []byte(req.Shard),
		ChunkIDs: req.ChunkIDs,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(registerShardResponse{
		FileHash:    req.FileHash,
		WasInserted: result.WasInserted,
	})
}

func (h *Handler) postBinaryShard(w http.ResponseWriter, r *http.Request) {
	shard, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	info, err := xetstore.ParseShardInfo(shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(info.Files) == 0 {
		http.Error(w, "shard contains no file entries", http.StatusBadRequest)
		return
	}
	if err := h.validateXorbs(r, info.XorbHashes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	summary, err := json.Marshal(info.Summary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	wasInserted := false
	for _, file := range info.Files {
		result, err := h.registry.RegisterShard(r.Context(), xetstore.RegisterShardParams{
			FileHash: file.FileHash,
			Shard:    shard,
			Summary:  summary,
			ChunkIDs: info.ChunkHashes,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		wasInserted = wasInserted || result.WasInserted
	}

	responseResult := 0
	if wasInserted {
		responseResult = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(uploadShardResponse{Result: responseResult})
}

func (h *Handler) validateXorbs(r *http.Request, xorbIDs []string) error {
	if len(xorbIDs) == 0 {
		return nil
	}
	if h.xorbs == nil {
		return fmt.Errorf("xorb store is not configured")
	}
	for _, xorbID := range xorbIDs {
		exists, err := h.xorbs.Exists(r.Context(), "default", xorbID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("xorb %s not found", xorbID)
		}
	}
	return nil
}

func (h *Handler) withXorbVerificationSlot(r *http.Request, verify func() error) error {
	select {
	case h.verifyTokens <- struct{}{}:
		defer func() { <-h.verifyTokens }()
		return verify()
	case <-r.Context().Done():
		return r.Context().Err()
	}
}

func normalizeVerifyMaxConcurrent(maxConcurrent int) int {
	if maxConcurrent > 0 {
		return maxConcurrent
	}
	return runtime.NumCPU()
}

func computedShimFileHash(shard string) string {
	sum := sha256.Sum256([]byte(shard))
	return hex.EncodeToString(sum[:])
}

func (h *Handler) getChunk(w http.ResponseWriter, r *http.Request) {
	shard, err := h.registry.GetDedupShardByChunk(r.Context(), chi.URLParam(r, "hash"))
	if errors.Is(err, kv.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(shard)
}
