package cas

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/kv"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type Handler struct {
	registry *xetstore.Registry
	xorbs    *XorbStore
}

type HandlerOption func(*Handler)

func WithXorbStore(store *XorbStore) HandlerOption {
	return func(h *Handler) {
		h.xorbs = store
	}
}

type registerShardRequest struct {
	FileHash string   `json:"file_hash"`
	Shard    string   `json:"shard"`
	ChunkIDs []string `json:"chunk_ids"`
}

type registerShardResponse struct {
	FileHash    string `json:"file_hash"`
	WasInserted bool   `json:"was_inserted"`
}

type putXorbResponse struct {
	WasInserted bool `json:"was_inserted"`
}

func NewHandler(registry *xetstore.Registry, opts ...HandlerOption) http.Handler {
	h := &Handler{registry: registry}
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
	result, err := h.xorbs.Put(r.Context(), chi.URLParam(r, "prefix"), chi.URLParam(r, "hash"), r.ContentLength, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(putXorbResponse{WasInserted: result.WasInserted})
}

func (h *Handler) postShard(w http.ResponseWriter, r *http.Request) {
	var req registerShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.FileHash == "" || req.Shard == "" {
		http.Error(w, "file_hash and shard are required", http.StatusBadRequest)
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
