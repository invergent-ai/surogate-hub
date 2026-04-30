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

func NewHandler(registry *xetstore.Registry) http.Handler {
	h := &Handler{registry: registry}
	r := chi.NewRouter()
	r.Get("/v1/chunks/{prefix}/{hash}", h.getChunk)
	r.Post("/v1/shards", h.postShard)
	return r
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
