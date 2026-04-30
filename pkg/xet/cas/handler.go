package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

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

	wasInserted := false
	for _, file := range info.Files {
		result, err := h.registry.RegisterShard(r.Context(), xetstore.RegisterShardParams{
			FileHash: file.FileHash,
			Shard:    shard,
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
