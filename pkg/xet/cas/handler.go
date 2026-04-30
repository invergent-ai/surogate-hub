package cas

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/kv"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type Handler struct {
	registry *xetstore.Registry
}

func NewHandler(registry *xetstore.Registry) http.Handler {
	h := &Handler{registry: registry}
	r := chi.NewRouter()
	r.Get("/xet/v1/chunks/{prefix}/{hash}", h.getChunk)
	return r
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
