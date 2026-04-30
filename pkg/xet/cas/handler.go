package cas

import (
	"bytes"
	"context"
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
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/xet/reconstruct"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type Handler struct {
	registry                           *xetstore.Registry
	xorbs                              *XorbStore
	verifyTokens                       chan struct{}
	verifyXorb                         func(expectedHash string, data []byte) error
	reconstructionRangeResolverFactory reconstructionRangeResolverFactory
}

type HandlerOption func(*Handler)

type reconstructionRangeResolverFactory func(ctx context.Context, terms []reconstruct.Term) (reconstruct.RangeResolver, error)

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

func withReconstructionRangeResolverFactory(factory reconstructionRangeResolverFactory) HandlerOption {
	return func(h *Handler) {
		h.reconstructionRangeResolverFactory = factory
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
	r.Get("/v2/reconstructions/{file_hash}", h.getReconstruction)
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

func (h *Handler) getReconstruction(w http.ResponseWriter, r *http.Request) {
	fileHash := chi.URLParam(r, "file_hash")
	shard, err := h.registry.GetShardByFileHash(r.Context(), fileHash)
	if errors.Is(err, kv.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	info, err := xetstore.ParseShardInfo(shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	file, ok := shardFileByHash(info, fileHash)
	if !ok {
		http.NotFound(w, r)
		return
	}
	terms, err := reconstruct.MapRange(info, fileHash, reconstruct.ByteRange{Start: 0, End: file.SizeBytes})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resolverFactory := h.reconstructionRangeResolverFactory
	if resolverFactory == nil {
		resolverFactory = h.presignedReconstructionRangeResolverFactory
	}
	resolver, err := resolverFactory(r.Context(), terms)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	manifest, err := reconstruct.BuildManifest(terms, resolver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(manifest)
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

func (h *Handler) presignedReconstructionRangeResolverFactory(ctx context.Context, _ []reconstruct.Term) (reconstruct.RangeResolver, error) {
	if h.xorbs == nil {
		return nil, fmt.Errorf("xorb store is not configured")
	}
	parsed := make(map[string]parsedXorbInfo)
	return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
		info, ok := parsed[xorbHash]
		if !ok {
			reader, err := h.xorbs.Get(ctx, "default", xorbHash)
			if err != nil {
				return reconstruct.ResolvedRange{}, err
			}
			data, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return reconstruct.ResolvedRange{}, err
			}
			info, _, err = parseXorbInfo(data)
			if err != nil {
				return reconstruct.ResolvedRange{}, err
			}
			parsed[xorbHash] = info
		}
		byteRange, err := xorbByteRange(info, chunks)
		if err != nil {
			return reconstruct.ResolvedRange{}, err
		}
		url, _, err := h.xorbs.adapter.GetPreSignedURL(ctx, xetstore.XorbObjectPointer(h.xorbs.storageNamespace, "default", xorbHash), block.PreSignModeRead)
		if err != nil {
			return reconstruct.ResolvedRange{}, err
		}
		return reconstruct.ResolvedRange{URL: url, Bytes: byteRange}, nil
	}, nil
}

func xorbByteRange(info parsedXorbInfo, chunks reconstruct.IndexRange) (reconstruct.HTTPRange, error) {
	if chunks.End <= chunks.Start || int(chunks.End) > len(info.ChunkBoundaries) {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid xorb chunk range %d-%d", chunks.Start, chunks.End)
	}
	start := uint64(0)
	if chunks.Start > 0 {
		start = uint64(info.ChunkBoundaries[chunks.Start-1])
	}
	end := uint64(info.ChunkBoundaries[chunks.End-1])
	if end == 0 || end <= start {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid xorb byte range")
	}
	return reconstruct.HTTPRange{Start: start, End: end - 1}, nil
}

func shardFileByHash(info xetstore.ShardInfo, fileHash string) (xetstore.ShardFileInfo, bool) {
	for _, file := range info.Files {
		if file.FileHash == fileHash {
			return file, true
		}
	}
	return xetstore.ShardFileInfo{}, false
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
