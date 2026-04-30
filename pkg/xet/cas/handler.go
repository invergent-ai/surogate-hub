package cas

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/auth/model"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/xet/reconstruct"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

type Handler struct {
	registry                           *xetstore.Registry
	xorbs                              *XorbStore
	verifyTokens                       chan struct{}
	verifyXorb                         func(expectedHash string, data []byte) error
	reconstructionRangeResolverFactory reconstructionRangeResolverFactory
	reconstructionCapabilityChecker    ReconstructionCapabilityChecker
	proxyGrantKey                      []byte
	tokenSigningKey                    []byte
	tokenTTL                           time.Duration
	tokenAuthRequired                  bool
}

type HandlerOption func(*Handler)

type reconstructionRangeResolverFactory func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error)

type ReconstructionLogicalContext struct {
	Repo string
	Ref  string
	Path string
}

type ReconstructionCapabilityChecker func(ctx context.Context, fileHash string, logical ReconstructionLogicalContext) error

var ErrReconstructionCapabilityNotFound = errors.New("xet reconstruction capability not found")

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

func WithProxyGrantKey(key []byte) HandlerOption {
	return func(h *Handler) {
		h.proxyGrantKey = append([]byte(nil), key...)
	}
}

func WithTokenSigningKey(key []byte) HandlerOption {
	return func(h *Handler) {
		h.tokenSigningKey = append([]byte(nil), key...)
	}
}

func WithTokenTTL(ttl time.Duration) HandlerOption {
	return func(h *Handler) {
		h.tokenTTL = ttl
	}
}

func WithTokenAuthRequired() HandlerOption {
	return func(h *Handler) {
		h.tokenAuthRequired = true
	}
}

func WithReconstructionCapabilityChecker(checker ReconstructionCapabilityChecker) HandlerOption {
	return func(h *Handler) {
		h.reconstructionCapabilityChecker = checker
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

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresAt   int64  `json:"exp"`
}

type proxyGrant struct {
	FileHash string                  `json:"file_hash"`
	XorbHash string                  `json:"xorb_hash"`
	Ranges   []reconstruct.HTTPRange `json:"ranges"`
	Expires  int64                   `json:"exp"`
}

func NewHandler(registry *xetstore.Registry, opts ...HandlerOption) http.Handler {
	h := &Handler{
		registry:     registry,
		verifyTokens: make(chan struct{}, normalizeVerifyMaxConcurrent(0)),
		verifyXorb:   validateSerializedXorb,
		tokenTTL:     time.Hour,
	}
	for _, opt := range opts {
		opt(h)
	}
	r := chi.NewRouter()
	r.With(h.requireXETScope("read")).Get("/v1/chunks/{prefix}/{hash}", h.getChunk)
	r.Get("/v1/token/refresh", h.getTokenRefresh)
	r.With(h.requireXETScope("read")).Get("/v1/reconstructions/{file_hash}", h.getReconstructionV1)
	r.Get("/v1/xorbs/{prefix}/{hash}", h.getXorbProxy)
	r.With(h.requireXETScope("read")).Get("/v2/reconstructions/{file_hash}", h.getReconstruction)
	r.Post("/v1/token", h.postToken)
	r.With(h.requireXETScope("write")).Post("/shards", h.postShard)
	r.With(h.requireXETScope("write")).Post("/v1/shards", h.postShard)
	r.With(h.requireXETScope("write")).Post("/v1/xorbs/{prefix}/{hash}", h.postXorb)
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
			canonical, err := canonicalSerializedXorb(hash, data)
			if err != nil {
				return err
			}
			if err := h.verifyXorb(hash, canonical); err != nil {
				return err
			}
			data = canonical
			return nil
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

func (h *Handler) postToken(w http.ResponseWriter, r *http.Request) {
	user, err := auth.GetUser(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	response, err := h.issueXETToken(user.Username, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func (h *Handler) getTokenRefresh(w http.ResponseWriter, r *http.Request) {
	tokenString, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	subject, err := h.verifyXETToken(tokenString)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	response, err := h.issueXETToken(subject, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func (h *Handler) requireXETScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if !h.tokenAuthRequired {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString, ok := bearerToken(r.Header.Get("Authorization"))
			if !ok {
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			claims, err := h.verifyXETTokenClaims(tokenString)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			if !hasXETScope(claims, scope) {
				http.Error(w, "insufficient token scope", http.StatusForbidden)
				return
			}
			subject, _ := claims["sub"].(string)
			ctx := auth.WithUser(r.Context(), &model.User{Username: subject})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func (h *Handler) issueXETToken(subject string, now time.Time) (tokenResponse, error) {
	if len(h.tokenSigningKey) == 0 {
		return tokenResponse{}, errors.New("token signing key is not configured")
	}
	expires := now.Add(h.tokenTTL)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   subject,
		"aud":   "xet",
		"scope": "read write",
		"iat":   now.Unix(),
		"exp":   expires.Unix(),
	})
	signed, err := token.SignedString(h.tokenSigningKey)
	if err != nil {
		return tokenResponse{}, err
	}
	return tokenResponse{
		AccessToken: signed,
		ExpiresAt:   expires.Unix(),
	}, nil
}

func (h *Handler) verifyXETToken(tokenString string) (string, error) {
	claims, err := h.verifyXETTokenClaims(tokenString)
	if err != nil {
		return "", err
	}
	subject, _ := claims["sub"].(string)
	if subject == "" {
		return "", errors.New("invalid token subject")
	}
	return subject, nil
}

func (h *Handler) verifyXETTokenClaims(tokenString string) (jwt.MapClaims, error) {
	if len(h.tokenSigningKey) == 0 {
		return nil, errors.New("token signing key is not configured")
	}
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method %s", token.Header["alg"])
		}
		return h.tokenSigningKey, nil
	})
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, errors.New("invalid token")
	}
	if audience, _ := claims["aud"].(string); audience != "xet" {
		return nil, errors.New("invalid token audience")
	}
	subject, _ := claims["sub"].(string)
	if subject == "" {
		return nil, errors.New("invalid token subject")
	}
	return claims, nil
}

func hasXETScope(claims jwt.MapClaims, required string) bool {
	scope, _ := claims["scope"].(string)
	for _, tokenScope := range strings.Fields(scope) {
		if tokenScope == required {
			return true
		}
	}
	return false
}

func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func (h *Handler) getXorbProxy(w http.ResponseWriter, r *http.Request) {
	if h.xorbs == nil {
		http.NotFound(w, r)
		return
	}
	hash := chi.URLParam(r, "hash")
	grant, err := h.verifyProxyGrant(r.URL.Query().Get("grant"), hash, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	byteRange, err := xorbProxyRange(r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if !rangeGranted(byteRange, grant.Ranges) {
		http.Error(w, "range is not granted", http.StatusForbidden)
		return
	}
	reader, err := h.xorbs.adapter.GetRange(
		r.Context(),
		xetstore.XorbObjectPointer(h.xorbs.storageNamespace, chi.URLParam(r, "prefix"), hash),
		int64(byteRange.Start),
		int64(byteRange.End),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = reader.Close() }()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func (h *Handler) getReconstruction(w http.ResponseWriter, r *http.Request) {
	manifest, ok := h.buildReconstructionManifest(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(manifest)
}

func (h *Handler) getReconstructionV1(w http.ResponseWriter, r *http.Request) {
	manifest, ok := h.buildReconstructionManifest(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(manifest.V1())
}

func (h *Handler) buildReconstructionManifest(w http.ResponseWriter, r *http.Request) (reconstruct.Manifest, bool) {
	fileHash := chi.URLParam(r, "file_hash")
	if h.reconstructionCapabilityChecker != nil {
		logical := ReconstructionLogicalContext{
			Repo: r.URL.Query().Get("repo"),
			Ref:  r.URL.Query().Get("ref"),
			Path: r.URL.Query().Get("path"),
		}
		if err := h.reconstructionCapabilityChecker(r.Context(), fileHash, logical); err != nil {
			if errors.Is(err, ErrReconstructionCapabilityNotFound) {
				http.NotFound(w, r)
				return reconstruct.Manifest{}, false
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return reconstruct.Manifest{}, false
		}
	}
	shard, err := h.registry.GetShardByFileHash(r.Context(), fileHash)
	if errors.Is(err, kv.ErrNotFound) {
		http.NotFound(w, r)
		return reconstruct.Manifest{}, false
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	info, err := xetstore.ParseShardInfo(shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	file, ok := shardFileByHash(info, fileHash)
	if !ok {
		http.NotFound(w, r)
		return reconstruct.Manifest{}, false
	}
	byteRange, err := reconstructionByteRange(r.Header.Get("Range"), file.SizeBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return reconstruct.Manifest{}, false
	}
	terms, err := reconstruct.MapRange(info, fileHash, byteRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	resolverFactory := h.reconstructionRangeResolverFactory
	var resolver reconstruct.RangeResolver
	if resolverFactory == nil {
		resolver, err = h.presignedReconstructionRangeResolverFactory(r, fileHash, terms)
	} else {
		resolver, err = resolverFactory(r.Context(), fileHash, terms)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	manifest, err := reconstruct.BuildManifest(terms, resolver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	return manifest, true
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
	canonicalShard, info, err := xetstore.CanonicalShard(shard)
	if err != nil {
		if h.acceptExistingAbbreviatedShard(r.Context(), shard) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(uploadShardResponse{Result: 0})
			return
		}
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
			Shard:    canonicalShard,
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

func (h *Handler) acceptExistingAbbreviatedShard(ctx context.Context, shard []byte) bool {
	files, err := xetstore.ParseShardFiles(shard)
	if err != nil || len(files) == 0 {
		return false
	}
	for _, file := range files {
		exists, err := h.registry.HasShard(ctx, file.FileHash)
		if err != nil || !exists {
			return false
		}
	}
	return true
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

func (h *Handler) presignedReconstructionRangeResolverFactory(r *http.Request, fileHash string, _ []reconstruct.Term) (reconstruct.RangeResolver, error) {
	if h.xorbs == nil {
		return nil, fmt.Errorf("xorb store is not configured")
	}
	ctx := r.Context()
	proxyBaseURL := reconstructionProxyBaseURL(r)
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
		if errors.Is(err, block.ErrOperationNotSupported) {
			url, err = h.proxyXorbURL(proxyBaseURL, fileHash, "default", xorbHash, []reconstruct.HTTPRange{byteRange})
			if err != nil {
				return reconstruct.ResolvedRange{}, err
			}
		} else if err != nil {
			return reconstruct.ResolvedRange{}, err
		}
		return reconstruct.ResolvedRange{URL: url, Bytes: byteRange}, nil
	}, nil
}

func (h *Handler) proxyXorbURL(baseURL, fileHash, prefix, xorbHash string, ranges []reconstruct.HTTPRange) (string, error) {
	grant, err := h.signProxyGrant(proxyGrant{
		FileHash: fileHash,
		XorbHash: xorbHash,
		Ranges:   ranges,
		Expires:  time.Now().Add(15 * time.Minute).Unix(),
	})
	if err != nil {
		return "", err
	}
	return baseURL + "/v1/xorbs/" + url.PathEscape(prefix) + "/" + url.PathEscape(xorbHash) + "?grant=" + url.QueryEscape(grant), nil
}

func reconstructionProxyBaseURL(r *http.Request) string {
	scheme := firstHeaderValue(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := firstHeaderValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	prefix := strings.TrimRight(r.Header.Get("X-Forwarded-Prefix"), "/")
	return scheme + "://" + host + prefix + reconstructionMountPrefix(r.URL.Path)
}

func reconstructionMountPrefix(path string) string {
	for _, marker := range []string{"/v1/reconstructions/", "/v2/reconstructions/"} {
		if idx := strings.Index(path, marker); idx >= 0 {
			return path[:idx]
		}
	}
	return ""
}

func firstHeaderValue(value string) string {
	if idx := strings.Index(value, ","); idx >= 0 {
		value = value[:idx]
	}
	return strings.TrimSpace(value)
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

func reconstructionByteRange(header string, fileSize uint64) (reconstruct.ByteRange, error) {
	if header == "" {
		return reconstruct.ByteRange{Start: 0, End: fileSize}, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return reconstruct.ByteRange{}, fmt.Errorf("invalid range header")
	}
	startSpec, endSpec, ok := strings.Cut(spec, "-")
	if !ok || startSpec == "" || endSpec == "" {
		return reconstruct.ByteRange{}, fmt.Errorf("invalid range header")
	}
	start, err := strconv.ParseUint(startSpec, 10, 64)
	if err != nil {
		return reconstruct.ByteRange{}, fmt.Errorf("invalid range start")
	}
	endInclusive, err := strconv.ParseUint(endSpec, 10, 64)
	if err != nil {
		return reconstruct.ByteRange{}, fmt.Errorf("invalid range end")
	}
	if start > endInclusive || start >= fileSize {
		return reconstruct.ByteRange{}, fmt.Errorf("range exceeds file size")
	}
	end := endInclusive + 1
	if end < endInclusive || end > fileSize {
		end = fileSize
	}
	return reconstruct.ByteRange{Start: start, End: end}, nil
}

func xorbProxyRange(header string) (reconstruct.HTTPRange, error) {
	if header == "" {
		return reconstruct.HTTPRange{}, fmt.Errorf("range header is required")
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid range header")
	}
	startSpec, endSpec, ok := strings.Cut(spec, "-")
	if !ok || startSpec == "" || endSpec == "" {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid range header")
	}
	start, err := strconv.ParseUint(startSpec, 10, 64)
	if err != nil {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid range start")
	}
	end, err := strconv.ParseUint(endSpec, 10, 64)
	if err != nil {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid range end")
	}
	if start > end {
		return reconstruct.HTTPRange{}, fmt.Errorf("invalid range")
	}
	return reconstruct.HTTPRange{Start: start, End: end}, nil
}

func (h *Handler) signProxyGrant(grant proxyGrant) (string, error) {
	if len(h.proxyGrantKey) == 0 {
		return "", fmt.Errorf("proxy grant key is not configured")
	}
	payload, err := json.Marshal(grant)
	if err != nil {
		return "", err
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, h.proxyGrantKey)
	_, _ = mac.Write([]byte(payloadPart))
	signaturePart := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payloadPart + "." + signaturePart, nil
}

func (h *Handler) verifyProxyGrant(encoded, xorbHash string, now time.Time) (proxyGrant, error) {
	if len(h.proxyGrantKey) == 0 {
		return proxyGrant{}, fmt.Errorf("proxy grant key is not configured")
	}
	payloadPart, signaturePart, ok := strings.Cut(encoded, ".")
	if !ok || payloadPart == "" || signaturePart == "" {
		return proxyGrant{}, fmt.Errorf("invalid grant")
	}
	mac := hmac.New(sha256.New, h.proxyGrantKey)
	_, _ = mac.Write([]byte(payloadPart))
	expectedSignature := mac.Sum(nil)
	actualSignature, err := base64.RawURLEncoding.DecodeString(signaturePart)
	if err != nil {
		return proxyGrant{}, fmt.Errorf("invalid grant signature")
	}
	if !hmac.Equal(actualSignature, expectedSignature) {
		return proxyGrant{}, fmt.Errorf("invalid grant signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadPart)
	if err != nil {
		return proxyGrant{}, fmt.Errorf("invalid grant payload")
	}
	var grant proxyGrant
	if err := json.Unmarshal(payload, &grant); err != nil {
		return proxyGrant{}, fmt.Errorf("invalid grant payload")
	}
	if grant.XorbHash != xorbHash {
		return proxyGrant{}, fmt.Errorf("grant xorb mismatch")
	}
	if now.Unix() > grant.Expires {
		return proxyGrant{}, fmt.Errorf("grant expired")
	}
	return grant, nil
}

func rangeGranted(requested reconstruct.HTTPRange, grants []reconstruct.HTTPRange) bool {
	for _, grant := range grants {
		if requested.Start >= grant.Start && requested.End <= grant.End {
			return true
		}
	}
	return false
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
