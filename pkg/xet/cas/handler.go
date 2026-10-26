package cas

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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

const (
	// xorbPrefix is the only CAS prefix hf_xet uses; every read path assumes it.
	xorbPrefix = "default"
	// hf_xet cuts xorbs at 64 MiB of chunk data; the rest covers chunk headers and the footer.
	maxXorbBytes = 80 << 20
	// hf_xet targets 64 MiB shards.
	maxShardBytes = 128 << 20
)

type Handler struct {
	registry                           *xetstore.Registry
	xorbs                              *XorbStore
	verifyTokens                       chan struct{}
	verifyXorb                         func(expectedHash string, data []byte) error
	reconstructionRangeResolverFactory reconstructionRangeResolverFactory
	reconstructionCapabilityChecker    ReconstructionCapabilityChecker
	tokenAuthorizer                    TokenAuthorizer
	proxyGrantKey                      []byte
	tokenSigningKey                    []byte
	tokenTTL                           time.Duration
	tokenAuthRequired                  bool
}

type HandlerOption func(*Handler)

type reconstructionRangeResolverFactory func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error)

// TokenGrant is what an xet token allows: reading or writing one object.
type TokenGrant struct {
	Mode string
	Repo string
	Ref  string
	Path string
}

// TokenAuthorizer decides whether a user may get a token for a grant. It runs
// when the token is minted.
type TokenAuthorizer func(ctx context.Context, username string, grant TokenGrant) error

// ReconstructionCapabilityChecker decides whether the grant of the token on a
// reconstruction request covers the file.
type ReconstructionCapabilityChecker func(ctx context.Context, fileHash string, grant TokenGrant) error

var (
	ErrReconstructionCapabilityNotFound = errors.New("xet reconstruction capability not found")
	ErrTokenNotAllowed                  = errors.New("xet token not allowed")
)

type tokenGrantKey struct{}

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

func WithTokenAuthorizer(authorizer TokenAuthorizer) HandlerOption {
	return func(h *Handler) {
		h.tokenAuthorizer = authorizer
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
	if chi.URLParam(r, "prefix") != xorbPrefix || !isXETHash(hash) {
		http.Error(w, "invalid xorb address", http.StatusBadRequest)
		return
	}
	data, err := readLimitedBody(w, r, maxXorbBytes)
	if err != nil {
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
	_, footerStart, err := parseXorbInfo(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := h.xorbs.Put(r.Context(), xorbPrefix, hash, int64(len(data)), bytes.NewReader(data))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.registry.PutXorbInfo(r.Context(), hash, data[footerStart:]); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(putXorbResponse{WasInserted: result.WasInserted})
}

// postToken mints a token for one object. hf_xet refreshes through the client,
// which mints again, so every token carries a fresh permission check.
func (h *Handler) postToken(w http.ResponseWriter, r *http.Request) {
	user, err := auth.GetUser(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	query := r.URL.Query()
	grant := TokenGrant{
		Mode: query.Get("mode"),
		Repo: query.Get("repository"),
		Ref:  query.Get("ref"),
		Path: query.Get("path"),
	}
	if (grant.Mode != "read" && grant.Mode != "write") || grant.Repo == "" || grant.Ref == "" || grant.Path == "" {
		http.Error(w, "mode (read or write), repository, ref and path are required", http.StatusBadRequest)
		return
	}
	if h.tokenAuthorizer != nil {
		if err := h.tokenAuthorizer(r.Context(), user.Username, grant); errors.Is(err, ErrTokenNotAllowed) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	response, err := h.issueXETToken(user.Username, grant, time.Now())
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
			repo, _ := claims["repo"].(string)
			ref, _ := claims["ref"].(string)
			path, _ := claims["path"].(string)
			ctx = context.WithValue(ctx, tokenGrantKey{}, TokenGrant{Mode: scope, Repo: repo, Ref: ref, Path: path})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func (h *Handler) issueXETToken(subject string, grant TokenGrant, now time.Time) (tokenResponse, error) {
	if len(h.tokenSigningKey) == 0 {
		return tokenResponse{}, errors.New("token signing key is not configured")
	}
	expires := now.Add(h.tokenTTL)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   subject,
		"aud":   "xet",
		"scope": grant.Mode,
		"repo":  grant.Repo,
		"ref":   grant.Ref,
		"path":  grant.Path,
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
		h.xorbs.objectPointer(chi.URLParam(r, "prefix"), hash),
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
		grant, _ := r.Context().Value(tokenGrantKey{}).(TokenGrant)
		if err := h.reconstructionCapabilityChecker(r.Context(), fileHash, grant); err != nil {
			if errors.Is(err, ErrReconstructionCapabilityNotFound) {
				http.NotFound(w, r)
				return reconstruct.Manifest{}, false
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return reconstruct.Manifest{}, false
		}
	}
	file, err := h.registry.GetFile(r.Context(), fileHash)
	if errors.Is(err, kv.ErrNotFound) {
		http.NotFound(w, r)
		return reconstruct.Manifest{}, false
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	byteRange, err := reconstructionByteRange(r.Header.Get("Range"), file.SizeBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return reconstruct.Manifest{}, false
	}
	infos, err := xorbInfos(r.Context(), h.registry, file.Segments)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	terms, err := reconstruct.MapRange(file, xorbChunks(infos), byteRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return reconstruct.Manifest{}, false
	}
	var resolver reconstruct.RangeResolver
	if h.reconstructionRangeResolverFactory == nil {
		resolver, err = h.presignedReconstructionRangeResolver(r, fileHash, terms, infos)
	} else {
		resolver, err = h.reconstructionRangeResolverFactory(r.Context(), fileHash, terms)
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

// postShard registers the files a shard describes. Each file is checked against
// the stored metadata of the xorbs it references; the shard's own xorb section is
// ignored, because hf_xet leaves out xorbs uploaded in earlier sessions and a
// client could put anything there.
func (h *Handler) postShard(w http.ResponseWriter, r *http.Request) {
	shard, err := readLimitedBody(w, r, maxShardBytes)
	if err != nil {
		return
	}
	files, err := xetstore.ParseShardFiles(shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(files) == 0 {
		http.Error(w, "shard contains no file entries", http.StatusBadRequest)
		return
	}
	var segments []xetstore.ShardFileSegment
	for _, file := range files {
		segments = append(segments, file.Segments...)
	}
	infos, err := xorbInfos(r.Context(), h.registry, segments)
	if errors.Is(err, kv.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	chunks := xorbChunks(infos)
	for i := range files {
		if files[i], err = xetstore.VerifyFile(files[i], chunks); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	wasInserted := false
	for _, file := range files {
		inserted, err := h.registry.RegisterFile(r.Context(), file)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		wasInserted = wasInserted || inserted
	}

	responseResult := 0
	if wasInserted {
		responseResult = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(uploadShardResponse{Result: responseResult})
}

// readLimitedBody reads at most limit bytes and writes the error response itself.
func readLimitedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
	}
	return data, err
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

func (h *Handler) presignedReconstructionRangeResolver(r *http.Request, fileHash string, terms []reconstruct.Term, infos map[string]parsedXorbInfo) (reconstruct.RangeResolver, error) {
	if h.xorbs == nil {
		return nil, fmt.Errorf("xorb store is not configured")
	}
	ctx := r.Context()
	proxyBaseURL := reconstructionProxyBaseURL(r)
	grantedRanges, err := reconstructionGrantedRanges(terms, infos)
	if err != nil {
		return nil, err
	}
	urls := make(map[string]string)
	return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
		byteRange, err := xorbByteRange(infos[xorbHash], chunks)
		if err != nil {
			return reconstruct.ResolvedRange{}, err
		}
		url, ok := urls[xorbHash]
		if !ok {
			url, _, err = h.xorbs.adapter.GetPreSignedURL(ctx, h.xorbs.objectPointer(xorbPrefix, xorbHash), block.PreSignModeRead)
			if errors.Is(err, block.ErrOperationNotSupported) {
				url, err = h.proxyXorbURL(proxyBaseURL, fileHash, xorbPrefix, xorbHash, grantedRanges[xorbHash])
				if err != nil {
					return reconstruct.ResolvedRange{}, err
				}
			} else if err != nil {
				return reconstruct.ResolvedRange{}, err
			}
			urls[xorbHash] = url
		}
		return reconstruct.ResolvedRange{URL: url, Bytes: byteRange}, nil
	}, nil
}

func reconstructionGrantedRanges(terms []reconstruct.Term, infos map[string]parsedXorbInfo) (map[string][]reconstruct.HTTPRange, error) {
	groupedTerms, err := reconstruct.GroupTerms(terms)
	if err != nil {
		return nil, err
	}
	grants := make(map[string][]reconstruct.HTTPRange)
	for _, term := range groupedTerms {
		byteRange, err := xorbByteRange(infos[term.Hash], term.Range)
		if err != nil {
			return nil, err
		}
		grants[term.Hash] = appendGrantedRange(grants[term.Hash], byteRange)
	}
	return grants, nil
}

func appendGrantedRange(ranges []reconstruct.HTTPRange, next reconstruct.HTTPRange) []reconstruct.HTTPRange {
	if len(ranges) == 0 {
		return []reconstruct.HTTPRange{next}
	}
	ranges[0] = reconstruct.HTTPRange{
		Start: min(ranges[0].Start, next.Start),
		End:   max(ranges[0].End, next.End),
	}
	return ranges
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
