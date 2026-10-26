package cas

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/auth/model"
	"github.com/invergent-ai/surogate-hub/pkg/block/mem"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/xet/reconstruct"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestPostTokenIssuesGrantScopedJWT(t *testing.T) {
	ctx := auth.WithUser(context.Background(), &model.User{Username: "user-a"})
	var authorized TokenGrant
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenTTL(time.Hour),
		WithTokenAuthorizer(func(ctx context.Context, username string, grant TokenGrant) error {
			require.Equal(t, "user-a", username)
			authorized = grant
			return nil
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/v1/token?mode=read&repository=owner%2Frepo&ref=main&path=models%2Fa.bin", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, TokenGrant{Mode: "read", Repo: "owner/repo", Ref: "main", Path: "models/a.bin"}, authorized)
	var body tokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Greater(t, body.ExpiresAt, time.Now().Unix())
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(body.AccessToken, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("test-token-key"), nil
	})
	require.NoError(t, err)
	require.True(t, token.Valid)
	require.Equal(t, "user-a", claims["sub"])
	require.Equal(t, "xet", claims["aud"])
	require.Equal(t, "read", claims["scope"])
	require.Equal(t, "owner/repo", claims["repo"])
	require.Equal(t, "main", claims["ref"])
	require.Equal(t, "models/a.bin", claims["path"])
}

func TestPostTokenRejectsIncompleteOrDeniedGrant(t *testing.T) {
	ctx := auth.WithUser(context.Background(), &model.User{Username: "user-a"})
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthorizer(func(ctx context.Context, username string, grant TokenGrant) error {
			return ErrTokenNotAllowed
		}),
	)

	for query, code := range map[string]int{
		"mode=read&repository=owner%2Frepo&ref=main":             http.StatusBadRequest,
		"mode=admin&repository=owner%2Frepo&ref=main&path=a.bin": http.StatusBadRequest,
		"mode=write&repository=owner%2Frepo&ref=main&path=a.bin": http.StatusForbidden,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/token?"+query, nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, code, rec.Code, query)
	}
}

func TestScopedAuthRejectsMissingToken(t *testing.T) {
	ctx := context.Background()
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
	)

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/file-a", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestScopedAuthRejectsInsufficientScope(t *testing.T) {
	ctx := context.Background()
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
	)
	token := testXETToken(t, "user-a", TokenGrant{Mode: "read"}, time.Now().Add(time.Hour))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/xorb-a", strings.NewReader("xorb-bytes"))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestPostXorbStoresBytesAndReportsIdempotency(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))

	for _, inserted := range []bool{true, false} {
		req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.JSONEq(t, fmt.Sprintf(`{"was_inserted":%t}`, inserted), rec.Body.String())
	}

	reader, err := xorbStore.Get(ctx, xorbHash)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, xorbBytes, body)
}

func TestPostXorbRejectsUnverifiableAddress(t *testing.T) {
	ctx := context.Background()
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))

	for _, path := range []string{"/v1/xorbs/default/xorb-a", "/v1/xorbs/other/" + xorbHash} {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(xorbBytes))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, path)
	}
}

func TestDecompressLZ4XorbChunkStopsPastDeclaredLength(t *testing.T) {
	var compressed bytes.Buffer
	writer := lz4.NewWriter(&compressed)
	_, err := writer.Write(make([]byte, 1<<20))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	decompressed, err := decompressLZ4XorbChunk(compressed.Bytes(), 16)

	require.NoError(t, err)
	require.Len(t, decompressed, 17)
}

func TestPostXorbRejectsMismatchedSerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/0000000000000000000000000000000000000000000000000000000000000000", bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.NotEqual(t, xorbHash, "0000000000000000000000000000000000000000000000000000000000000000")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "xorb hash does not match body")
}

func TestPostXorbAcceptsMatchingSerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"was_inserted":true}`, rec.Body.String())
	reader, err := xorbStore.Get(ctx, xorbHash)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, xorbBytes, body)
}

func TestPostXorbAcceptsNoFooterSerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))
	noFooter := xettest.StripFooter(t, xorbBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(noFooter))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	reader, err := xorbStore.Get(ctx, xorbHash)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, xorbBytes, body)
}

func TestPostXorbAcceptsLZ4SerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorbWithScheme(t, xettest.SchemeLZ4, bytes.Repeat([]byte("a"), 2048))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"was_inserted":true}`, rec.Body.String())
}

func TestPostXorbAcceptsBG4LZ4SerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := xettest.SerializedXorbWithScheme(t, xettest.SchemeBG4LZ4, bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 512))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"was_inserted":true}`, rec.Body.String())
}

func TestPostXorbVerificationHonorsMaxConcurrency(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	var current int32
	var maxSeen int32
	verify := func(expectedHash string, data []byte) error {
		n := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if n <= old || atomic.CompareAndSwapInt32(&maxSeen, old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return validateSerializedXorb(expectedHash, data)
	}
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithXorbStore(xorbStore),
		WithVerifyMaxConcurrent(1),
		withXorbVerifier(verify),
	)

	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("chunk-data"))
	var wg sync.WaitGroup
	codes := make(chan int, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			codes <- rec.Code
		}()
	}
	wg.Wait()
	close(codes)
	for code := range codes {
		require.Equal(t, http.StatusOK, code)
	}

	require.Equal(t, int32(1), maxSeen)
}

func testXETToken(t *testing.T, subject string, grant TokenGrant, expires time.Time) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   subject,
		"aud":   "xet",
		"scope": grant.Mode,
		"repo":  grant.Repo,
		"ref":   grant.Ref,
		"path":  grant.Path,
		"iat":   time.Now().Unix(),
		"exp":   expires.Unix(),
	})
	signed, err := token.SignedString([]byte("test-token-key"))
	require.NoError(t, err)
	return signed
}

func TestPostShardRegistersFileBuiltFromUploadedXorb(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	handler := NewHandler(registry, WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")))
	chunk := []byte("hello world!")
	xorbHash, xorbBytes := xettest.SerializedXorb(t, chunk)
	fileHash := xettest.FileHash(t, chunk)
	postTestXorb(t, handler, xorbHash, xorbBytes)

	// The claimed segment size is ignored in favour of the xorb's chunk sizes.
	shard := xettest.Shard(t, fileHash, xorbHash, 0, 1, 999)
	for _, post := range []struct {
		path   string
		result int
	}{{"/v1/shards", 1}, {"/shards", 0}} {
		req := httptest.NewRequest(http.MethodPost, post.path, bytes.NewReader(shard))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.JSONEq(t, fmt.Sprintf(`{"result":%d}`, post.result), rec.Body.String())
	}

	file, err := registry.GetFile(ctx, fileHash)
	require.NoError(t, err)
	require.Equal(t, uint64(len(chunk)), file.SizeBytes)
	require.Equal(t, uint64(len(chunk)), file.Segments[0].SizeBytes)
}

func TestPostShardRejectsUnknownXorb(t *testing.T) {
	ctx := context.Background()
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)))
	chunk := []byte("hello world!")
	xorbHash, _ := xettest.SerializedXorb(t, chunk)

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(xettest.Shard(t, xettest.FileHash(t, chunk), xorbHash, 0, 1, 12)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), xorbHash)
}

func TestPostShardRejectsFileHashNotMatchingXorbChunks(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	handler := NewHandler(registry, WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")))
	xorbHash, xorbBytes := xettest.SerializedXorb(t, []byte("hello world!"))
	postTestXorb(t, handler, xorbHash, xorbBytes)
	otherFileHash := xettest.FileHash(t, []byte("other bytes!"))

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(xettest.Shard(t, otherFileHash, xorbHash, 0, 1, 12)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "file hash mismatch")
	_, err := registry.GetFile(ctx, otherFileHash)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func postTestXorb(t *testing.T, handler http.Handler, xorbHash string, xorbBytes []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
}

func TestGetReconstructionReturnsV2Manifest(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	fileHash, xorbHash, _ := xettest.SeedFile(t, ctx, registry, mem.New(ctx), "mem://xet-cas", []byte("hello world!"))
	handler := NewHandler(registry, withReconstructionRangeResolverFactory(func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error) {
		return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
			return reconstruct.ResolvedRange{
				URL:   "https://cas.example/" + xorbHash,
				Bytes: reconstruct.HTTPRange{Start: 0, End: 63},
			}, nil
		}, nil
	}))

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"offset_into_first_range": 0,
		"terms": [{
			"hash": "`+xorbHash+`",
			"range": {"start": 0, "end": 1},
			"unpacked_length": 12
		}],
		"xorbs": {
			"`+xorbHash+`": [{
				"url": "https://cas.example/`+xorbHash+`",
				"ranges": [{
					"chunks": {"start": 0, "end": 1},
					"bytes": {"start": 0, "end": 63}
				}]
			}]
		}
	}`, rec.Body.String())
}

func TestGetReconstructionReturnsV1Manifest(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	fileHash, xorbHash, _ := xettest.SeedFile(t, ctx, registry, mem.New(ctx), "mem://xet-cas", []byte("hello world!"))
	handler := NewHandler(registry, withReconstructionRangeResolverFactory(func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error) {
		return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
			return reconstruct.ResolvedRange{
				URL:   "https://cas.example/" + xorbHash,
				Bytes: reconstruct.HTTPRange{Start: 0, End: 63},
			}, nil
		}, nil
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/reconstructions/"+fileHash, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"offset_into_first_range": 0,
		"terms": [{
			"hash": "`+xorbHash+`",
			"range": {"start": 0, "end": 1},
			"unpacked_length": 12
		}],
		"fetch_info": {
			"`+xorbHash+`": [{
				"url": "https://cas.example/`+xorbHash+`",
				"range": {"start": 0, "end": 1},
				"url_range": {"start": 0, "end": 63}
			}]
		}
	}`, rec.Body.String())
}

func TestGetReconstructionChecksTheTokenGrant(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	fileHash, _, _ := xettest.SeedFile(t, ctx, registry, mem.New(ctx), "mem://xet-cas", []byte("hello world!"))
	called := false
	handler := NewHandler(
		registry,
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
		WithReconstructionCapabilityChecker(func(ctx context.Context, fileHash string, grant TokenGrant) error {
			called = true
			require.Equal(t, TokenGrant{Mode: "read", Repo: "repo-a", Ref: "main", Path: "models/checkpoint.bin"}, grant)
			return nil
		}),
		withReconstructionRangeResolverFactory(func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error) {
			return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
				return reconstruct.ResolvedRange{
					URL:   "https://cas.example/" + xorbHash,
					Bytes: reconstruct.HTTPRange{Start: 0, End: 63},
				}, nil
			}, nil
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	req.Header.Set("Authorization", "Bearer "+testXETToken(t, "user-a", TokenGrant{Mode: "read", Repo: "repo-a", Ref: "main", Path: "models/checkpoint.bin"}, time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, called)
}

func TestGetReconstructionReturnsNotFoundWhenGrantDoesNotCoverFile(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	fileHash, _, _ := xettest.SeedFile(t, ctx, registry, mem.New(ctx), "mem://xet-cas", []byte("hello world!"))
	handler := NewHandler(
		registry,
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
		WithReconstructionCapabilityChecker(func(ctx context.Context, fileHash string, grant TokenGrant) error {
			return ErrReconstructionCapabilityNotFound
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	req.Header.Set("Authorization", "Bearer "+testXETToken(t, "user-a", TokenGrant{Mode: "read"}, time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetReconstructionHonorsRangeHeader(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	fileHash, xorbHash, _ := xettest.SeedFile(t, ctx, registry, mem.New(ctx), "mem://xet-cas", []byte("hello world!"))
	handler := NewHandler(registry, withReconstructionRangeResolverFactory(func(ctx context.Context, fileHash string, terms []reconstruct.Term) (reconstruct.RangeResolver, error) {
		return func(xorbHash string, chunks reconstruct.IndexRange) (reconstruct.ResolvedRange, error) {
			return reconstruct.ResolvedRange{
				URL:   "https://cas.example/" + xorbHash,
				Bytes: reconstruct.HTTPRange{Start: 0, End: 63},
			}, nil
		}, nil
	}))

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	req.Header.Set("Range", "bytes=3-8")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"offset_into_first_range": 3,
		"terms": [{
			"hash": "`+xorbHash+`",
			"range": {"start": 0, "end": 1},
			"unpacked_length": 12
		}],
		"xorbs": {
			"`+xorbHash+`": [{
				"url": "https://cas.example/`+xorbHash+`",
				"ranges": [{
					"chunks": {"start": 0, "end": 1},
					"bytes": {"start": 0, "end": 63}
				}]
			}]
		}
	}`, rec.Body.String())
}

func TestGetReconstructionFallsBackToGrantedProxyURL(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	adapter := mem.New(ctx)
	xorbStore := NewXorbStore(adapter, "mem://xet-cas")
	fileHash, xorbHash, xorbBytes := xettest.SeedFile(t, ctx, registry, adapter, "mem://xet-cas", []byte("hello world!"))
	handler := NewHandler(
		registry,
		WithXorbStore(xorbStore),
		WithProxyGrantKey([]byte("test-proxy-grant-key")),
	)

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var manifest reconstruct.Manifest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &manifest))
	require.Len(t, manifest.Xorbs[xorbHash], 1)
	fetch := manifest.Xorbs[xorbHash][0]
	require.Len(t, fetch.Ranges, 1)
	require.Contains(t, fetch.URL, "http://example.com/v1/xorbs/default/"+xorbHash+"?grant=")
	parsedURL, err := url.Parse(fetch.URL)
	require.NoError(t, err)
	require.True(t, parsedURL.IsAbs())
	grantToken := parsedURL.Query().Get("grant")
	grantPayload, err := base64.RawURLEncoding.DecodeString(strings.Split(grantToken, ".")[0])
	require.NoError(t, err)
	var grant proxyGrant
	require.NoError(t, json.Unmarshal(grantPayload, &grant))
	require.Equal(t, fileHash, grant.FileHash)

	proxyReq := httptest.NewRequest(http.MethodGet, fetch.URL, nil)
	byteRange := fetch.Ranges[0].Bytes
	proxyReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteRange.Start, byteRange.End))
	proxyRec := httptest.NewRecorder()
	handler.ServeHTTP(proxyRec, proxyReq)

	require.Equal(t, http.StatusPartialContent, proxyRec.Code)
	require.Equal(t, xorbBytes[byteRange.Start:byteRange.End+1], proxyRec.Body.Bytes())
}

func TestReconstructionGrantedRangesMatchGroupedManifestTerms(t *testing.T) {
	parsed := map[string]parsedXorbInfo{
		"xorb-a": {ChunkBoundaries: []uint32{10, 20, 30}},
		"xorb-b": {ChunkBoundaries: []uint32{40}},
	}
	terms := []reconstruct.Term{
		{XorbHash: "xorb-a", ChunkIndex: 0, ChunkSizeBytes: 10},
		{XorbHash: "xorb-a", ChunkIndex: 1, ChunkSizeBytes: 10},
		{XorbHash: "xorb-b", ChunkIndex: 0, ChunkSizeBytes: 40},
		{XorbHash: "xorb-a", ChunkIndex: 2, ChunkSizeBytes: 10},
	}

	grants, err := reconstructionGrantedRanges(terms, parsed)
	require.NoError(t, err)

	require.Equal(t, []reconstruct.HTTPRange{
		{Start: 0, End: 29},
	}, grants["xorb-a"])
}

func TestReconstructFileRangeReadsXorbBytes(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	adapter := mem.New(ctx)
	xorbStore := NewXorbStore(adapter, "mem://xet-cas")
	fileHash, _, _ := xettest.SeedFile(t, ctx, registry, adapter, "mem://xet-cas", []byte("hello world!"))

	reader, err := ReconstructFileRange(ctx, registry, xorbStore, fileHash, reconstruct.ByteRange{Start: 3, End: 9})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, []byte("lo wor"), got)
}

func TestReconstructFileRangeStreamsChunkRunsAcrossXorbs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	adapter := mem.New(ctx)
	xorbA, xorbABytes := xettest.SerializedXorbWithScheme(t, xettest.SchemeLZ4, []byte("hello "), []byte("world"), []byte("!!"))
	xorbB, xorbBBytes := xettest.SerializedXorb(t, []byte("tail"))
	xettest.PutXorb(t, ctx, registry, adapter, "mem://xet-cas", xorbA, xorbABytes)
	xettest.PutXorb(t, ctx, registry, adapter, "mem://xet-cas", xorbB, xorbBBytes)
	fileHash := xettest.FileHash(t, []byte("world"), []byte("!!"), []byte("tail"))
	_, err := registry.RegisterFile(ctx, xetstore.ShardFileInfo{
		FileHash:  fileHash,
		SizeBytes: 11,
		Segments: []xetstore.ShardFileSegment{
			{XorbHash: xorbA, SizeBytes: 7, ChunkIndexStart: 1, ChunkIndexEnd: 3},
			{XorbHash: xorbB, SizeBytes: 4, ChunkIndexEnd: 1},
		},
	})
	require.NoError(t, err)

	reader, err := ReconstructFileRange(ctx, registry, NewXorbStore(adapter, "mem://xet-cas"), fileHash, reconstruct.ByteRange{Start: 3, End: 9})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, "ld!!ta", string(got))
}
