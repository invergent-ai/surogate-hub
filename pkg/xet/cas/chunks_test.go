package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/xet/reconstruct"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestPostTokenIssuesScopedJWT(t *testing.T) {
	ctx := auth.WithUser(context.Background(), &model.User{Username: "user-a"})
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenTTL(time.Hour),
	)

	req := httptest.NewRequest(http.MethodPost, "/v1/token", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body tokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.NotEmpty(t, body.AccessToken)
	require.Greater(t, body.ExpiresAt, time.Now().Unix())

	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(body.AccessToken, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("test-token-key"), nil
	})
	require.NoError(t, err)
	require.True(t, token.Valid)
	require.Equal(t, "user-a", claims["sub"])
	require.Equal(t, "xet", claims["aud"])
	require.Equal(t, "read write", claims["scope"])
}

func TestRefreshTokenIssuesNewScopedJWT(t *testing.T) {
	ctx := auth.WithUser(context.Background(), &model.User{Username: "user-a"})
	handler := NewHandler(
		xetstore.NewRegistry(kvtest.GetStore(ctx, t)),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenTTL(time.Hour),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/token", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var issued tokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &issued))

	refreshReq := httptest.NewRequest(http.MethodGet, "/v1/token/refresh", nil)
	refreshReq.Header.Set("Authorization", "Bearer "+issued.AccessToken)
	refreshRec := httptest.NewRecorder()
	handler.ServeHTTP(refreshRec, refreshReq)

	require.Equal(t, http.StatusOK, refreshRec.Code)
	var refreshed tokenResponse
	require.NoError(t, json.Unmarshal(refreshRec.Body.Bytes(), &refreshed))
	require.NotEmpty(t, refreshed.AccessToken)
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(refreshed.AccessToken, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("test-token-key"), nil
	})
	require.NoError(t, err)
	require.True(t, token.Valid)
	require.Equal(t, "user-a", claims["sub"])
	require.Equal(t, "read write", claims["scope"])
}

func TestScopedAuthRejectsMissingToken(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := xetstore.NewRegistry(kvStore)
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
		ChunkIDs: []string{"chunk-a"},
	})
	require.NoError(t, err)
	handler := NewHandler(
		registry,
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/chunks/default/chunk-a", nil)
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
	token := testXETToken(t, "user-a", "read", time.Now().Add(time.Hour))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/xorb-a", strings.NewReader("xorb-bytes"))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestGetChunkReturnsDedupShardBytes(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := xetstore.NewRegistry(kvStore)
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
		ChunkIDs: []string{"chunk-a"},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/v1/chunks/default/chunk-a", nil)
	rec := httptest.NewRecorder()

	NewHandler(registry).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	require.Equal(t, []byte("raw-shard"), body)
}

func TestPostXorbStoresBytesAndReportsIdempotency(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/xorb-a", bytes.NewReader([]byte("xorb-bytes")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"was_inserted":true}`, rec.Body.String())

	secondReq := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/xorb-a", bytes.NewReader([]byte("different")))
	secondRec := httptest.NewRecorder()
	handler.ServeHTTP(secondRec, secondReq)

	require.Equal(t, http.StatusOK, secondRec.Code)
	require.JSONEq(t, `{"was_inserted":false}`, secondRec.Body.String())

	reader, err := xorbStore.Get(ctx, "default", "xorb-a")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("xorb-bytes"), body)
}

func TestPostXorbRejectsMismatchedSerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := testSerializedXorb(t, []byte("chunk-data"))

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
	xorbHash, xorbBytes := testSerializedXorb(t, []byte("chunk-data"))

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(xorbBytes))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"was_inserted":true}`, rec.Body.String())
	reader, err := xorbStore.Get(ctx, "default", xorbHash)
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
	xorbHash, xorbBytes := testSerializedXorb(t, []byte("chunk-data"))
	noFooter := stripXorbFooter(t, xorbBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash, bytes.NewReader(noFooter))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	reader, err := xorbStore.Get(ctx, "default", xorbHash)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, xorbBytes, body)
}

func TestPostXorbAcceptsLZ4SerializedHash(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	xorbHash, xorbBytes := testSerializedXorbWithScheme(t, bytes.Repeat([]byte("a"), 2048), 1)

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
	xorbHash, xorbBytes := testSerializedXorbWithScheme(t, bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 512), 2)

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

	xorbHash, xorbBytes := testSerializedXorb(t, []byte("chunk-data"))
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

func testXETToken(t *testing.T, subject string, scope string, expires time.Time) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   subject,
		"aud":   "xet",
		"scope": scope,
		"iat":   time.Now().Unix(),
		"exp":   expires.Unix(),
	})
	signed, err := token.SignedString([]byte("test-token-key"))
	require.NoError(t, err)
	return signed
}

func TestPostShardRegistersChunkDedupIndex(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := xetstore.NewRegistry(kvStore)
	handler := NewHandler(registry)
	fileHash := testShimFileHash("raw-shard")

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewBufferString(fmt.Sprintf(`{
		"file_hash": %q,
		"shard": "raw-shard",
		"chunk_ids": ["chunk-a"]
	}`, fileHash)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, fmt.Sprintf(`{"file_hash":%q,"was_inserted":true}`, fileHash), rec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/v1/chunks/default/chunk-a", nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	body, err := io.ReadAll(getRec.Result().Body)
	require.NoError(t, err)
	require.Equal(t, []byte("raw-shard"), body)
}

func TestPostShardRejectsMismatchedComputedFileHash(t *testing.T) {
	ctx := context.Background()
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)))

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewBufferString(`{
		"file_hash": "wrong-hash",
		"shard": "raw-shard",
		"chunk_ids": ["chunk-a"]
	}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "file_hash does not match shard")
}

func TestPostShardRejectsMissingReferencedXorb(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	fileHash := testShimFileHash("raw-shard")

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewBufferString(fmt.Sprintf(`{
		"file_hash": %q,
		"shard": "raw-shard",
		"chunk_ids": ["chunk-a"],
		"xorb_ids": ["missing-xorb"]
	}`, fileHash)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "xorb missing-xorb not found")
}

func TestPostShardAcceptsExistingReferencedXorb(t *testing.T) {
	ctx := context.Background()
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	_, err := xorbStore.Put(ctx, "default", "xorb-a", int64(len("xorb-bytes")), bytes.NewReader([]byte("xorb-bytes")))
	require.NoError(t, err)
	handler := NewHandler(xetstore.NewRegistry(kvtest.GetStore(ctx, t)), WithXorbStore(xorbStore))
	fileHash := testShimFileHash("raw-shard")

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewBufferString(fmt.Sprintf(`{
		"file_hash": %q,
		"shard": "raw-shard",
		"chunk_ids": ["chunk-a"],
		"xorb_ids": ["xorb-a"]
	}`, fileHash)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, fmt.Sprintf(`{"file_hash":%q,"was_inserted":true}`, fileHash), rec.Body.String())
}

func TestPostBinaryShardRegistersFileAndChunks(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := xetstore.NewRegistry(kvStore)
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(registry, WithXorbStore(xorbStore))

	xorbHash := "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
	chunkHash := "aaaaaaaaaaaaaaaa000000000000000000000000000000000000000000000000"
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = xorbStore.Put(ctx, "default", xorbHash, int64(len("xorb-bytes")), bytes.NewReader([]byte("xorb-bytes")))
	require.NoError(t, err)
	shard := testXETBinaryShard(t, fileHash, xorbHash, chunkHash)

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(shard))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"result":1}`, rec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/v1/chunks/default/"+chunkHash, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	body, err := io.ReadAll(getRec.Result().Body)
	require.NoError(t, err)
	require.Equal(t, shard, body)
	hasShard, err := registry.HasShard(ctx, fileHash)
	require.NoError(t, err)
	require.True(t, hasShard)
	meta, err := kvStore.Get(ctx, []byte(xetstore.Partition), []byte("xet/shard_meta/"+fileHash))
	require.NoError(t, err)
	require.JSONEq(t, `{"created_at":0,"size":12,"num_xorbs":1,"num_chunks":1}`, string(meta.Value))
}

func TestPostRootShardRegistersFileAndChunks(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	handler := NewHandler(registry, WithXorbStore(xorbStore))

	xorbHash := "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
	chunkHash := "aaaaaaaaaaaaaaaa000000000000000000000000000000000000000000000000"
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = xorbStore.Put(ctx, "default", xorbHash, int64(len("xorb-bytes")), bytes.NewReader([]byte("xorb-bytes")))
	require.NoError(t, err)
	shard := testXETBinaryShard(t, fileHash, xorbHash, chunkHash)

	req := httptest.NewRequest(http.MethodPost, "/shards", bytes.NewReader(shard))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"result":1}`, rec.Body.String())
	hasShard, err := registry.HasShard(ctx, fileHash)
	require.NoError(t, err)
	require.True(t, hasShard)
}

func TestPostDuplicateAbbreviatedShardIsIdempotent(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	handler := NewHandler(registry, WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")))
	abbreviatedShard := testHFAbbreviatedShard(t)
	fileHash := "42226dec0f0a21fe4e9404480ecdc7714b963c61c37c7870b2c8b18dea5b274c"
	chunkHash := "9b23a87215b2aaf3f91275d77f3aafce3079a83de4d13523d010c51eb11489a3"
	canonicalShard := testXETBinaryShard(
		t,
		fileHash,
		"3a6082dc9d95bdb717b5d799bdbbb71ec4b3b3502847e0d50f536113e143f0c8",
		chunkHash,
	)
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    canonicalShard,
		ChunkIDs: []string{chunkHash},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shards", bytes.NewReader(abbreviatedShard))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.JSONEq(t, `{"result":0}`, rec.Body.String())
	stored, err := registry.GetShardByFileHash(ctx, fileHash)
	require.NoError(t, err)
	require.Equal(t, canonicalShard, stored)
}

func TestGetReconstructionReturnsV2Manifest(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	chunkHash := xetstore.ComputeDataHash([]byte("hello world!"))
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
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
	chunkHash := xetstore.ComputeDataHash([]byte("hello world!"))
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
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

func TestGetReconstructionRequiresDirectCapabilityWhenConfigured(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	chunkHash := xetstore.ComputeDataHash([]byte("hello world!"))
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
	called := false
	handler := NewHandler(
		registry,
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
		WithReconstructionCapabilityChecker(func(ctx context.Context, fileHash string, logical ReconstructionLogicalContext) error {
			called = true
			require.Equal(t, "repo-a", logical.Repo)
			require.Equal(t, "main", logical.Ref)
			require.Equal(t, "models/checkpoint.bin", logical.Path)
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

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash+"?repo=repo-a&ref=main&path=models/checkpoint.bin", nil)
	req.Header.Set("Authorization", "Bearer "+testXETToken(t, "user-a", "read", time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, called)
}

func TestGetReconstructionRejectsMissingDirectCapabilityContext(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	chunkHash := xetstore.ComputeDataHash([]byte("hello world!"))
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
	handler := NewHandler(
		registry,
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
		WithReconstructionCapabilityChecker(func(ctx context.Context, fileHash string, logical ReconstructionLogicalContext) error {
			require.Empty(t, logical.Repo)
			require.Empty(t, logical.Ref)
			require.Empty(t, logical.Path)
			return ErrReconstructionCapabilityNotFound
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileHash, nil)
	req.Header.Set("Authorization", "Bearer "+testXETToken(t, "user-a", "read", time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetReconstructionHonorsRangeHeader(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	chunkHash := xetstore.ComputeDataHash([]byte("hello world!"))
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: 12,
	}})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
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
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	chunk := []byte("hello world!")
	xorbHash, xorbBytes := testSerializedXorb(t, chunk)
	chunkHash := xetstore.ComputeDataHash(chunk)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)
	_, err = xorbStore.Put(ctx, "default", xorbHash, int64(len(xorbBytes)), bytes.NewReader(xorbBytes))
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)
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

	require.Equal(t, http.StatusOK, proxyRec.Code)
	require.Equal(t, xorbBytes[byteRange.Start:byteRange.End+1], proxyRec.Body.Bytes())
}

func TestReconstructionGrantedRangesMatchGroupedManifestTerms(t *testing.T) {
	handler := &Handler{}
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

	grants, err := handler.reconstructionGrantedRanges(context.Background(), terms, parsed)
	require.NoError(t, err)

	require.Equal(t, []reconstruct.HTTPRange{
		{Start: 0, End: 19},
		{Start: 20, End: 29},
	}, grants["xorb-a"])
}

func TestReconstructFileRangeReadsXorbBytes(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	xorbStore := NewXorbStore(mem.New(ctx), "mem://xet-cas")
	chunk := []byte("hello world!")
	xorbHash, xorbBytes := testSerializedXorb(t, chunk)
	chunkHash := xetstore.ComputeDataHash(chunk)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)
	_, err = xorbStore.Put(ctx, "default", xorbHash, int64(len(xorbBytes)), bytes.NewReader(xorbBytes))
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testXETBinaryShard(t, fileHash, xorbHash, chunkHash),
	})
	require.NoError(t, err)

	reader, err := ReconstructFileRange(ctx, registry, xorbStore, fileHash, reconstruct.ByteRange{Start: 3, End: 9})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, []byte("lo wor"), got)
}

func testShimFileHash(shard string) string {
	sum := sha256.Sum256([]byte(shard))
	return hex.EncodeToString(sum[:])
}

func testHFAbbreviatedShard(t *testing.T) []byte {
	t.Helper()
	shard, err := base64.StdEncoding.DecodeString("SEZSZXBvTWV0YURhdGEAVWlnRWp7gVeDpb3ZXM3RSqkCAAAAAAAAAAAAAAAAAAAA/iEKD+xtIkJxx80OSASUTnB4fMNhPJZLTCdb6o2xyLIAAADAAQAAAAAAAAAAAAAAt72VndyCYDoet7u9mde1F9XgRyhQs7PEyPBD4RNhUw8AAAAABBADAAAAAAACAAAASjM7S0fdp35JEFNeZGUvD497UZdxStR3ZflLrOyaGVMAAAAAAAAAAAAAAAAAAAAAye/Q1kDccACMFyFz8/1cV7h0QlzKd10VNm2G5UuZoeYAAAAAAAAAAAAAAAAAAAAA//////////////////////////////////////////8AAAAAAAAAAAAAAAAAAAAA//////////////////////////////////////////8AAAAAAAAAAAAAAAAAAAAA")
	require.NoError(t, err)
	return shard
}

func testXETBinaryShard(t *testing.T, fileHash, xorbHash, chunkHash string) []byte {
	t.Helper()
	var b bytes.Buffer
	b.Write([]byte{'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0, 85,
		105, 103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169})
	testWriteU64(&b, 2)
	testWriteU64(&b, 200)

	testWriteHash(t, &b, fileHash)
	testWriteU32(&b, 0)
	testWriteU32(&b, 1)
	testWriteU64(&b, 0)

	testWriteHash(t, &b, xorbHash)
	testWriteU32(&b, 0)
	testWriteU32(&b, 12)
	testWriteU32(&b, 0)
	testWriteU32(&b, 1)

	b.Write(bytes.Repeat([]byte{0xff}, 32))
	testWriteU32(&b, 0)
	testWriteU32(&b, 0)
	testWriteU64(&b, 0)

	testWriteHash(t, &b, xorbHash)
	testWriteU32(&b, 0)
	testWriteU32(&b, 1)
	testWriteU32(&b, 12)
	testWriteU32(&b, 10)

	testWriteHash(t, &b, chunkHash)
	testWriteU32(&b, 0)
	testWriteU32(&b, 12)
	testWriteU32(&b, 0)
	testWriteU32(&b, 0)

	b.Write(bytes.Repeat([]byte{0xff}, 32))
	testWriteU32(&b, 0)
	testWriteU32(&b, 0)
	testWriteU32(&b, 0)
	testWriteU32(&b, 0)

	footerOffset := uint64(b.Len())
	testWriteU64(&b, 1)
	testWriteU64(&b, 48)
	testWriteU64(&b, 192)
	testWriteU64(&b, footerOffset)
	testWriteU64(&b, 0)
	testWriteU64(&b, footerOffset)
	testWriteU64(&b, 0)
	testWriteU64(&b, footerOffset)
	testWriteU64(&b, 0)
	b.Write(make([]byte, 32))
	testWriteU64(&b, 0)
	testWriteU64(&b, ^uint64(0))
	for i := 0; i < 6; i++ {
		testWriteU64(&b, 0)
	}
	testWriteU64(&b, 10)
	testWriteU64(&b, 12)
	testWriteU64(&b, 12)
	testWriteU64(&b, footerOffset)

	return b.Bytes()
}

func testWriteHash(t *testing.T, b *bytes.Buffer, value string) {
	t.Helper()
	raw, err := hex.DecodeString(value)
	require.NoError(t, err)
	require.Len(t, raw, 32)
	for i := 0; i < 4; i++ {
		for j := 7; j >= 0; j-- {
			b.WriteByte(raw[i*8+j])
		}
	}
}

func testWriteU32(b *bytes.Buffer, value uint32) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func testWriteU64(b *bytes.Buffer, value uint64) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func testSerializedXorb(t *testing.T, chunk []byte) (string, []byte) {
	return testSerializedXorbWithScheme(t, chunk, 0)
}

func stripXorbFooter(t *testing.T, xorbBytes []byte) []byte {
	t.Helper()
	require.GreaterOrEqual(t, len(xorbBytes), 4)
	infoLength := int(binary.LittleEndian.Uint32(xorbBytes[len(xorbBytes)-4:]))
	footerStart := len(xorbBytes) - 4 - infoLength
	require.GreaterOrEqual(t, footerStart, 0)
	return xorbBytes[:footerStart]
}

func testSerializedXorbWithScheme(t *testing.T, chunk []byte, scheme byte) (string, []byte) {
	t.Helper()
	chunkHash := xetstore.ComputeDataHash(chunk)
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)

	var b bytes.Buffer
	serializedChunk := chunk
	if scheme == 2 {
		chunk = bg4Split(chunk)
	}
	if scheme == 1 || scheme == 2 {
		var compressed bytes.Buffer
		writer := lz4.NewWriter(&compressed)
		_, err := writer.Write(chunk)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		serializedChunk = compressed.Bytes()
	}
	b.WriteByte(0)
	writeThreeByteLE(&b, uint32(len(serializedChunk)))
	b.WriteByte(scheme)
	writeThreeByteLE(&b, uint32(len(chunk)))
	b.Write(serializedChunk)

	chunkBoundary := uint32(b.Len())
	b.WriteString("XETBLOB")
	b.WriteByte(1)
	testWriteHash(t, &b, xorbHash)

	b.WriteString("XBLBHSH")
	b.WriteByte(0)
	testWriteU32(&b, 1)
	testWriteHash(t, &b, chunkHash)

	b.WriteString("XBLBBND")
	b.WriteByte(1)
	testWriteU32(&b, 1)
	testWriteU32(&b, chunkBoundary)
	testWriteU32(&b, uint32(len(chunk)))
	testWriteU32(&b, 1)
	testWriteU32(&b, 92)
	testWriteU32(&b, 48)
	b.Write(make([]byte, 16))
	testWriteU32(&b, 132)

	return xorbHash, b.Bytes()
}

func writeThreeByteLE(b *bytes.Buffer, value uint32) {
	b.WriteByte(byte(value))
	b.WriteByte(byte(value >> 8))
	b.WriteByte(byte(value >> 16))
}

func bg4Split(data []byte) []byte {
	n := len(data)
	split := n / 4
	rem := n % 4
	grouped := make([]byte, n)
	g0 := 0
	g1 := g0 + split + min(1, rem)
	g2 := g1 + split + min(1, max(0, rem-1))
	g3 := g2 + split + min(1, max(0, rem-2))
	for i := 0; i < split; i++ {
		grouped[g0+i] = data[4*i]
		grouped[g1+i] = data[4*i+1]
		grouped[g2+i] = data[4*i+2]
		grouped[g3+i] = data[4*i+3]
	}
	switch rem {
	case 1:
		grouped[g0+split] = data[4*split]
	case 2:
		grouped[g0+split] = data[4*split]
		grouped[g1+split] = data[4*split+1]
	case 3:
		grouped[g0+split] = data[4*split]
		grouped[g1+split] = data[4*split+1]
		grouped[g2+split] = data[4*split+2]
	}
	return grouped
}
