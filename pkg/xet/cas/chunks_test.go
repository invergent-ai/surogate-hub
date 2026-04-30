package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

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

func testShimFileHash(shard string) string {
	sum := sha256.Sum256([]byte(shard))
	return hex.EncodeToString(sum[:])
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
