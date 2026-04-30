package cas

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestPostShardRegistersChunkDedupIndex(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := xetstore.NewRegistry(kvStore)
	handler := NewHandler(registry)

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewBufferString(`{
		"file_hash": "file-a",
		"shard": "raw-shard",
		"chunk_ids": ["chunk-a"]
	}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"file_hash":"file-a","was_inserted":true}`, rec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/v1/chunks/default/chunk-a", nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	body, err := io.ReadAll(getRec.Result().Body)
	require.NoError(t, err)
	require.Equal(t, []byte("raw-shard"), body)
}
