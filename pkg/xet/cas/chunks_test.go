package cas

import (
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

	req := httptest.NewRequest(http.MethodGet, "/xet/v1/chunks/default/chunk-a", nil)
	rec := httptest.NewRecorder()

	NewHandler(registry).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	require.Equal(t, []byte("raw-shard"), body)
}
