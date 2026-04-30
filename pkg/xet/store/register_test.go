package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestRegisterShardWritesCanonicalShardAndIndexesChunks(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)

	res, err := registry.RegisterShard(ctx, RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
		Summary:  []byte(`{"size":9}`),
		ChunkIDs: []string{"chunk-a", "chunk-b"},
	})
	require.NoError(t, err)
	require.True(t, res.WasInserted)

	requireKVValue(t, ctx, kvStore, "xet/shard/file-a", []byte("raw-shard"))
	requireKVValue(t, ctx, kvStore, "xet/shard_meta/file-a", []byte(`{"size":9}`))
	requireKVValue(t, ctx, kvStore, "xet/chunk/chunk-a", []byte("file-a"))
	requireKVValue(t, ctx, kvStore, "xet/chunk/chunk-b", []byte("file-a"))
}

func TestGetDedupShardByChunkReturnsShardBytes(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	_, err := registry.RegisterShard(ctx, RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
		ChunkIDs: []string{"chunk-a"},
	})
	require.NoError(t, err)

	shard, err := registry.GetDedupShardByChunk(ctx, "chunk-a")
	require.NoError(t, err)
	require.Equal(t, []byte("raw-shard"), shard)
}

func TestGetDedupShardByChunkReturnsNotFoundWhenIndexedShardIsMissing(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	require.NoError(t, kvStore.Set(ctx, []byte(Partition), []byte("xet/chunk/chunk-a"), []byte("missing-file")))

	_, err := registry.GetDedupShardByChunk(ctx, "chunk-a")
	require.True(t, errors.Is(err, kv.ErrNotFound), "expected missing shard to look like a dedup miss, got %v", err)
}

func requireKVValue(t *testing.T, ctx context.Context, kvStore kv.Store, key string, expected []byte) {
	t.Helper()
	res, err := kvStore.Get(ctx, []byte(Partition), []byte(key))
	require.NoError(t, err)
	require.Equal(t, expected, res.Value)
}

func requireKVNotFound(t *testing.T, ctx context.Context, kvStore kv.Store, key string) {
	t.Helper()
	_, err := kvStore.Get(ctx, []byte(Partition), []byte(key))
	require.True(t, errors.Is(err, kv.ErrNotFound), "expected %s to be missing, got %v", key, err)
}
