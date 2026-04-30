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

func TestGetShardByFileHashReturnsCanonicalShardBytes(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	_, err := registry.RegisterShard(ctx, RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
	})
	require.NoError(t, err)

	shard, err := registry.GetShardByFileHash(ctx, "file-a")
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

func TestHasShardReportsCanonicalShardVisibility(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)

	exists, err := registry.HasShard(ctx, "file-a")
	require.NoError(t, err)
	require.False(t, exists)

	_, err = registry.RegisterShard(ctx, RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("raw-shard"),
	})
	require.NoError(t, err)

	exists, err = registry.HasShard(ctx, "file-a")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestPutFileRefWritesOneKeyPerTuple(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)

	err := registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "models/checkpoint.bin",
	})
	require.NoError(t, err)

	requireKVValue(t, ctx, kvStore, "xet/file_refs/file-a/repo-a/main/models/checkpoint.bin", []byte{})
}

func TestListFileRefsScansOneFileHashPrefix(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	require.NoError(t, registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "models/a.bin",
	}))
	require.NoError(t, registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "repo-b",
		Ref:      "dev",
		Path:     "models/b.bin",
	}))
	require.NoError(t, registry.PutFileRef(ctx, FileRef{
		FileHash: "file-b",
		Repo:     "repo-c",
		Ref:      "main",
		Path:     "models/c.bin",
	}))

	refs, err := registry.ListFileRefs(ctx, "file-a", 32)

	require.NoError(t, err)
	require.ElementsMatch(t, []FileRef{
		{FileHash: "file-a", Repo: "repo-a", Ref: "main", Path: "models/a.bin"},
		{FileHash: "file-a", Repo: "repo-b", Ref: "dev", Path: "models/b.bin"},
	}, refs)
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
