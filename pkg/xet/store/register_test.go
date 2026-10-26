package store

import (
	"context"
	"errors"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/stretchr/testify/require"
)

func TestRegisterFileKeepsFirstRecord(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	first := ShardFileInfo{FileHash: "file-a", SizeBytes: 12, Segments: []ShardFileSegment{{XorbHash: "xorb-a", SizeBytes: 12, ChunkIndexEnd: 1}}}

	inserted, err := registry.RegisterFile(ctx, first)
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = registry.RegisterFile(ctx, ShardFileInfo{FileHash: "file-a", SizeBytes: 99})
	require.NoError(t, err)
	require.False(t, inserted)

	got, err := registry.GetFile(ctx, "file-a")
	require.NoError(t, err)
	require.Equal(t, first, got)
	hashes, err := registry.ListFileHashes(ctx, 32)
	require.NoError(t, err)
	require.Equal(t, []string{"file-a"}, hashes)
}

func TestGetFileReportsMissingFile(t *testing.T) {
	ctx := context.Background()
	_, err := NewRegistry(kvtest.GetStore(ctx, t)).GetFile(ctx, "file-a")
	require.True(t, errors.Is(err, kv.ErrNotFound), "got %v", err)
}

func TestXorbInfoRoundTrips(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	require.NoError(t, registry.PutXorbInfo(ctx, "xorb-a", []byte("footer")))

	footer, err := registry.GetXorbInfo(ctx, "xorb-a")
	require.NoError(t, err)
	require.Equal(t, []byte("footer"), footer)
}

func TestFileRefsRoundTripRepositoriesWithSlashes(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	refs := []FileRef{
		{FileHash: "file-a", Repo: "test-user/repo-a"},
		{FileHash: "file-a", Repo: "test-user/repo-b"},
		{FileHash: "file-b", Repo: "repo-c"},
	}
	for _, ref := range refs {
		require.NoError(t, registry.PutFileRef(ctx, ref))
	}

	got, err := registry.ListAllFileRefs(ctx, 32)

	require.NoError(t, err)
	require.ElementsMatch(t, refs, got)
	requireKVValue(t, ctx, kvStore, "xet/file_refs/file-a/test-user%2Frepo-a", []byte{})
}

func requireKVValue(t *testing.T, ctx context.Context, kvStore kv.Store, key string, expected []byte) {
	t.Helper()
	res, err := kvStore.Get(ctx, []byte(Partition), []byte(key))
	require.NoError(t, err)
	require.Equal(t, expected, res.Value)
}
