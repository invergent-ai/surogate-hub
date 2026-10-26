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

func TestPutFileRefWritesOneKeyPerTuple(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)

	err := registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "test-user/repo-a",
		Ref:      "feature/xet",
		Path:     "models/checkpoint.bin",
	})
	require.NoError(t, err)

	requireKVValue(t, ctx, kvStore, "xet/file_refs/file-a/test-user%2Frepo-a/feature%2Fxet/models%2Fcheckpoint.bin", []byte{})
}

func TestListFileRefsScansOneFileHashPrefix(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	registry := NewRegistry(kvStore)
	require.NoError(t, registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "test-user/repo-a",
		Ref:      "main",
		Path:     "models/a.bin",
	}))
	require.NoError(t, registry.PutFileRef(ctx, FileRef{
		FileHash: "file-a",
		Repo:     "test-user/repo-b",
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
		{FileHash: "file-a", Repo: "test-user/repo-a", Ref: "main", Path: "models/a.bin"},
		{FileHash: "file-a", Repo: "test-user/repo-b", Ref: "dev", Path: "models/b.bin"},
	}, refs)
}

func requireKVValue(t *testing.T, ctx context.Context, kvStore kv.Store, key string, expected []byte) {
	t.Helper()
	res, err := kvStore.Get(ctx, []byte(Partition), []byte(key))
	require.NoError(t, err)
	require.Equal(t, expected, res.Value)
}
