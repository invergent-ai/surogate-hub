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
		{FileHash: "file-a", Repo: "test-user/repo-a", SizeBytes: 12},
		{FileHash: "file-a", Repo: "test-user/repo-b", SizeBytes: 12},
		{FileHash: "file-b", Repo: "repo-c", SizeBytes: 7},
	}
	for _, ref := range refs {
		_, err := registry.AddFileRef(ctx, ref)
		require.NoError(t, err)
	}

	got, err := registry.ListAllFileRefs(ctx, 32)

	require.NoError(t, err)
	require.ElementsMatch(t, refs, got)
	requireKVValue(t, ctx, kvStore, "xet/file_refs/test-user%2Frepo-a/file-a", []byte("12"))
}

func TestAddFileRefCreatesOnlyWhenAbsent(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))

	created, err := registry.AddFileRef(ctx, FileRef{FileHash: "file-a", Repo: "o/a", SizeBytes: 12})
	require.NoError(t, err)
	require.True(t, created)
	created, err = registry.AddFileRef(ctx, FileRef{FileHash: "file-a", Repo: "o/a", SizeBytes: 99})
	require.NoError(t, err)
	require.False(t, created)

	got, err := registry.RepoXetBytes(ctx, "o/a")
	require.NoError(t, err)
	require.Equal(t, int64(12), got)
}

func TestHasFileRefIsPerRepository(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.AddFileRef(ctx, FileRef{FileHash: "file-a", Repo: "o/a", SizeBytes: 12})
	require.NoError(t, err)

	for repo, expected := range map[string]bool{"o/a": true, "o/ab": false, "o/b": false} {
		held, err := registry.HasFileRef(ctx, repo, "file-a")
		require.NoError(t, err)
		require.Equal(t, expected, held, repo)
	}
	held, err := registry.HasFileRef(ctx, "o/a", "file-b")
	require.NoError(t, err)
	require.False(t, held)
}

func TestRepoXetBytesSumsOnlyThatRepository(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	for _, ref := range []FileRef{
		{FileHash: "file-a", Repo: "o/a", SizeBytes: 10},
		{FileHash: "file-b", Repo: "o/a", SizeBytes: 5},
		{FileHash: "file-a", Repo: "o/ab", SizeBytes: 100},
	} {
		_, err := registry.AddFileRef(ctx, ref)
		require.NoError(t, err)
	}

	for repo, expected := range map[string]int64{"o/a": 15, "o/ab": 100, "o/none": 0} {
		got, err := registry.RepoXetBytes(ctx, repo)
		require.NoError(t, err)
		require.Equal(t, expected, got, repo)
	}
}

func TestDeleteRepoFileRefsKeepsOtherRepositories(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	kept := []FileRef{
		{FileHash: "file-a", Repo: "o/ab", SizeBytes: 100},
		{FileHash: "file-c", Repo: "o/b", SizeBytes: 3},
	}
	for _, ref := range append([]FileRef{
		{FileHash: "file-a", Repo: "o/a", SizeBytes: 10},
		{FileHash: "file-b", Repo: "o/a", SizeBytes: 5},
	}, kept...) {
		_, err := registry.AddFileRef(ctx, ref)
		require.NoError(t, err)
	}

	require.NoError(t, registry.DeleteRepoFileRefs(ctx, "o/a"))

	got, err := registry.ListAllFileRefs(ctx, 32)
	require.NoError(t, err)
	require.ElementsMatch(t, kept, got)
}

func requireKVValue(t *testing.T, ctx context.Context, kvStore kv.Store, key string, expected []byte) {
	t.Helper()
	res, err := kvStore.Get(ctx, []byte(Partition), []byte(key))
	require.NoError(t, err)
	require.Equal(t, expected, res.Value)
}
