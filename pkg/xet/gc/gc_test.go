package gc

import (
	"context"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/stretchr/testify/require"
)

func TestDryRunReportsStaleFileRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("live-shard"),
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "stale.bin",
	}))

	report, err := DryRun(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return ref.Path == "live.bin", nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			return xetstore.ShardInfo{}, nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, Report{
		FileRefsScanned: 2,
		StaleFileRefs: []xetstore.FileRef{{
			FileHash: "file-a",
			Repo:     "repo-a",
			Ref:      "main",
			Path:     "stale.bin",
		}},
	}, report)
	refs, err := registry.ListFileRefs(ctx, "file-a", 32)
	require.NoError(t, err)
	require.Len(t, refs, 2)
}

func TestDryRunReportsUnreferencedShardsAndChunkRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-live",
		Shard:    []byte("live-shard"),
		ChunkIDs: []string{"chunk-live"},
	})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-stale",
		Shard:    []byte("stale-shard"),
		ChunkIDs: []string{"chunk-stale"},
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-live",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))

	report, err := DryRun(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return true, nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			return xetstore.ShardInfo{}, nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"file-stale"}, report.StaleShards)
	require.Equal(t, []xetstore.ChunkRef{{
		ChunkHash: "chunk-stale",
		FileHash:  "file-stale",
	}}, report.StaleChunkRefs)
}

func TestDryRunReportsUnreferencedXorbs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-live",
		Shard:    []byte("live-shard"),
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-live",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))

	report, err := DryRun(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return true, nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			require.Equal(t, []byte("live-shard"), data)
			return xetstore.ShardInfo{XorbHashes: []string{"xorb-live"}}, nil
		},
		ListXorbs: func(ctx context.Context) ([]XorbRef, error) {
			return []XorbRef{
				{Prefix: "default", Hash: "xorb-live"},
				{Prefix: "default", Hash: "xorb-stale"},
			}, nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, []XorbRef{{Prefix: "default", Hash: "xorb-stale"}}, report.StaleXorbs)
}

func TestSweepDeletesStaleFileRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-a",
		Shard:    []byte("live-shard"),
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "stale.bin",
	}))

	report, err := Sweep(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return ref.Path == "live.bin", nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			return xetstore.ShardInfo{}, nil
		},
	})

	require.NoError(t, err)
	require.Len(t, report.StaleFileRefs, 1)
	refs, err := registry.ListFileRefs(ctx, "file-a", 32)
	require.NoError(t, err)
	require.Equal(t, []xetstore.FileRef{{
		FileHash: "file-a",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}}, refs)
}

func TestSweepDeletesStaleShardsAndChunkRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-live",
		Shard:    []byte("live-shard"),
		ChunkIDs: []string{"chunk-live"},
	})
	require.NoError(t, err)
	_, err = registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-stale",
		Shard:    []byte("stale-shard"),
		ChunkIDs: []string{"chunk-stale"},
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-live",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))

	report, err := Sweep(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return true, nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			return xetstore.ShardInfo{}, nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"file-stale"}, report.StaleShards)
	hasLive, err := registry.HasShard(ctx, "file-live")
	require.NoError(t, err)
	require.True(t, hasLive)
	hasStale, err := registry.HasShard(ctx, "file-stale")
	require.NoError(t, err)
	require.False(t, hasStale)
	_, err = registry.GetDedupShardByChunk(ctx, "chunk-stale")
	require.Error(t, err)
}

func TestSweepDeletesOnlyOldStaleXorbs(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	_, err := registry.RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: "file-live",
		Shard:    []byte("live-shard"),
	})
	require.NoError(t, err)
	require.NoError(t, registry.PutFileRef(ctx, xetstore.FileRef{
		FileHash: "file-live",
		Repo:     "repo-a",
		Ref:      "main",
		Path:     "live.bin",
	}))
	var removed []XorbRef

	report, err := Sweep(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return true, nil
		},
		ParseShard: func(data []byte) (xetstore.ShardInfo, error) {
			return xetstore.ShardInfo{XorbHashes: []string{"xorb-live"}}, nil
		},
		ListXorbs: func(ctx context.Context) ([]XorbRef, error) {
			return []XorbRef{
				{Prefix: "default", Hash: "xorb-live", Mtime: now.Add(-48 * time.Hour)},
				{Prefix: "default", Hash: "xorb-old-stale", Mtime: now.Add(-48 * time.Hour)},
				{Prefix: "default", Hash: "xorb-young-stale", Mtime: now.Add(-time.Hour)},
			}, nil
		},
		RemoveXorb: func(ctx context.Context, ref XorbRef) error {
			removed = append(removed, ref)
			return nil
		},
		MinAge: 24 * time.Hour,
		Now:    func() time.Time { return now },
	})

	require.NoError(t, err)
	require.Equal(t, []XorbRef{{
		Prefix: "default",
		Hash:   "xorb-old-stale",
		Mtime:  now.Add(-48 * time.Hour),
	}}, report.StaleXorbs)
	require.Equal(t, report.StaleXorbs, removed)
}
