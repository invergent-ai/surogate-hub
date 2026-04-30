package gc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

func TestDryRunReportsStaleFileRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
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
	})

	require.NoError(t, err)
	require.Equal(t, []string{"file-stale"}, report.StaleShards)
	require.Equal(t, []xetstore.ChunkRef{{
		ChunkHash: "chunk-stale",
		FileHash:  "file-stale",
	}}, report.StaleChunkRefs)
}
