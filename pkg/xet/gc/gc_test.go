package gc

import (
	"context"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/stretchr/testify/require"
)

func TestDryRunReportsStaleFileRefs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-a", "xorb-a")
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

func TestDryRunReportsUnreferencedFiles(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-live", "xorb-live")
	registerTestFile(t, registry, "file-stale", "xorb-stale")
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
	require.Equal(t, []string{"file-stale"}, report.StaleFiles)
}

func TestDryRunReportsUnreferencedXorbs(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-live", "xorb-live")
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
	registerTestFile(t, registry, "file-a", "xorb-a")
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

func TestSweepDeletesStaleFiles(t *testing.T) {
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-live", "xorb-live")
	registerTestFile(t, registry, "file-stale", "xorb-stale")
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
	})

	require.NoError(t, err)
	require.Equal(t, []string{"file-stale"}, report.StaleFiles)
	_, err = registry.GetFile(ctx, "file-live")
	require.NoError(t, err)
	_, err = registry.GetFile(ctx, "file-stale")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestSweepDeletesOnlyOldStaleXorbs(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-live", "xorb-live")
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

func registerTestFile(t *testing.T, registry *xetstore.Registry, fileHash, xorbHash string) {
	t.Helper()
	_, err := registry.RegisterFile(context.Background(), xetstore.ShardFileInfo{
		FileHash: fileHash,
		Segments: []xetstore.ShardFileSegment{{XorbHash: xorbHash}},
	})
	require.NoError(t, err)
}
