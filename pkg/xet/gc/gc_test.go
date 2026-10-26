package gc

import (
	"context"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/stretchr/testify/require"
)

func TestDryRunReportsDataOnlyDeletedRepositoriesHold(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	registerTestFile(t, registry, "file-live", "xorb-live")
	registerTestFile(t, registry, "file-gone", "xorb-gone")
	registerTestFile(t, registry, "file-unlinked", "xorb-unlinked")
	for _, ref := range []xetstore.FileRef{
		{FileHash: "file-live", Repo: "owner/live"},
		{FileHash: "file-live", Repo: "owner/deleted"},
		{FileHash: "file-gone", Repo: "owner/deleted"},
	} {
		require.NoError(t, registry.PutFileRef(ctx, ref))
	}

	report, err := DryRun(ctx, Params{
		Registry: registry,
		FileRefLive: func(ctx context.Context, ref xetstore.FileRef) (bool, error) {
			return ref.Repo == "owner/live", nil
		},
		ListXorbs: func(ctx context.Context) ([]XorbRef, error) {
			return []XorbRef{
				{Prefix: "default", Hash: "xorb-live", Mtime: now.Add(-48 * time.Hour)},
				{Prefix: "default", Hash: "xorb-gone", Mtime: now.Add(-48 * time.Hour)},
				{Prefix: "default", Hash: "xorb-unlinked", Mtime: now.Add(-time.Hour)},
			}, nil
		},
		MinAge: 24 * time.Hour,
		Now:    func() time.Time { return now },
	})

	require.NoError(t, err)
	require.Equal(t, 3, report.FileRefsScanned)
	require.ElementsMatch(t, []xetstore.FileRef{
		{FileHash: "file-live", Repo: "owner/deleted"},
		{FileHash: "file-gone", Repo: "owner/deleted"},
	}, report.StaleFileRefs)
	require.ElementsMatch(t, []string{"file-gone", "file-unlinked"}, report.StaleFiles)
	require.Equal(t, []XorbRef{{Prefix: "default", Hash: "xorb-gone", Mtime: now.Add(-48 * time.Hour)}}, report.StaleXorbs)
}

func registerTestFile(t *testing.T, registry *xetstore.Registry, fileHash, xorbHash string) {
	t.Helper()
	_, err := registry.RegisterFile(context.Background(), xetstore.ShardFileInfo{
		FileHash: fileHash,
		Segments: []xetstore.ShardFileSegment{{XorbHash: xorbHash}},
	})
	require.NoError(t, err)
}
