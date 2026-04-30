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
