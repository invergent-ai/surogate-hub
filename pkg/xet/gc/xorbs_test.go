package gc

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/stretchr/testify/require"
)

func TestListXorbsFromWalkerKeepsMtimeAndSize(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("mem://_hub_xet")
	require.NoError(t, err)
	walker := &fakeWalker{entries: []block.ObjectStoreEntry{
		{RelativeKey: "xet/xorbs/default/xorb-a", Size: 10, Mtime: time.Unix(100, 0)},
		{RelativeKey: "xet/xorbs/other/xorb-b", Size: 20, Mtime: time.Unix(200, 0)},
		{RelativeKey: "not-xet/object"},
	}}

	xorbs, err := ListXorbsFromWalker(ctx, walker, storageURI)

	require.NoError(t, err)
	require.Equal(t, []XorbRef{
		{Prefix: "default", Hash: "xorb-a", Size: 10, Mtime: time.Unix(100, 0)},
		{Prefix: "other", Hash: "xorb-b", Size: 20, Mtime: time.Unix(200, 0)},
	}, xorbs)
}

type fakeWalker struct {
	entries []block.ObjectStoreEntry
}

func (w *fakeWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	for _, entry := range w.entries {
		if err := walkFn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (w *fakeWalker) Marker() block.Mark {
	return block.Mark{}
}

func (w *fakeWalker) GetSkippedEntries() []block.ObjectStoreEntry {
	return nil
}
