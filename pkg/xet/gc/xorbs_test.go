package gc

import (
	"context"
	"net/url"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/stretchr/testify/require"
)

func TestListXorbsFromWalkerParsesXETXorbKeys(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("mem://_lakefs_xet")
	require.NoError(t, err)
	walker := &fakeWalker{entries: []block.ObjectStoreEntry{
		{RelativeKey: "xet/xorbs/default/xorb-a"},
		{RelativeKey: "xet/xorbs/other/xorb-b"},
		{RelativeKey: "not-xet/object"},
	}}

	xorbs, err := ListXorbsFromWalker(ctx, walker, storageURI)

	require.NoError(t, err)
	require.Equal(t, []XorbRef{
		{Prefix: "default", Hash: "xorb-a"},
		{Prefix: "other", Hash: "xorb-b"},
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
