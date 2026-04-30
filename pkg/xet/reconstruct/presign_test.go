package reconstruct

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

func TestPresignedRangeResolverReturnsURLAndBytes(t *testing.T) {
	presigner := &recordingPresigner{url: "https://blocks.example/xorb-a"}
	ranges := map[string]map[IndexRange]HTTPRange{
		"xorb-a": {
			{Start: 1, End: 3}: {Start: 100, End: 299},
		},
	}

	resolve := NewPresignedRangeResolver(context.Background(), presigner, "mem://xet-cas", "default", ranges)
	got, err := resolve("xorb-a", IndexRange{Start: 1, End: 3})
	require.NoError(t, err)

	require.Equal(t, ResolvedRange{
		URL:   "https://blocks.example/xorb-a",
		Bytes: HTTPRange{Start: 100, End: 299},
	}, got)
	require.Equal(t, block.PreSignModeRead, presigner.mode)
	require.Equal(t, block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: "mem://xet-cas",
		Identifier:       "xet/xorbs/default/xorb-a",
		IdentifierType:   block.IdentifierTypeRelative,
	}, presigner.obj)
}

type recordingPresigner struct {
	url  string
	obj  block.ObjectPointer
	mode block.PreSignMode
}

func (p *recordingPresigner) GetPreSignedURL(_ context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, time.Time, error) {
	p.obj = obj
	p.mode = mode
	return p.url, time.Now().Add(time.Minute), nil
}
