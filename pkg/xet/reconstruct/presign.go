package reconstruct

import (
	"context"
	"fmt"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

type ReadPresigner interface {
	GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, time.Time, error)
}

func NewPresignedRangeResolver(ctx context.Context, presigner ReadPresigner, storageNamespace, prefix string, ranges map[string]map[IndexRange]HTTPRange) RangeResolver {
	return func(xorbHash string, chunks IndexRange) (ResolvedRange, error) {
		byXorb, ok := ranges[xorbHash]
		if !ok {
			return ResolvedRange{}, fmt.Errorf("missing byte ranges for xorb %s", xorbHash)
		}
		byteRange, ok := byXorb[chunks]
		if !ok {
			return ResolvedRange{}, fmt.Errorf("missing byte range for xorb %s chunks %d-%d", xorbHash, chunks.Start, chunks.End)
		}
		url, _, err := presigner.GetPreSignedURL(ctx, xetstore.XorbObjectPointer(storageNamespace, prefix, xorbHash), block.PreSignModeRead)
		if err != nil {
			return ResolvedRange{}, err
		}
		return ResolvedRange{URL: url, Bytes: byteRange}, nil
	}
}
