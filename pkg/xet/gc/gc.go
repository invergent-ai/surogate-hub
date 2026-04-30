package gc

import (
	"context"

	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type FileRefLiveFunc func(ctx context.Context, ref xetstore.FileRef) (bool, error)

type Params struct {
	Registry      *xetstore.Registry
	ScanBatchSize int
	FileRefLive   FileRefLiveFunc
}

type Report struct {
	FileRefsScanned int
	StaleFileRefs   []xetstore.FileRef
}

func DryRun(ctx context.Context, params Params) (Report, error) {
	refs, err := params.Registry.ListAllFileRefs(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	report := Report{FileRefsScanned: len(refs)}
	for _, ref := range refs {
		live, err := params.FileRefLive(ctx, ref)
		if err != nil {
			return Report{}, err
		}
		if !live {
			report.StaleFileRefs = append(report.StaleFileRefs, ref)
		}
	}
	return report, nil
}
