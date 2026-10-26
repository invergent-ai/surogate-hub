package gc

import (
	"context"
	"time"

	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

// FileRefLiveFunc reports whether the repository a ref names still exists.
type FileRefLiveFunc func(ctx context.Context, ref xetstore.FileRef) (bool, error)
type ListXorbsFunc func(ctx context.Context) ([]XorbRef, error)

type XorbRef struct {
	Prefix string
	Hash   string
	Mtime  time.Time
}

type Params struct {
	Registry      *xetstore.Registry
	ScanBatchSize int
	FileRefLive   FileRefLiveFunc
	ListXorbs     ListXorbsFunc
	MinAge        time.Duration
	Now           func() time.Time
}

type Report struct {
	FileRefsScanned int
	StaleFileRefs   []xetstore.FileRef
	StaleFiles      []string
	StaleXorbs      []XorbRef
}

// DryRun reports xet data no live repository holds. Nothing deletes it yet.
func DryRun(ctx context.Context, params Params) (Report, error) {
	refs, err := params.Registry.ListAllFileRefs(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	report := Report{FileRefsScanned: len(refs)}
	now := time.Now
	if params.Now != nil {
		now = params.Now
	}

	liveFileHashes := make(map[string]struct{})
	for _, ref := range refs {
		live, err := params.FileRefLive(ctx, ref)
		if err != nil {
			return Report{}, err
		}
		if !live {
			report.StaleFileRefs = append(report.StaleFileRefs, ref)
			continue
		}
		liveFileHashes[ref.FileHash] = struct{}{}
	}

	liveXorbHashes := make(map[string]struct{})
	for fileHash := range liveFileHashes {
		file, err := params.Registry.GetFile(ctx, fileHash)
		if err != nil {
			return Report{}, err
		}
		for _, segment := range file.Segments {
			liveXorbHashes[segment.XorbHash] = struct{}{}
		}
	}

	files, err := params.Registry.ListFileHashes(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	for _, fileHash := range files {
		if _, ok := liveFileHashes[fileHash]; !ok {
			report.StaleFiles = append(report.StaleFiles, fileHash)
		}
	}

	if params.ListXorbs != nil {
		xorbs, err := params.ListXorbs(ctx)
		if err != nil {
			return Report{}, err
		}
		for _, xorb := range xorbs {
			if params.MinAge > 0 && now().Sub(xorb.Mtime) < params.MinAge {
				continue
			}
			if _, ok := liveXorbHashes[xorb.Hash]; !ok {
				report.StaleXorbs = append(report.StaleXorbs, xorb)
			}
		}
	}
	return report, nil
}
