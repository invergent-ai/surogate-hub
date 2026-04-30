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
	StaleShards     []string
	StaleChunkRefs  []xetstore.ChunkRef
}

func DryRun(ctx context.Context, params Params) (Report, error) {
	refs, err := params.Registry.ListAllFileRefs(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	report := Report{FileRefsScanned: len(refs)}
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

	shards, err := params.Registry.ListShardFileHashes(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	for _, fileHash := range shards {
		if _, ok := liveFileHashes[fileHash]; !ok {
			report.StaleShards = append(report.StaleShards, fileHash)
		}
	}

	chunkRefs, err := params.Registry.ListChunkRefs(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	for _, ref := range chunkRefs {
		if _, ok := liveFileHashes[ref.FileHash]; !ok {
			report.StaleChunkRefs = append(report.StaleChunkRefs, ref)
		}
	}
	return report, nil
}
