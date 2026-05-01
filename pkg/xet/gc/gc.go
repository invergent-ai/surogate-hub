package gc

import (
	"context"
	"time"

	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

type FileRefLiveFunc func(ctx context.Context, ref xetstore.FileRef) (bool, error)
type ParseShardFunc func(data []byte) (xetstore.ShardInfo, error)
type ListXorbsFunc func(ctx context.Context) ([]XorbRef, error)
type RemoveXorbFunc func(ctx context.Context, ref XorbRef) error

type XorbRef struct {
	Prefix string
	Hash   string
	Mtime  time.Time
}

type Params struct {
	Registry      *xetstore.Registry
	ScanBatchSize int
	FileRefLive   FileRefLiveFunc
	ParseShard    ParseShardFunc
	ListXorbs     ListXorbsFunc
	RemoveXorb    RemoveXorbFunc
	MinAge        time.Duration
	Now           func() time.Time
}

type Report struct {
	FileRefsScanned int
	StaleFileRefs   []xetstore.FileRef
	StaleShards     []string
	StaleChunkRefs  []xetstore.ChunkRef
	StaleXorbs      []XorbRef
}

func DryRun(ctx context.Context, params Params) (Report, error) {
	refs, err := params.Registry.ListAllFileRefs(ctx, params.ScanBatchSize)
	if err != nil {
		return Report{}, err
	}
	report := Report{FileRefsScanned: len(refs)}
	parseShard := params.ParseShard
	if parseShard == nil {
		parseShard = xetstore.ParseShardInfo
	}
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
		shard, err := params.Registry.GetShardByFileHash(ctx, fileHash)
		if err != nil {
			return Report{}, err
		}
		info, err := parseShard(shard)
		if err != nil {
			return Report{}, err
		}
		for _, xorbHash := range info.XorbHashes {
			liveXorbHashes[xorbHash] = struct{}{}
		}
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

func Sweep(ctx context.Context, params Params) (Report, error) {
	report, err := DryRun(ctx, params)
	if err != nil {
		return Report{}, err
	}
	for _, ref := range report.StaleFileRefs {
		if err := params.Registry.DeleteFileRef(ctx, ref); err != nil {
			return Report{}, err
		}
	}
	for _, ref := range report.StaleChunkRefs {
		if err := params.Registry.DeleteChunkRef(ctx, ref); err != nil {
			return Report{}, err
		}
	}
	for _, fileHash := range report.StaleShards {
		if err := params.Registry.DeleteShard(ctx, fileHash); err != nil {
			return Report{}, err
		}
	}
	if params.RemoveXorb != nil {
		for _, xorb := range report.StaleXorbs {
			if err := params.RemoveXorb(ctx, xorb); err != nil {
				return Report{}, err
			}
		}
	}
	return report, nil
}
