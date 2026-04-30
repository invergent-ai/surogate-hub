package store

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/kv"
)

const Partition = "xet"

type Registry struct {
	store kv.Store
}

type RegisterShardParams struct {
	FileHash string
	Shard    []byte
	Summary  []byte
	ChunkIDs []string
}

type RegisterShardResult struct {
	WasInserted bool
}

type FileRef struct {
	FileHash string
	Repo     string
	Ref      string
	Path     string
}

func NewRegistry(store kv.Store) *Registry {
	return &Registry{store: store}
}

func (r *Registry) RegisterShard(ctx context.Context, params RegisterShardParams) (RegisterShardResult, error) {
	inserted := true
	err := r.store.SetIf(ctx, []byte(Partition), shardKey(params.FileHash), params.Shard, nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		inserted = false
	} else if err != nil {
		return RegisterShardResult{}, err
	}

	if inserted && len(params.Summary) > 0 {
		if err := r.store.Set(ctx, []byte(Partition), shardMetaKey(params.FileHash), params.Summary); err != nil {
			return RegisterShardResult{}, err
		}
	}

	for _, chunkID := range params.ChunkIDs {
		err := r.store.SetIf(ctx, []byte(Partition), chunkKey(chunkID), []byte(params.FileHash), nil)
		if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
			return RegisterShardResult{}, err
		}
	}

	return RegisterShardResult{WasInserted: inserted}, nil
}

func (r *Registry) GetDedupShardByChunk(ctx context.Context, chunkID string) ([]byte, error) {
	chunk, err := r.store.Get(ctx, []byte(Partition), chunkKey(chunkID))
	if err != nil {
		return nil, err
	}
	return r.GetShardByFileHash(ctx, string(chunk.Value))
}

func (r *Registry) GetShardByFileHash(ctx context.Context, fileHash string) ([]byte, error) {
	shard, err := r.store.Get(ctx, []byte(Partition), shardKey(fileHash))
	if err != nil {
		return nil, err
	}
	return shard.Value, nil
}

func (r *Registry) HasShard(ctx context.Context, fileHash string) (bool, error) {
	_, err := r.store.Get(ctx, []byte(Partition), shardKey(fileHash))
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Registry) PutFileRef(ctx context.Context, ref FileRef) error {
	return r.store.Set(ctx, []byte(Partition), fileRefKey(ref), []byte{})
}

func shardKey(fileHash string) []byte {
	return []byte("xet/shard/" + fileHash)
}

func shardMetaKey(fileHash string) []byte {
	return []byte("xet/shard_meta/" + fileHash)
}

func chunkKey(chunkID string) []byte {
	return []byte("xet/chunk/" + chunkID)
}

func fileRefKey(ref FileRef) []byte {
	return []byte("xet/file_refs/" + ref.FileHash + "/" + ref.Repo + "/" + ref.Ref + "/" + ref.Path)
}
