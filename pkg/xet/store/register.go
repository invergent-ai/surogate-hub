package store

import (
	"context"
	"errors"
	"strings"

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

type ChunkRef struct {
	ChunkHash string
	FileHash  string
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

func (r *Registry) ListFileRefs(ctx context.Context, fileHash string, batchSize int) ([]FileRef, error) {
	prefix := fileRefPrefix(fileHash)
	return r.listFileRefsByPrefix(ctx, prefix, batchSize)
}

func (r *Registry) ListAllFileRefs(ctx context.Context, batchSize int) ([]FileRef, error) {
	return r.listFileRefsByPrefix(ctx, "xet/file_refs/", batchSize)
}

func (r *Registry) ListShardFileHashes(ctx context.Context, batchSize int) ([]string, error) {
	prefix := "xet/shard/"
	iter, err := r.store.Scan(ctx, []byte(Partition), kv.ScanOptions{
		KeyStart:  []byte(prefix),
		BatchSize: batchSize,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var hashes []string
	for iter.Next() {
		key := string(iter.Entry().Key)
		if !strings.HasPrefix(key, prefix) {
			break
		}
		hash := strings.TrimPrefix(key, prefix)
		if hash != "" {
			hashes = append(hashes, hash)
		}
	}
	if err := iter.Err(); err != nil && !errors.Is(err, kv.ErrClosedEntries) {
		return nil, err
	}
	return hashes, nil
}

func (r *Registry) ListChunkRefs(ctx context.Context, batchSize int) ([]ChunkRef, error) {
	prefix := "xet/chunk/"
	iter, err := r.store.Scan(ctx, []byte(Partition), kv.ScanOptions{
		KeyStart:  []byte(prefix),
		BatchSize: batchSize,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var refs []ChunkRef
	for iter.Next() {
		entry := iter.Entry()
		key := string(entry.Key)
		if !strings.HasPrefix(key, prefix) {
			break
		}
		chunkHash := strings.TrimPrefix(key, prefix)
		if chunkHash != "" {
			refs = append(refs, ChunkRef{ChunkHash: chunkHash, FileHash: string(entry.Value)})
		}
	}
	if err := iter.Err(); err != nil && !errors.Is(err, kv.ErrClosedEntries) {
		return nil, err
	}
	return refs, nil
}

func (r *Registry) listFileRefsByPrefix(ctx context.Context, prefix string, batchSize int) ([]FileRef, error) {
	iter, err := r.store.Scan(ctx, []byte(Partition), kv.ScanOptions{
		KeyStart:  []byte(prefix),
		BatchSize: batchSize,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var refs []FileRef
	for iter.Next() {
		entry := iter.Entry()
		key := string(entry.Key)
		if !strings.HasPrefix(key, prefix) {
			break
		}
		ref, ok := parseFileRefKey(key)
		if !ok {
			continue
		}
		refs = append(refs, ref)
	}
	if err := iter.Err(); err != nil && !errors.Is(err, kv.ErrClosedEntries) {
		return nil, err
	}
	return refs, nil
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
	return []byte(fileRefPrefix(ref.FileHash) + ref.Repo + "/" + ref.Ref + "/" + ref.Path)
}

func fileRefPrefix(fileHash string) string {
	return "xet/file_refs/" + fileHash + "/"
}

func parseFileRefKey(key string) (FileRef, bool) {
	rest, ok := strings.CutPrefix(key, "xet/file_refs/")
	if !ok {
		return FileRef{}, false
	}
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return FileRef{}, false
	}
	return FileRef{
		FileHash: parts[0],
		Repo:     parts[1],
		Ref:      parts[2],
		Path:     parts[3],
	}, true
}
