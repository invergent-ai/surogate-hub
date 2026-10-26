package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

const Partition = "xet"

type Registry struct {
	store kv.Store
}

// FileRef records that a repository holds a file. xet GC keeps a file while
// any repository that holds it exists.
type FileRef struct {
	FileHash string
	Repo     string
}

func NewRegistry(store kv.Store) *Registry {
	return &Registry{store: store}
}

// RegisterFile stores a verified file record. Records are content-addressed, so
// the first one for a hash wins and later uploads of the same file are no-ops.
func (r *Registry) RegisterFile(ctx context.Context, file ShardFileInfo) (bool, error) {
	data, err := json.Marshal(file)
	if err != nil {
		return false, err
	}
	err = r.store.SetIf(ctx, []byte(Partition), fileKey(file.FileHash), data, nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Registry) GetFile(ctx context.Context, fileHash string) (ShardFileInfo, error) {
	res, err := r.store.Get(ctx, []byte(Partition), fileKey(fileHash))
	if err != nil {
		return ShardFileInfo{}, err
	}
	var file ShardFileInfo
	if err := json.Unmarshal(res.Value, &file); err != nil {
		return ShardFileInfo{}, fmt.Errorf("decode xet file %s: %w", fileHash, err)
	}
	return file, nil
}

func (r *Registry) ListFileHashes(ctx context.Context, batchSize int) ([]string, error) {
	prefix := "xet/file/"
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
		if hash := strings.TrimPrefix(key, prefix); hash != "" {
			hashes = append(hashes, hash)
		}
	}
	if err := iter.Err(); err != nil && !errors.Is(err, kv.ErrClosedEntries) {
		return nil, err
	}
	return hashes, nil
}

// PutXorbInfo stores a verified xorb's footer: its chunk hashes, compressed
// chunk boundaries and unpacked offsets.
func (r *Registry) PutXorbInfo(ctx context.Context, xorbHash string, footer []byte) error {
	return r.store.Set(ctx, []byte(Partition), xorbKey(xorbHash), footer)
}

func (r *Registry) GetXorbInfo(ctx context.Context, xorbHash string) ([]byte, error) {
	res, err := r.store.Get(ctx, []byte(Partition), xorbKey(xorbHash))
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

func (r *Registry) PutFileRef(ctx context.Context, ref FileRef) error {
	return r.store.Set(ctx, []byte(Partition), fileRefKey(ref), []byte{})
}

func (r *Registry) ListAllFileRefs(ctx context.Context, batchSize int) ([]FileRef, error) {
	prefix := "xet/file_refs/"
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

func fileKey(fileHash string) []byte {
	return []byte("xet/file/" + fileHash)
}

func xorbKey(xorbHash string) []byte {
	return []byte("xet/xorb/" + xorbHash)
}

func fileRefKey(ref FileRef) []byte {
	return []byte("xet/file_refs/" + ref.FileHash + "/" + url.PathEscape(ref.Repo))
}

func parseFileRefKey(key string) (FileRef, bool) {
	rest, ok := strings.CutPrefix(key, "xet/file_refs/")
	if !ok {
		return FileRef{}, false
	}
	fileHash, escapedRepo, ok := strings.Cut(rest, "/")
	if !ok || fileHash == "" || escapedRepo == "" {
		return FileRef{}, false
	}
	repo, err := url.PathUnescape(escapedRepo)
	if err != nil {
		return FileRef{}, false
	}
	return FileRef{FileHash: fileHash, Repo: repo}, true
}
