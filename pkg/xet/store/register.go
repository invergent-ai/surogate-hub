package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

const Partition = "xet"

type Registry struct {
	store kv.Store
}

// FileRef records that a repository holds a file. xet GC keeps a file while
// any repository that holds it exists, and the repository's storage usage
// counts the file's SizeBytes once.
type FileRef struct {
	FileHash  string
	Repo      string
	SizeBytes uint64
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
	return r.store.Set(ctx, []byte(Partition), fileRefKey(ref.Repo, ref.FileHash), []byte(strconv.FormatUint(ref.SizeBytes, 10)))
}

func (r *Registry) HasFileRef(ctx context.Context, repo, fileHash string) (bool, error) {
	_, err := r.store.Get(ctx, []byte(Partition), fileRefKey(repo, fileHash))
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// RepoXetBytes returns the total size of the files the repository holds.
func (r *Registry) RepoXetBytes(ctx context.Context, repo string) (int64, error) {
	refs, err := r.scanFileRefs(ctx, repoFileRefPrefix(repo), 0)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, ref := range refs {
		total += int64(ref.SizeBytes)
	}
	return total, nil
}

// DeleteRepoFileRefs drops every file ref of a deleted repository.
func (r *Registry) DeleteRepoFileRefs(ctx context.Context, repo string) error {
	refs, err := r.scanFileRefs(ctx, repoFileRefPrefix(repo), 0)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := r.store.Delete(ctx, []byte(Partition), fileRefKey(ref.Repo, ref.FileHash)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) ListAllFileRefs(ctx context.Context, batchSize int) ([]FileRef, error) {
	return r.scanFileRefs(ctx, fileRefsPrefix, batchSize)
}

func (r *Registry) scanFileRefs(ctx context.Context, prefix string, batchSize int) ([]FileRef, error) {
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
		ref.SizeBytes, err = strconv.ParseUint(string(entry.Value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode xet file ref %s: %w", key, err)
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

const fileRefsPrefix = "xet/file_refs/"

// repoFileRefPrefix ends in "/" so that repository "o/a" does not match "o/ab".
func repoFileRefPrefix(repo string) string {
	return fileRefsPrefix + url.PathEscape(repo) + "/"
}

func fileRefKey(repo, fileHash string) []byte {
	return []byte(repoFileRefPrefix(repo) + fileHash)
}

func parseFileRefKey(key string) (FileRef, bool) {
	rest, ok := strings.CutPrefix(key, fileRefsPrefix)
	if !ok {
		return FileRef{}, false
	}
	escapedRepo, fileHash, ok := strings.Cut(rest, "/")
	if !ok || fileHash == "" || escapedRepo == "" {
		return FileRef{}, false
	}
	repo, err := url.PathUnescape(escapedRepo)
	if err != nil {
		return FileRef{}, false
	}
	return FileRef{FileHash: fileHash, Repo: repo}, true
}
