package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

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
	if file.RegisteredAt == 0 {
		file.RegisteredAt = time.Now().Unix()
	}
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

func (r *Registry) ListFiles(ctx context.Context, batchSize int) ([]ShardFileInfo, error) {
	var files []ShardFileInfo
	err := r.scanPrefix(ctx, "xet/file/", batchSize, func(hash string, value []byte) error {
		var file ShardFileInfo
		if err := json.Unmarshal(value, &file); err != nil {
			return fmt.Errorf("decode xet file %s: %w", hash, err)
		}
		files = append(files, file)
		return nil
	})
	return files, err
}

func (r *Registry) ListXorbFooterHashes(ctx context.Context, batchSize int) ([]string, error) {
	var hashes []string
	err := r.scanPrefix(ctx, "xet/xorb/", batchSize, func(hash string, _ []byte) error {
		hashes = append(hashes, hash)
		return nil
	})
	return hashes, err
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

// AddFileRef records that a repository holds a file, only if it does not
// already. It reports whether the ref was created.
func (r *Registry) AddFileRef(ctx context.Context, ref FileRef) (bool, error) {
	err := r.store.SetIf(ctx, []byte(Partition), fileRefKey(ref.Repo, ref.FileHash), []byte(strconv.FormatUint(ref.SizeBytes, 10)), nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// AddUploadClaim records that a verified shard registered the file for repo:
// the uploader proved it has the file's chunk hashes, so repo may link it.
func (r *Registry) AddUploadClaim(ctx context.Context, repo, fileHash string) error {
	return r.store.Set(ctx, []byte(Partition), uploadClaimKey(repo, fileHash), []byte{1})
}

func (r *Registry) HasUploadClaim(ctx context.Context, repo, fileHash string) (bool, error) {
	_, err := r.store.Get(ctx, []byte(Partition), uploadClaimKey(repo, fileHash))
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// DeleteRepoFileRefs drops every file ref and upload claim of a deleted repository.
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
	var claims []string
	if err := r.scanPrefix(ctx, repoUploadClaimPrefix(repo), 0, func(fileHash string, _ []byte) error {
		claims = append(claims, fileHash)
		return nil
	}); err != nil {
		return err
	}
	for _, fileHash := range claims {
		if err := r.store.Delete(ctx, []byte(Partition), uploadClaimKey(repo, fileHash)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) DeleteFileRef(ctx context.Context, ref FileRef) error {
	return r.store.Delete(ctx, []byte(Partition), fileRefKey(ref.Repo, ref.FileHash))
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

// scanPrefix calls fn with the rest of the key and the value of every key under prefix.
func (r *Registry) scanPrefix(ctx context.Context, prefix string, batchSize int, fn func(rest string, value []byte) error) error {
	iter, err := r.store.Scan(ctx, []byte(Partition), kv.ScanOptions{
		KeyStart:  []byte(prefix),
		BatchSize: batchSize,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		entry := iter.Entry()
		rest, ok := strings.CutPrefix(string(entry.Key), prefix)
		if !ok {
			break
		}
		if rest == "" {
			continue
		}
		if err := fn(rest, entry.Value); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil && !errors.Is(err, kv.ErrClosedEntries) {
		return err
	}
	return nil
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

func repoUploadClaimPrefix(repo string) string {
	return "xet/claims/" + url.PathEscape(repo) + "/"
}

func uploadClaimKey(repo, fileHash string) []byte {
	return []byte(repoUploadClaimPrefix(repo) + fileHash)
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
