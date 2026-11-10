package cas

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

const (
	// maxDedupXorbs bounds a chunk lookup answer to ~2 GiB of data: usually
	// ~1.5 MiB of shard, at most ~17 MiB when every chunk is minimum size.
	maxDedupXorbs = 32
	// defaultDedupShardValidFor is how long the client may use an answer.
	defaultDedupShardValidFor = 24 * time.Hour
)

// DedupShardValidFor is how long a chunk lookup answer may be used for a given
// xet GC xorb grace period: a quarter of it, at most a day, so a client never
// dedups against a xorb GC may already delete. Zero (lookups off) below an hour.
func DedupShardValidFor(xorbMinAge time.Duration) time.Duration {
	validFor := min(xorbMinAge/4, defaultDedupShardValidFor)
	if validFor < time.Hour {
		return 0
	}
	return validFor
}

// WithGlobalDedup lets a chunk lookup find chunks uploaded to any owner's
// repositories, as Hugging Face does. Without it a lookup only finds chunks of
// the owner named in the upload token.
func WithGlobalDedup() HandlerOption {
	return func(h *Handler) {
		h.globalDedup = true
	}
}

// WithDedupShardValidFor sets how long the client may use a chunk lookup
// answer; zero turns lookups off. See DedupShardValidFor.
func WithDedupShardValidFor(validFor time.Duration) HandlerOption {
	return func(h *Handler) {
		h.dedupValidFor = validFor
	}
}

func (h *Handler) dedupScope(r *http.Request) string {
	if h.globalDedup {
		return ""
	}
	grant, _ := r.Context().Value(tokenGrantKey{}).(TokenGrant)
	owner, _, _ := strings.Cut(grant.Repo, "/")
	return owner
}

// indexDedupChunks records the chunks hf_xet looks up (each file's first chunk
// and the eligible chunks of its xorbs) for verified, registered files. A file
// that was already registered is indexed from its stored record: the first
// upload's segments are what lookups answer from, and the re-upload's own xorbs
// are used by no record. It is best effort: a failure only costs dedup, never
// the upload.
func (h *Handler) indexDedupChunks(r *http.Request, files []xetstore.ShardFileInfo, inserted []bool, chunks map[string][]xetstore.ShardChunkInfo) {
	ctx := r.Context()
	log := logging.FromContext(ctx)
	scope := h.dedupScope(r)
	for i, file := range files {
		if !inserted[i] {
			stored, err := h.registry.GetFile(ctx, file.FileHash)
			if err != nil {
				log.WithError(err).WithField("file_hash", file.FileHash).Warn("Failed to read xet file record for dedup")
				continue
			}
			if err := h.loadChunks(ctx, stored.Segments, chunks); err != nil {
				log.WithError(err).WithField("file_hash", file.FileHash).Warn("Failed to read xet footers for dedup")
				continue
			}
			file = stored
		}
		if err := h.registry.IndexDedupChunks(ctx, scope, dedupEntries(file, chunks)); err != nil {
			log.WithError(err).WithField("file_hash", file.FileHash).Warn("Failed to index xet chunks for dedup")
		}
	}
}

// loadChunks adds the chunk lists of the segments' xorbs missing from chunks,
// from their live footers.
func (h *Handler) loadChunks(ctx context.Context, segments []xetstore.ShardFileSegment, chunks map[string][]xetstore.ShardChunkInfo) error {
	for _, segment := range segments {
		if _, ok := chunks[segment.XorbHash]; ok {
			continue
		}
		footer, err := h.registry.GetXorbInfo(ctx, segment.XorbHash)
		if err != nil {
			return fmt.Errorf("xorb %s: %w", segment.XorbHash, err)
		}
		info, _, err := parseXorbInfo(footer)
		if err != nil {
			return fmt.Errorf("xorb %s: %w", segment.XorbHash, err)
		}
		chunks[segment.XorbHash] = shardChunksFromParsedXorb(info)
	}
	return nil
}

func dedupEntries(file xetstore.ShardFileInfo, chunks map[string][]xetstore.ShardChunkInfo) map[string]xetstore.DedupEntry {
	entries := make(map[string]xetstore.DedupEntry)
	if len(file.Segments) == 0 {
		return entries
	}
	seen := make(map[string]struct{})
	for _, segment := range file.Segments {
		if _, dup := seen[segment.XorbHash]; dup {
			continue
		}
		seen[segment.XorbHash] = struct{}{}
		for _, chunk := range chunks[segment.XorbHash] {
			if _, done := entries[chunk.Hash]; !done && xetstore.GlobalDedupEligible(chunk.Hash) {
				entries[chunk.Hash] = xetstore.DedupEntry{FileHash: file.FileHash, XorbHash: segment.XorbHash}
			}
		}
	}
	first := file.Segments[0]
	if xorb := chunks[first.XorbHash]; int(first.ChunkIndexStart) < len(xorb) {
		entries[xorb[first.ChunkIndexStart].Hash] = xetstore.DedupEntry{FileHash: file.FileHash, XorbHash: first.XorbHash}
	}
	return entries
}

// getDedupShard answers hf_xet's chunk lookup with a shard describing the live
// xorbs of the file holding the chunk, from the xorb holding it onwards.
func (h *Handler) getDedupShard(w http.ResponseWriter, r *http.Request) {
	chunkHash := chi.URLParam(r, "hash")
	if !isXETHash(chunkHash) {
		http.Error(w, "invalid chunk hash", http.StatusBadRequest)
		return
	}
	if h.dedupValidFor <= 0 {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}
	scope := h.dedupScope(r)
	xorbs, err := h.dedupXorbs(r.Context(), scope, chunkHash)
	if errors.Is(err, kv.ErrNotFound) {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	now := time.Now()
	shard, err := xetstore.WriteDedupShard(xorbs, h.dedupKey(scope, now), now, h.dedupValidFor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(shard)
}

// dedupKey keys the chunk hashes of an answer. The key is public (it is in the
// shard footer); keyed hashes only keep the caller from learning hashes of
// chunks it lacks. One key per scope and day keeps the client, which holds one
// shard collection per key, from slowing down as answers accumulate.
func (h *Handler) dedupKey(scope string, now time.Time) [32]byte {
	mac := hmac.New(sha256.New, h.tokenSigningKey)
	_, _ = mac.Write([]byte("xet-dedup\x00" + scope + "\x00" + now.UTC().Format(time.DateOnly)))
	var key [32]byte
	copy(key[:], mac.Sum(nil))
	return key
}

// dedupXorbs returns up to maxDedupXorbs live xorbs of the indexed file, in file
// order, starting at the indexed xorb, which must hold the chunk. Parked file
// records and footers are skipped, never restored: GC already considers them
// unused.
func (h *Handler) dedupXorbs(ctx context.Context, scope, chunkHash string) ([]xetstore.DedupXorb, error) {
	entry, err := h.registry.DedupEntryForChunk(ctx, scope, chunkHash)
	if err != nil {
		return nil, err
	}
	file, err := h.registry.GetFile(ctx, entry.FileHash)
	if err != nil {
		return nil, err
	}
	var out []xetstore.DedupXorb
	seen := make(map[string]struct{})
	started := false
	for _, segment := range file.Segments {
		if len(out) == maxDedupXorbs {
			break
		}
		if !started && segment.XorbHash != entry.XorbHash {
			continue
		}
		if _, dup := seen[segment.XorbHash]; dup {
			continue
		}
		seen[segment.XorbHash] = struct{}{}
		footer, err := h.registry.GetXorbInfo(ctx, segment.XorbHash)
		if errors.Is(err, kv.ErrNotFound) {
			if !started {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		info, _, err := parseXorbInfo(footer)
		if err != nil {
			return nil, fmt.Errorf("xorb %s: %w", segment.XorbHash, err)
		}
		chunks := shardChunksFromParsedXorb(info)
		if !started {
			if !holdsChunk(chunks, chunkHash) {
				return nil, kv.ErrNotFound
			}
			started = true
		} else if len(chunks) < 2 {
			// A one-chunk xorb's hash is its chunk's hash: offering it would hand
			// out a real chunk hash the caller may not have. The first xorb holds
			// the queried chunk, which the caller has.
			continue
		}
		out = append(out, xetstore.DedupXorb{Hash: segment.XorbHash, Chunks: chunks, BytesOnDisk: bytesOnDisk(info)})
	}
	if len(out) == 0 {
		return nil, kv.ErrNotFound
	}
	return out, nil
}

func holdsChunk(chunks []xetstore.ShardChunkInfo, chunkHash string) bool {
	for _, c := range chunks {
		if c.Hash == chunkHash {
			return true
		}
	}
	return false
}

func bytesOnDisk(info parsedXorbInfo) uint32 {
	if len(info.ChunkBoundaries) == 0 {
		return 0
	}
	return info.ChunkBoundaries[len(info.ChunkBoundaries)-1]
}
