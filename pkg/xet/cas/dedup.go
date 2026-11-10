package cas

import (
	"context"
	"crypto/rand"
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
	// maxDedupXorbs bounds a chunk lookup answer to ~2 GiB of data (~1.5 MiB of shard).
	maxDedupXorbs = 32
	// dedupShardValidFor must stay well under xet GC's xorb grace period, so a
	// client never dedups against a xorb GC may already delete.
	dedupShardValidFor = 24 * time.Hour
)

// WithGlobalDedup lets a chunk lookup find chunks uploaded to any owner's
// repositories, as Hugging Face does. Without it a lookup only finds chunks of
// the owner named in the upload token.
func WithGlobalDedup() HandlerOption {
	return func(h *Handler) {
		h.globalDedup = true
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
// and the eligible chunks of its xorbs) for verified, registered files. It is
// best effort: a failure only costs dedup, never the upload.
func (h *Handler) indexDedupChunks(r *http.Request, files []xetstore.ShardFileInfo, chunks map[string][]xetstore.ShardChunkInfo) {
	scope := h.dedupScope(r)
	for _, file := range files {
		var hashes []string
		seen := make(map[string]struct{})
		for i, segment := range file.Segments {
			for j, chunk := range chunks[segment.XorbHash] {
				first := i == 0 && uint32(j) == segment.ChunkIndexStart
				if _, dup := seen[chunk.Hash]; dup || !(first || xetstore.GlobalDedupEligible(chunk.Hash)) {
					continue
				}
				seen[chunk.Hash] = struct{}{}
				hashes = append(hashes, chunk.Hash)
			}
		}
		if err := h.registry.IndexDedupChunks(r.Context(), scope, file.FileHash, hashes); err != nil {
			logging.FromContext(r.Context()).WithError(err).WithField("file_hash", file.FileHash).Warn("Failed to index xet chunks for dedup")
		}
	}
}

// getDedupShard answers hf_xet's chunk lookup with a shard describing the live
// xorbs of the newest file holding the chunk, from the one holding it onwards.
func (h *Handler) getDedupShard(w http.ResponseWriter, r *http.Request) {
	chunkHash := chi.URLParam(r, "hash")
	if !isXETHash(chunkHash) {
		http.Error(w, "invalid chunk hash", http.StatusBadRequest)
		return
	}
	xorbs, err := h.dedupXorbs(r.Context(), h.dedupScope(r), chunkHash)
	if errors.Is(err, kv.ErrNotFound) {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	shard, err := xetstore.WriteDedupShard(xorbs, key, time.Now(), dedupShardValidFor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(shard)
}

// dedupXorbs returns up to maxDedupXorbs live xorbs of the indexed file, in file
// order, starting at the first one holding the chunk. Parked file records and
// footers are skipped, never restored: GC already considers them unused.
func (h *Handler) dedupXorbs(ctx context.Context, scope, chunkHash string) ([]xetstore.DedupXorb, error) {
	fileHash, err := h.registry.DedupFileForChunk(ctx, scope, chunkHash)
	if err != nil {
		return nil, err
	}
	file, err := h.registry.GetFile(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	var out []xetstore.DedupXorb
	seen := make(map[string]struct{})
	for _, segment := range file.Segments {
		if len(out) == maxDedupXorbs {
			break
		}
		if _, dup := seen[segment.XorbHash]; dup {
			continue
		}
		seen[segment.XorbHash] = struct{}{}
		footer, err := h.registry.GetXorbInfo(ctx, segment.XorbHash)
		if errors.Is(err, kv.ErrNotFound) {
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
		if len(out) == 0 && !holdsChunk(chunks, chunkHash) {
			continue
		}
		// A one-chunk xorb's hash is its chunk's hash: offering it would hand out
		// a real chunk hash the caller may not have. The first xorb holds the
		// queried chunk, which the caller has.
		if len(out) > 0 && len(chunks) < 2 {
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
