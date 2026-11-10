package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"

	"lukechampine.com/blake3"
)

// globalDedupChunkModulus is hf_xet's MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS.
const globalDedupChunkModulus = 1024

const dedupChunkPrefix = "xet/chunk/"

// GlobalDedupEligible reports whether hf_xet may query the chunk lookup for
// this chunk: word 3 of the hash (its last 16 MDB hex digits) is a multiple of
// 1024. The client also queries the first chunk of every file.
func GlobalDedupEligible(chunkHash string) bool {
	if len(chunkHash) != 64 {
		return false
	}
	word, err := strconv.ParseUint(chunkHash[48:], 16, 64)
	return err == nil && word%globalDedupChunkModulus == 0
}

// IndexDedupChunks records that fileHash holds each chunk, so a chunk lookup in
// the same scope finds the file. The newest file wins.
func (r *Registry) IndexDedupChunks(ctx context.Context, scope, fileHash string, chunkHashes []string) error {
	for _, chunk := range chunkHashes {
		if err := r.store.Set(ctx, []byte(Partition), dedupChunkKey(scope, chunk), []byte(fileHash)); err != nil {
			return err
		}
	}
	return nil
}

// DedupFileForChunk returns the file a chunk lookup in scope should answer
// from, or kv.ErrNotFound.
func (r *Registry) DedupFileForChunk(ctx context.Context, scope, chunkHash string) (string, error) {
	res, err := r.store.Get(ctx, []byte(Partition), dedupChunkKey(scope, chunkHash))
	if err != nil {
		return "", err
	}
	return string(res.Value), nil
}

// ponytail: index keys of files xet GC deleted are never removed; lookups treat
// them as misses. One key per ~64 MiB uploaded; add a GC step if that ever matters.
func dedupChunkKey(scope, chunkHash string) []byte {
	return []byte(dedupChunkPrefix + url.PathEscape(scope) + "/" + chunkHash)
}

// DedupXorb is a xorb returned by a chunk lookup: its chunks in order, with
// their unpacked sizes, and its serialized size.
type DedupXorb struct {
	Hash        string
	Chunks      []ShardChunkInfo
	BytesOnDisk uint32
}

// WriteDedupShard builds the shard a chunk lookup returns: a complete MDB shard
// with no file records, the given xorbs, and chunk hashes keyed with hmacKey so
// the caller can only recognise chunks it already has. The client ignores it
// after validFor.
func WriteDedupShard(xorbs []DedupXorb, hmacKey [32]byte, now time.Time, validFor time.Duration) ([]byte, error) {
	sorted := append([]DedupXorb(nil), xorbs...)
	sort.Slice(sorted, func(i, j int) bool { return mdbHashLess(sorted[i].Hash, sorted[j].Hash) })

	type lookup struct {
		key  uint64
		a, b uint32
	}
	var b bytes.Buffer
	b.Write(mdbShardHeaderTag[:])
	writeU64(&b, mdbShardHeaderVersion)
	writeU64(&b, mdbShardFooterSize)

	fileInfoOffset := uint64(b.Len())
	writeBookend(&b)

	xorbInfoOffset := uint64(b.Len())
	var xorbLookup, chunkLookup []lookup
	var entry uint32
	var storedOnDisk, stored uint64
	for _, x := range sorted {
		raw, err := mdbHexToRaw(x.Hash)
		if err != nil {
			return nil, fmt.Errorf("xorb %s: %w", x.Hash, err)
		}
		var unpacked uint64
		for _, c := range x.Chunks {
			unpacked += c.SizeBytes
		}
		if unpacked > uint64(^uint32(0)) {
			return nil, fmt.Errorf("xorb %s is too large", x.Hash)
		}
		xorbLookup = append(xorbLookup, lookup{key: binary.LittleEndian.Uint64(raw[:8]), a: entry})
		b.Write(raw)
		writeU32(&b, 0) // xorb flags
		writeU32(&b, uint32(len(x.Chunks)))
		writeU32(&b, uint32(unpacked))
		writeU32(&b, x.BytesOnDisk)

		var start uint32
		for i, c := range x.Chunks {
			chunkRaw, err := mdbHexToRaw(c.Hash)
			if err != nil {
				return nil, fmt.Errorf("chunk %s: %w", c.Hash, err)
			}
			hasher := blake3.New(32, hmacKey[:])
			_, _ = hasher.Write(chunkRaw)
			keyedRaw := hasher.Sum(nil)
			chunkLookup = append(chunkLookup, lookup{key: binary.LittleEndian.Uint64(keyedRaw[:8]), a: entry, b: uint32(i)})
			b.Write(keyedRaw)
			writeU32(&b, start)
			writeU32(&b, uint32(c.SizeBytes))
			writeU32(&b, 0) // chunk flags
			writeU32(&b, 0)
			start += uint32(c.SizeBytes)
		}
		entry += 1 + uint32(len(x.Chunks))
		storedOnDisk += uint64(x.BytesOnDisk)
		stored += unpacked
	}
	writeBookend(&b)

	fileLookupOffset := uint64(b.Len())
	xorbLookupOffset := fileLookupOffset
	for _, l := range xorbLookup {
		writeU64(&b, l.key)
		writeU32(&b, l.a)
	}
	chunkLookupOffset := uint64(b.Len())
	sort.SliceStable(chunkLookup, func(i, j int) bool { return chunkLookup[i].key < chunkLookup[j].key })
	for _, l := range chunkLookup {
		writeU64(&b, l.key)
		writeU32(&b, l.a)
		writeU32(&b, l.b)
	}

	footerOffset := uint64(b.Len())
	for _, v := range []uint64{
		1, // footer version
		fileInfoOffset, xorbInfoOffset,
		fileLookupOffset, 0,
		xorbLookupOffset, uint64(len(xorbLookup)),
		chunkLookupOffset, uint64(len(chunkLookup)),
	} {
		writeU64(&b, v)
	}
	b.Write(hmacKey[:])
	writeU64(&b, uint64(now.Unix()))
	writeU64(&b, uint64(now.Add(validFor).Unix()))
	b.Write(make([]byte, 6*8))
	writeU64(&b, storedOnDisk)
	writeU64(&b, 0) // materialized bytes: no file records
	writeU64(&b, stored)
	writeU64(&b, footerOffset)
	return b.Bytes(), nil
}

// mdbHashLess orders hashes as xet-core does (by their four words). The MDB hex
// form prints each word as fixed-width big-endian hex, so string order matches.
func mdbHashLess(a, b string) bool {
	return a < b
}

func writeBookend(b *bytes.Buffer) {
	b.Write(bytes.Repeat([]byte{0xff}, 32))
	b.Write(make([]byte, 16))
}

func writeU32(b *bytes.Buffer, v uint32) {
	_ = binary.Write(b, binary.LittleEndian, v)
}

func writeU64(b *bytes.Buffer, v uint64) {
	_ = binary.Write(b, binary.LittleEndian, v)
}
