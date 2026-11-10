package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func TestGlobalDedupEligibleMatchesTheClientRule(t *testing.T) {
	// hf_xet: word[3] % 1024 == 0, word 3 being the last 16 hex digits of the MDB hex form.
	prefix := "0123456789abcdef0123456789abcdef0123456789abcdef"
	require.True(t, GlobalDedupEligible(prefix+"0000000000000400"))
	require.True(t, GlobalDedupEligible(prefix+"ffffffffffff0000"))
	require.False(t, GlobalDedupEligible(prefix+"0000000000000401"))
	require.False(t, GlobalDedupEligible(prefix+"0000000000000200"))
	require.False(t, GlobalDedupEligible("not-a-hash"))
	require.False(t, GlobalDedupEligible(prefix+"zzzzzzzzzzzz0000"))
}

func TestDedupIndexIsKeyedByScope(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	chunk := ComputeDataHash([]byte("chunk"))

	require.NoError(t, registry.IndexDedupChunks(ctx, "owner-a", "file-1", []string{chunk}))

	file, err := registry.DedupFileForChunk(ctx, "owner-a", chunk)
	require.NoError(t, err)
	require.Equal(t, "file-1", file)
	_, err = registry.DedupFileForChunk(ctx, "owner-b", chunk)
	require.ErrorIs(t, err, kv.ErrNotFound)

	require.NoError(t, registry.IndexDedupChunks(ctx, "owner-a", "file-2", []string{chunk}))
	file, err = registry.DedupFileForChunk(ctx, "owner-a", chunk)
	require.NoError(t, err)
	require.Equal(t, "file-2", file, "the newest file wins")
}

func TestDedupIndexKeysDoNotOverlapFileOrXorbScans(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(kvtest.GetStore(ctx, t))
	require.NoError(t, registry.IndexDedupChunks(ctx, "", "file-1", []string{ComputeDataHash([]byte("c"))}))

	files, err := registry.ListFiles(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, files)
	footers, err := registry.ListXorbFooterHashes(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, footers)
}

func TestWriteDedupShardIsAValidKeyedShard(t *testing.T) {
	var key [32]byte
	key[0], key[31] = 7, 9
	chunks := func(names ...string) []ShardChunkInfo {
		var out []ShardChunkInfo
		for i, n := range names {
			out = append(out, ShardChunkInfo{Hash: ComputeDataHash([]byte(n)), SizeBytes: uint64(100 + i)})
		}
		return out
	}
	xorbA := DedupXorb{Hash: ComputeDataHash([]byte("xorb-a")), Chunks: chunks("a1", "a2", "a3"), BytesOnDisk: 250}
	xorbB := DedupXorb{Hash: ComputeDataHash([]byte("xorb-b")), Chunks: chunks("b1"), BytesOnDisk: 90}
	now := time.Unix(1_800_000_000, 0)

	shard, err := WriteDedupShard([]DedupXorb{xorbA, xorbB}, key, now, 24*time.Hour)
	require.NoError(t, err)

	// The shard carries no file records.
	files, err := ParseShardFiles(shard)
	require.NoError(t, err)
	require.Empty(t, files)

	parsed := parseTestShard(t, shard)
	require.Equal(t, key, parsed.hmacKey)
	require.Equal(t, uint64(now.Unix()), parsed.created)
	require.Equal(t, uint64(now.Add(24*time.Hour).Unix()), parsed.expiry)
	require.Equal(t, uint64(340), parsed.storedBytesOnDisk)
	require.Equal(t, uint64(100+101+102+100), parsed.storedBytes)
	require.Zero(t, parsed.fileLookupEntries)

	// Xorbs are sorted by hash and keep their chunk layout, with keyed chunk hashes.
	want := []DedupXorb{xorbA, xorbB}
	sort.Slice(want, func(i, j int) bool { return mdbHashLess(want[i].Hash, want[j].Hash) })
	require.Len(t, parsed.xorbs, 2)
	for i, x := range parsed.xorbs {
		require.Equal(t, want[i].Hash, x.hash)
		require.Equal(t, want[i].BytesOnDisk, x.bytesOnDisk)
		var offset, total uint32
		for j, c := range x.chunks {
			require.Equal(t, keyed(t, key, want[i].Chunks[j].Hash), c.hash)
			require.Equal(t, offset, c.start)
			require.Equal(t, uint32(want[i].Chunks[j].SizeBytes), c.size)
			offset += c.size
			total += c.size
		}
		require.Equal(t, total, x.bytesInXorb)
	}

	// Lookup tables point back at the right entries and are sorted for the client's search.
	require.Len(t, parsed.xorbLookup, 2)
	for i, entry := range parsed.xorbLookup {
		require.Equal(t, truncated(t, parsed.xorbs[i].hash), entry.key)
		require.Equal(t, parsed.xorbs[i].entryIndex, entry.a)
	}
	require.Len(t, parsed.chunkLookup, 4)
	require.True(t, sort.SliceIsSorted(parsed.chunkLookup, func(i, j int) bool { return parsed.chunkLookup[i].key < parsed.chunkLookup[j].key }))
	for _, entry := range parsed.chunkLookup {
		x := parsed.xorbByEntry[entry.a]
		require.NotNil(t, x)
		require.Equal(t, truncated(t, x.chunks[entry.b].hash), entry.key)
	}
}

func keyed(t *testing.T, key [32]byte, hash string) string {
	t.Helper()
	raw, err := mdbHexToRaw(hash)
	require.NoError(t, err)
	h := blake3.New(32, key[:])
	_, _ = h.Write(raw)
	return mdbHexFromRaw(h.Sum(nil))
}

func truncated(t *testing.T, hash string) uint64 {
	t.Helper()
	raw, err := mdbHexToRaw(hash)
	require.NoError(t, err)
	return binary.LittleEndian.Uint64(raw[:8])
}

type testShardChunk struct {
	hash        string
	start, size uint32
}

type testShardXorb struct {
	hash                     string
	entryIndex               uint32
	bytesInXorb, bytesOnDisk uint32
	chunks                   []testShardChunk
}

type testLookup struct {
	key  uint64
	a, b uint32
}

type testShard struct {
	xorbs                          []*testShardXorb
	xorbByEntry                    map[uint32]*testShardXorb
	xorbLookup, chunkLookup        []testLookup
	fileLookupEntries              uint64
	hmacKey                        [32]byte
	created, expiry                uint64
	storedBytesOnDisk, storedBytes uint64
}

// parseTestShard reads a shard the way xet-core's MDBShardInfo does: header,
// footer from the end, then each section at the offset the footer names.
func parseTestShard(t *testing.T, shard []byte) testShard {
	t.Helper()
	require.GreaterOrEqual(t, len(shard), 48+int(mdbShardFooterSize))
	require.Equal(t, mdbShardHeaderTag[:], shard[:32])
	require.Equal(t, mdbShardHeaderVersion, binary.LittleEndian.Uint64(shard[32:40]))
	require.Equal(t, mdbShardFooterSize, binary.LittleEndian.Uint64(shard[40:48]))

	footer := shard[len(shard)-int(mdbShardFooterSize):]
	u64 := func(i int) uint64 { return binary.LittleEndian.Uint64(footer[i*8 : i*8+8]) }
	require.Equal(t, uint64(1), u64(0), "footer version")
	fileInfo, xorbInfo := u64(1), u64(2)
	fileLookup, fileLookupN := u64(3), u64(4)
	xorbLookup, xorbLookupN := u64(5), u64(6)
	chunkLookup, chunkLookupN := u64(7), u64(8)
	var parsed testShard
	copy(parsed.hmacKey[:], footer[72:104])
	parsed.created = binary.LittleEndian.Uint64(footer[104:112])
	parsed.expiry = binary.LittleEndian.Uint64(footer[112:120])
	parsed.storedBytesOnDisk = binary.LittleEndian.Uint64(footer[168:176])
	parsed.storedBytes = binary.LittleEndian.Uint64(footer[184:192])
	footerOffset := binary.LittleEndian.Uint64(footer[192:200])
	require.Equal(t, uint64(len(shard))-mdbShardFooterSize, footerOffset)
	parsed.fileLookupEntries = fileLookupN

	require.Equal(t, uint64(48), fileInfo)
	require.Equal(t, bytes.Repeat([]byte{0xff}, 32), shard[fileInfo:fileInfo+32], "file section is only a bookend")
	require.Equal(t, fileInfo+48, xorbInfo)

	parsed.xorbByEntry = map[uint32]*testShardXorb{}
	pos, entry := xorbInfo, uint32(0)
	for {
		header := shard[pos : pos+48]
		if bytes.Equal(header[:32], bytes.Repeat([]byte{0xff}, 32)) {
			pos += 48
			break
		}
		x := &testShardXorb{
			hash:        mdbHexFromRaw(header[:32]),
			entryIndex:  entry,
			bytesInXorb: binary.LittleEndian.Uint32(header[40:44]),
			bytesOnDisk: binary.LittleEndian.Uint32(header[44:48]),
		}
		require.Zero(t, binary.LittleEndian.Uint32(header[32:36]), "xorb flags")
		n := binary.LittleEndian.Uint32(header[36:40])
		pos += 48
		for i := uint32(0); i < n; i++ {
			c := shard[pos : pos+48]
			x.chunks = append(x.chunks, testShardChunk{
				hash:  mdbHexFromRaw(c[:32]),
				start: binary.LittleEndian.Uint32(c[32:36]),
				size:  binary.LittleEndian.Uint32(c[36:40]),
			})
			pos += 48
		}
		parsed.xorbs = append(parsed.xorbs, x)
		parsed.xorbByEntry[entry] = x
		entry += 1 + n
	}
	require.Equal(t, pos, fileLookup)
	require.Equal(t, fileLookup+12*fileLookupN, xorbLookup)
	for i := uint64(0); i < xorbLookupN; i++ {
		p := xorbLookup + 12*i
		parsed.xorbLookup = append(parsed.xorbLookup, testLookup{key: binary.LittleEndian.Uint64(shard[p:]), a: binary.LittleEndian.Uint32(shard[p+8:])})
	}
	require.Equal(t, xorbLookup+12*xorbLookupN, chunkLookup)
	for i := uint64(0); i < chunkLookupN; i++ {
		p := chunkLookup + 16*i
		parsed.chunkLookup = append(parsed.chunkLookup, testLookup{
			key: binary.LittleEndian.Uint64(shard[p:]),
			a:   binary.LittleEndian.Uint32(shard[p+8:]),
			b:   binary.LittleEndian.Uint32(shard[p+12:]),
		})
	}
	require.Equal(t, chunkLookup+16*chunkLookupN, footerOffset)
	return parsed
}
