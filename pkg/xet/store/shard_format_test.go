package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseShardInfoExtractsFilesXorbsAndChunks(t *testing.T) {
	xorbHash := "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
	chunkA := "aaaaaaaaaaaaaaaa000000000000000000000000000000000000000000000000"
	chunkB := "bbbbbbbbbbbbbbbb000000000000000000000000000000000000000000000000"
	fileHash, err := ComputeFileMerkleHash([]ShardChunkInfo{
		{Hash: chunkA, SizeBytes: 10},
		{Hash: chunkB, SizeBytes: 20},
	})
	require.NoError(t, err)

	info, err := ParseShardInfo(testBinaryShard(t, fileHash, xorbHash, chunkA, chunkB))
	require.NoError(t, err)

	require.Equal(t, []ShardFileInfo{{
		FileHash:  fileHash,
		SizeBytes: 30,
		Segments: []ShardFileSegment{{
			XorbHash:        xorbHash,
			SizeBytes:       30,
			ChunkIndexStart: 0,
			ChunkIndexEnd:   2,
		}},
	}}, info.Files)
	require.Equal(t, []string{xorbHash}, info.XorbHashes)
	require.Equal(t, []ShardXorbInfo{{
		Hash: xorbHash,
		Chunks: []ShardChunkInfo{
			{Hash: chunkA, SizeBytes: 10},
			{Hash: chunkB, SizeBytes: 20},
		},
	}}, info.Xorbs)
	require.Equal(t, []string{chunkA, chunkB}, info.ChunkHashes)
}

func TestParseShardInfoRejectsMismatchedFileHash(t *testing.T) {
	fileHash := "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210"
	xorbHash := "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
	chunkA := "aaaaaaaaaaaaaaaa000000000000000000000000000000000000000000000000"
	chunkB := "bbbbbbbbbbbbbbbb000000000000000000000000000000000000000000000000"

	_, err := ParseShardInfo(testBinaryShard(t, fileHash, xorbHash, chunkA, chunkB))
	require.Error(t, err)
	require.Contains(t, err.Error(), "file hash mismatch")
}

func TestParseShardInfoAcceptsFooterlessHFShard(t *testing.T) {
	raw := testFooterlessHFShard(t)

	info, err := ParseShardInfo(raw)

	require.NoError(t, err)
	require.Len(t, info.Files, 1)
	require.Len(t, info.Xorbs, 1)
	require.Len(t, info.ChunkHashes, 1)
	require.Equal(t, uint64(98308), info.Summary.SizeBytes)
	require.Equal(t, 1, info.Summary.NumXorbs)
	require.Equal(t, 1, info.Summary.NumChunks)
}

func TestCanonicalShardAppendsFooterToFooterlessHFShard(t *testing.T) {
	raw := testFooterlessHFShard(t)
	require.Equal(t, uint64(0), binary.LittleEndian.Uint64(raw[40:48]))

	canonical, info, err := CanonicalShard(raw)

	require.NoError(t, err)
	require.Len(t, canonical, len(raw)+int(mdbShardFooterSize))
	require.Equal(t, uint64(mdbShardFooterSize), binary.LittleEndian.Uint64(canonical[40:48]))
	require.Equal(t, uint64(98308), info.Summary.SizeBytes)
	parsed, err := ParseShardInfo(canonical)
	require.NoError(t, err)
	require.Equal(t, info.Summary, parsed.Summary)
	require.Equal(t, info.Files, parsed.Files)
}

func testFooterlessHFShard(t *testing.T) []byte {
	t.Helper()
	raw, err := hex.DecodeString("48465265706f4d6574614461746100556967456a7b815783a5bdd95ccdd14aa902000000000000000000000000000000f48967c6e58211baf99bfe05d3c0cd5b5d0b1c778ceff407ad8180a90f3debe5000000c00100000000000000000000008ff201a43c17cc7e887f4e5353d7526245381eaf3586a4211745ce927ece4e49000000000480010000000000010000007462ee32ff53d2416a470825aa6c9a916428d3786356a7d1f056176ff22eaf2500000000000000000000000000000000aed885ef055c83e5123e720d9352487f4e97cf9cba7db9e9b9ded40d2f410d4000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000000000000008ff201a43c17cc7e887f4e5353d7526245381eaf3586a4211745ce927ece4e49000000000100000004800100000000008ff201a43c17cc7e887f4e5353d7526245381eaf3586a4211745ce927ece4e4900000000048001000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000000000000000000000000000")
	require.NoError(t, err)
	return raw
}

func testBinaryShard(t *testing.T, fileHash, xorbHash, chunkA, chunkB string) []byte {
	t.Helper()
	var b bytes.Buffer

	b.Write(mdbShardHeaderTag[:])
	writeU64(&b, 2)
	writeU64(&b, 200)

	writeHash(t, &b, fileHash)
	writeU32(&b, 0)
	writeU32(&b, 1)
	writeU64(&b, 0)

	writeHash(t, &b, xorbHash)
	writeU32(&b, 0)
	writeU32(&b, 30)
	writeU32(&b, 0)
	writeU32(&b, 2)

	writeBookendHash(&b)
	writeU32(&b, 0)
	writeU32(&b, 0)
	writeU64(&b, 0)

	writeHash(t, &b, xorbHash)
	writeU32(&b, 0)
	writeU32(&b, 2)
	writeU32(&b, 30)
	writeU32(&b, 24)

	writeHash(t, &b, chunkA)
	writeU32(&b, 0)
	writeU32(&b, 10)
	writeU32(&b, 0)
	writeU32(&b, 0)

	writeHash(t, &b, chunkB)
	writeU32(&b, 10)
	writeU32(&b, 20)
	writeU32(&b, 0)
	writeU32(&b, 0)

	writeBookendHash(&b)
	writeU32(&b, 0)
	writeU32(&b, 0)
	writeU32(&b, 0)
	writeU32(&b, 0)

	footerOffset := uint64(b.Len())
	writeU64(&b, 1)
	writeU64(&b, 48)
	writeU64(&b, 192)
	writeU64(&b, footerOffset)
	writeU64(&b, 0)
	writeU64(&b, footerOffset)
	writeU64(&b, 0)
	writeU64(&b, footerOffset)
	writeU64(&b, 0)
	b.Write(make([]byte, 32))
	writeU64(&b, 0)
	writeU64(&b, ^uint64(0))
	for i := 0; i < 6; i++ {
		writeU64(&b, 0)
	}
	writeU64(&b, 24)
	writeU64(&b, 30)
	writeU64(&b, 30)
	writeU64(&b, footerOffset)

	return b.Bytes()
}

func writeHash(t *testing.T, b *bytes.Buffer, value string) {
	t.Helper()
	raw, err := hex.DecodeString(value)
	require.NoError(t, err)
	require.Len(t, raw, 32)
	for i := 0; i < 4; i++ {
		for j := 7; j >= 0; j-- {
			b.WriteByte(raw[i*8+j])
		}
	}
}

func writeBookendHash(b *bytes.Buffer) {
	b.Write(bytes.Repeat([]byte{0xff}, 32))
}

func writeU32(b *bytes.Buffer, value uint32) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func writeU64(b *bytes.Buffer, value uint64) {
	_ = binary.Write(b, binary.LittleEndian, value)
}
