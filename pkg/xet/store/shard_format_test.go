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
