package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseShardFilesExtractsSegments(t *testing.T) {
	xorbHash := "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
	chunkA := "aaaaaaaaaaaaaaaa000000000000000000000000000000000000000000000000"
	chunkB := "bbbbbbbbbbbbbbbb000000000000000000000000000000000000000000000000"
	fileHash := "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210"

	files, err := ParseShardFiles(testBinaryShard(t, fileHash, xorbHash, chunkA, chunkB))
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
	}}, files)
}

func TestParseShardFilesReadsHFShardWithVerificationEntries(t *testing.T) {
	files, err := ParseShardFiles(testFooterlessHFShard(t))

	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, uint64(98308), files[0].SizeBytes)
}

func TestParseShardFilesRejectsEntryCountBeyondData(t *testing.T) {
	var b bytes.Buffer
	b.Write(mdbShardHeaderTag[:])
	writeU64(&b, mdbShardHeaderVersion)
	writeU64(&b, 0)
	writeHash(t, &b, "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210")
	writeU32(&b, 0)
	writeU32(&b, ^uint32(0))
	writeU64(&b, 0)

	_, err := ParseShardFiles(b.Bytes())

	require.ErrorContains(t, err, "read shard file xorb hash")
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
