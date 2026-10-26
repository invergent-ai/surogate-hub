// Package xettest builds xet wire-format fixtures for tests.
package xettest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

// Compression schemes of a serialized xorb chunk.
const (
	SchemeNone   = 0
	SchemeLZ4    = 1
	SchemeBG4LZ4 = 2
)

// SerializedXorb returns the hash and serialization, footer included, of a xorb
// holding the given uncompressed chunks.
func SerializedXorb(t testing.TB, chunks ...[]byte) (string, []byte) {
	return SerializedXorbWithScheme(t, SchemeNone, chunks...)
}

func SerializedXorbWithScheme(t testing.TB, scheme byte, chunks ...[]byte) (string, []byte) {
	t.Helper()
	infos := make([]xetstore.ShardChunkInfo, 0, len(chunks))
	for _, chunk := range chunks {
		infos = append(infos, xetstore.ShardChunkInfo{Hash: xetstore.ComputeDataHash(chunk), SizeBytes: uint64(len(chunk))})
	}
	xorbHash, err := xetstore.ComputeXorbMerkleHash(infos)
	require.NoError(t, err)

	var b bytes.Buffer
	var boundaries, unpackedOffsets []uint32
	var unpacked uint32
	for _, chunk := range chunks {
		serializedChunk := chunk
		if scheme == SchemeBG4LZ4 {
			serializedChunk = bg4Split(chunk)
		}
		if scheme == SchemeLZ4 || scheme == SchemeBG4LZ4 {
			var compressed bytes.Buffer
			writer := lz4.NewWriter(&compressed)
			_, err := writer.Write(serializedChunk)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			serializedChunk = compressed.Bytes()
		}
		b.WriteByte(0)
		writeU24(&b, uint32(len(serializedChunk)))
		b.WriteByte(scheme)
		writeU24(&b, uint32(len(chunk)))
		b.Write(serializedChunk)
		unpacked += uint32(len(chunk))
		boundaries = append(boundaries, uint32(b.Len()))
		unpackedOffsets = append(unpackedOffsets, unpacked)
	}

	n := uint32(len(chunks))
	infoLength := 92 + 40*n
	b.WriteString("XETBLOB")
	b.WriteByte(1)
	WriteHash(t, &b, xorbHash)

	b.WriteString("XBLBHSH")
	b.WriteByte(0)
	writeU32(&b, n)
	for _, info := range infos {
		WriteHash(t, &b, info.Hash)
	}

	b.WriteString("XBLBBND")
	b.WriteByte(1)
	writeU32(&b, n)
	for _, boundary := range boundaries {
		writeU32(&b, boundary)
	}
	for _, offset := range unpackedOffsets {
		writeU32(&b, offset)
	}
	writeU32(&b, n)
	writeU32(&b, infoLength-40)
	writeU32(&b, infoLength-52-32*n)
	b.Write(make([]byte, 16))
	writeU32(&b, infoLength)

	return xorbHash, b.Bytes()
}

// StripFooter returns a xorb serialization without its footer, as hf_xet uploads it.
func StripFooter(t testing.TB, xorb []byte) []byte {
	t.Helper()
	return xorb[:footerStart(t, xorb)]
}

// FileHash returns the hash of a file made of the given chunks.
func FileHash(t testing.TB, chunks ...[]byte) string {
	t.Helper()
	infos := make([]xetstore.ShardChunkInfo, 0, len(chunks))
	for _, chunk := range chunks {
		infos = append(infos, xetstore.ShardChunkInfo{Hash: xetstore.ComputeDataHash(chunk), SizeBytes: uint64(len(chunk))})
	}
	fileHash, err := xetstore.ComputeFileMerkleHash(infos)
	require.NoError(t, err)
	return fileHash
}

// Shard returns a footerless shard with one file made of one segment, and no
// xorb section, like the shards hf_xet sends for files built from known xorbs.
func Shard(t testing.TB, fileHash, xorbHash string, chunkStart, chunkEnd, sizeBytes uint32) []byte {
	t.Helper()
	var b bytes.Buffer
	b.Write([]byte{'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0, 85,
		105, 103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169})
	writeU64(&b, 2)
	writeU64(&b, 0)

	WriteHash(t, &b, fileHash)
	writeU32(&b, 0)
	writeU32(&b, 1)
	writeU64(&b, 0)

	WriteHash(t, &b, xorbHash)
	writeU32(&b, 0)
	writeU32(&b, sizeBytes)
	writeU32(&b, chunkStart)
	writeU32(&b, chunkEnd)

	writeBookend(&b)
	writeBookend(&b)
	return b.Bytes()
}

// PutXorb stores a serialized xorb and its footer the way a verified upload does.
func PutXorb(t testing.TB, ctx context.Context, registry *xetstore.Registry, adapter block.Adapter, storageNamespace, xorbHash string, xorb []byte) {
	t.Helper()
	_, err := adapter.Put(ctx, xetstore.XorbObjectPointer(storageNamespace, "default", xorbHash), int64(len(xorb)), bytes.NewReader(xorb), block.PutOpts{})
	require.NoError(t, err)
	require.NoError(t, registry.PutXorbInfo(ctx, xorbHash, xorb[footerStart(t, xorb):]))
}

// SeedFile stores a one-chunk xorb and its file record the way a verified
// upload does, and returns the file and xorb hashes and the xorb bytes.
func SeedFile(t testing.TB, ctx context.Context, registry *xetstore.Registry, adapter block.Adapter, storageNamespace string, chunk []byte) (string, string, []byte) {
	t.Helper()
	xorbHash, xorb := SerializedXorb(t, chunk)
	PutXorb(t, ctx, registry, adapter, storageNamespace, xorbHash, xorb)
	fileHash := FileHash(t, chunk)
	_, err := registry.RegisterFile(ctx, xetstore.ShardFileInfo{
		FileHash:  fileHash,
		SizeBytes: uint64(len(chunk)),
		Segments: []xetstore.ShardFileSegment{{
			XorbHash:      xorbHash,
			SizeBytes:     uint64(len(chunk)),
			ChunkIndexEnd: 1,
		}},
	})
	require.NoError(t, err)
	return fileHash, xorbHash, xorb
}

// WriteHash writes a hash in the xet on-disk byte order.
func WriteHash(t testing.TB, b *bytes.Buffer, value string) {
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

func footerStart(t testing.TB, xorb []byte) int {
	t.Helper()
	require.GreaterOrEqual(t, len(xorb), 4)
	start := len(xorb) - 4 - int(binary.LittleEndian.Uint32(xorb[len(xorb)-4:]))
	require.GreaterOrEqual(t, start, 0)
	return start
}

func writeBookend(b *bytes.Buffer) {
	b.Write(bytes.Repeat([]byte{0xff}, 32))
	b.Write(make([]byte, 16))
}

func writeU24(b *bytes.Buffer, value uint32) {
	b.WriteByte(byte(value))
	b.WriteByte(byte(value >> 8))
	b.WriteByte(byte(value >> 16))
}

func writeU32(b *bytes.Buffer, value uint32) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func writeU64(b *bytes.Buffer, value uint64) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func bg4Split(data []byte) []byte {
	n := len(data)
	split := n / 4
	rem := n % 4
	grouped := make([]byte, n)
	g1 := split + min(1, rem)
	g2 := g1 + split + min(1, max(0, rem-1))
	g3 := g2 + split + min(1, max(0, rem-2))
	for i := 0; i < split; i++ {
		grouped[i] = data[4*i]
		grouped[g1+i] = data[4*i+1]
		grouped[g2+i] = data[4*i+2]
		grouped[g3+i] = data[4*i+3]
	}
	for i := 0; i < rem; i++ {
		grouped[[]int{0, g1, g2}[i]+split] = data[4*split+i]
	}
	return grouped
}
