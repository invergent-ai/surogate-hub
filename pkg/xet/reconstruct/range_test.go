package reconstruct

import (
	"testing"

	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/stretchr/testify/require"
)

func TestMapRangeReturnsOverlappingChunkTerms(t *testing.T) {
	fileHash := "file-a"
	xorbA := "xorb-a"
	xorbB := "xorb-b"
	file := xetstore.ShardFileInfo{
		FileHash:  fileHash,
		SizeBytes: 60,
		Segments: []xetstore.ShardFileSegment{
			{XorbHash: xorbA, SizeBytes: 30, ChunkIndexStart: 0, ChunkIndexEnd: 2},
			{XorbHash: xorbB, SizeBytes: 30, ChunkIndexStart: 0, ChunkIndexEnd: 1},
		},
	}
	xorbChunks := map[string][]xetstore.ShardChunkInfo{
		xorbA: {{Hash: "chunk-a", SizeBytes: 10}, {Hash: "chunk-b", SizeBytes: 20}},
		xorbB: {{Hash: "chunk-c", SizeBytes: 30}},
	}

	terms, err := MapRange(file, xorbChunks, ByteRange{Start: 15, End: 45})
	require.NoError(t, err)

	require.Equal(t, []Term{
		{
			FileOffset:     15,
			XorbHash:       xorbA,
			ChunkHash:      "chunk-b",
			ChunkIndex:     1,
			ChunkSizeBytes: 20,
			ChunkOffset:    5,
			Length:         15,
		},
		{
			FileOffset:     30,
			XorbHash:       xorbB,
			ChunkHash:      "chunk-c",
			ChunkIndex:     0,
			ChunkSizeBytes: 30,
			ChunkOffset:    0,
			Length:         15,
		},
	}, terms)
}

func TestMapRangeRejectsOutOfBoundsRange(t *testing.T) {
	file := xetstore.ShardFileInfo{FileHash: "file-a", SizeBytes: 10}

	_, err := MapRange(file, nil, ByteRange{Start: 5, End: 11})
	require.Error(t, err)
	require.Contains(t, err.Error(), "range exceeds file size")
}
