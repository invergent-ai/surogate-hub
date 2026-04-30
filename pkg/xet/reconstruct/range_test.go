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
	info := xetstore.ShardInfo{
		Files: []xetstore.ShardFileInfo{{
			FileHash:  fileHash,
			SizeBytes: 60,
			Segments: []xetstore.ShardFileSegment{
				{XorbHash: xorbA, SizeBytes: 30, ChunkIndexStart: 0, ChunkIndexEnd: 2},
				{XorbHash: xorbB, SizeBytes: 30, ChunkIndexStart: 0, ChunkIndexEnd: 1},
			},
		}},
		Xorbs: []xetstore.ShardXorbInfo{
			{Hash: xorbA, Chunks: []xetstore.ShardChunkInfo{
				{Hash: "chunk-a", SizeBytes: 10},
				{Hash: "chunk-b", SizeBytes: 20},
			}},
			{Hash: xorbB, Chunks: []xetstore.ShardChunkInfo{
				{Hash: "chunk-c", SizeBytes: 30},
			}},
		},
	}

	terms, err := MapRange(info, fileHash, ByteRange{Start: 15, End: 45})
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
	info := xetstore.ShardInfo{
		Files: []xetstore.ShardFileInfo{{
			FileHash:  "file-a",
			SizeBytes: 10,
		}},
	}

	_, err := MapRange(info, "file-a", ByteRange{Start: 5, End: 11})
	require.Error(t, err)
	require.Contains(t, err.Error(), "range exceeds file size")
}
