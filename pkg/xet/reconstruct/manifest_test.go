package reconstruct

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildManifestGroupsTermsAndResolvesXorbRanges(t *testing.T) {
	terms := []Term{
		{XorbHash: "xorb-a", ChunkIndex: 1, ChunkSizeBytes: 20, ChunkOffset: 5},
		{XorbHash: "xorb-a", ChunkIndex: 2, ChunkSizeBytes: 30},
		{XorbHash: "xorb-b", ChunkIndex: 0, ChunkSizeBytes: 40},
	}
	resolve := func(xorbHash string, chunks IndexRange) (ResolvedRange, error) {
		return ResolvedRange{
			URL: fmt.Sprintf("https://cas.example/%s/%d-%d", xorbHash, chunks.Start, chunks.End),
			Bytes: HTTPRange{
				Start: uint64(chunks.Start) * 100,
				End:   uint64(chunks.End)*100 - 1,
			},
		}, nil
	}

	manifest, err := BuildManifest(terms, resolve)
	require.NoError(t, err)

	require.Equal(t, Manifest{
		OffsetIntoFirstRange: 5,
		Terms: []ManifestTerm{
			{Hash: "xorb-a", Range: IndexRange{Start: 1, End: 3}, UnpackedLength: 50},
			{Hash: "xorb-b", Range: IndexRange{Start: 0, End: 1}, UnpackedLength: 40},
		},
		Xorbs: map[string][]XorbMultiRangeFetch{
			"xorb-a": {{
				URL: "https://cas.example/xorb-a/1-3",
				Ranges: []XorbRangeDescriptor{{
					Chunks: IndexRange{Start: 1, End: 3},
					Bytes:  HTTPRange{Start: 100, End: 299},
				}},
			}},
			"xorb-b": {{
				URL: "https://cas.example/xorb-b/0-1",
				Ranges: []XorbRangeDescriptor{{
					Chunks: IndexRange{Start: 0, End: 1},
					Bytes:  HTTPRange{Start: 0, End: 99},
				}},
			}},
		},
	}, manifest)
}

func TestBuildManifestCoalescesRepeatedXorbRangesByURL(t *testing.T) {
	terms := []Term{
		{XorbHash: "xorb-a", ChunkIndex: 0, ChunkSizeBytes: 10},
		{XorbHash: "xorb-a", ChunkIndex: 1, ChunkSizeBytes: 20},
		{XorbHash: "xorb-b", ChunkIndex: 0, ChunkSizeBytes: 30},
		{XorbHash: "xorb-a", ChunkIndex: 2, ChunkSizeBytes: 40},
	}
	resolve := func(xorbHash string, chunks IndexRange) (ResolvedRange, error) {
		return ResolvedRange{
			URL: fmt.Sprintf("https://cas.example/%s", xorbHash),
			Bytes: HTTPRange{
				Start: uint64(chunks.Start) * 100,
				End:   uint64(chunks.End)*100 - 1,
			},
		}, nil
	}

	manifest, err := BuildManifest(terms, resolve)
	require.NoError(t, err)

	require.Equal(t, []ManifestTerm{
		{Hash: "xorb-a", Range: IndexRange{Start: 0, End: 2}, UnpackedLength: 30},
		{Hash: "xorb-b", Range: IndexRange{Start: 0, End: 1}, UnpackedLength: 30},
		{Hash: "xorb-a", Range: IndexRange{Start: 2, End: 3}, UnpackedLength: 40},
	}, manifest.Terms)
	require.Equal(t, []XorbMultiRangeFetch{{
		URL: "https://cas.example/xorb-a",
		Ranges: []XorbRangeDescriptor{
			{
				Chunks: IndexRange{Start: 0, End: 2},
				Bytes:  HTTPRange{Start: 0, End: 199},
			},
			{
				Chunks: IndexRange{Start: 2, End: 3},
				Bytes:  HTTPRange{Start: 200, End: 299},
			},
		},
	}}, manifest.Xorbs["xorb-a"])
}
