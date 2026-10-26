package reconstruct

import (
	"fmt"

	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

type ByteRange struct {
	Start uint64
	End   uint64
}

type Term struct {
	FileOffset     uint64
	XorbHash       string
	ChunkHash      string
	ChunkIndex     uint32
	ChunkSizeBytes uint64
	ChunkOffset    uint64
	Length         uint64
}

// MapRange maps a byte range of a file onto chunk terms, given the chunks of
// every xorb the file references.
func MapRange(file xetstore.ShardFileInfo, xorbChunks map[string][]xetstore.ShardChunkInfo, requested ByteRange) ([]Term, error) {
	if requested.End < requested.Start {
		return nil, fmt.Errorf("invalid range")
	}
	if requested.End > file.SizeBytes {
		return nil, fmt.Errorf("range exceeds file size")
	}
	if requested.Start == requested.End {
		return nil, nil
	}

	var terms []Term
	var fileOffset uint64
	for _, segment := range file.Segments {
		chunks, ok := xorbChunks[segment.XorbHash]
		if !ok {
			return nil, fmt.Errorf("segment references missing xorb %s", segment.XorbHash)
		}
		if segment.ChunkIndexEnd < segment.ChunkIndexStart || int(segment.ChunkIndexEnd) > len(chunks) {
			return nil, fmt.Errorf("segment references invalid chunk range %d-%d in xorb %s", segment.ChunkIndexStart, segment.ChunkIndexEnd, segment.XorbHash)
		}
		for chunkIndex := segment.ChunkIndexStart; chunkIndex < segment.ChunkIndexEnd; chunkIndex++ {
			chunk := chunks[chunkIndex]
			chunkStart := fileOffset
			chunkEnd := fileOffset + chunk.SizeBytes
			if chunkEnd > requested.Start && chunkStart < requested.End {
				termStart := max(chunkStart, requested.Start)
				termEnd := min(chunkEnd, requested.End)
				terms = append(terms, Term{
					FileOffset:     termStart,
					XorbHash:       segment.XorbHash,
					ChunkHash:      chunk.Hash,
					ChunkIndex:     chunkIndex,
					ChunkSizeBytes: chunk.SizeBytes,
					ChunkOffset:    termStart - chunkStart,
					Length:         termEnd - termStart,
				})
			}
			fileOffset = chunkEnd
		}
	}
	return terms, nil
}
