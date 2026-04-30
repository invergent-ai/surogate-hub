package reconstruct

import (
	"fmt"

	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
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

func MapRange(info xetstore.ShardInfo, fileHash string, requested ByteRange) ([]Term, error) {
	file, ok := findFile(info.Files, fileHash)
	if !ok {
		return nil, fmt.Errorf("file hash %s not found in shard", fileHash)
	}
	if requested.End < requested.Start {
		return nil, fmt.Errorf("invalid range")
	}
	if requested.End > file.SizeBytes {
		return nil, fmt.Errorf("range exceeds file size")
	}
	if requested.Start == requested.End {
		return nil, nil
	}

	xorbs := make(map[string]xetstore.ShardXorbInfo, len(info.Xorbs))
	for _, xorb := range info.Xorbs {
		xorbs[xorb.Hash] = xorb
	}

	var terms []Term
	var fileOffset uint64
	for _, segment := range file.Segments {
		xorb, ok := xorbs[segment.XorbHash]
		if !ok {
			return nil, fmt.Errorf("segment references missing xorb %s", segment.XorbHash)
		}
		if segment.ChunkIndexEnd < segment.ChunkIndexStart || int(segment.ChunkIndexEnd) > len(xorb.Chunks) {
			return nil, fmt.Errorf("segment references invalid chunk range %d-%d in xorb %s", segment.ChunkIndexStart, segment.ChunkIndexEnd, segment.XorbHash)
		}
		for chunkIndex := segment.ChunkIndexStart; chunkIndex < segment.ChunkIndexEnd; chunkIndex++ {
			chunk := xorb.Chunks[chunkIndex]
			chunkStart := fileOffset
			chunkEnd := fileOffset + chunk.SizeBytes
			if chunkEnd > requested.Start && chunkStart < requested.End {
				termStart := max(chunkStart, requested.Start)
				termEnd := min(chunkEnd, requested.End)
				terms = append(terms, Term{
					FileOffset:     termStart,
					XorbHash:       xorb.Hash,
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

func findFile(files []xetstore.ShardFileInfo, fileHash string) (xetstore.ShardFileInfo, bool) {
	for _, file := range files {
		if file.FileHash == fileHash {
			return file, true
		}
	}
	return xetstore.ShardFileInfo{}, false
}
