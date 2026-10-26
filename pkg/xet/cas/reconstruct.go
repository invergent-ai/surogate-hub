package cas

import (
	"context"
	"fmt"
	"io"

	"github.com/invergent-ai/surogate-hub/pkg/xet/reconstruct"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

// ReconstructFileRange streams a byte range of a file. It fetches only the
// chunks the range needs, one run of consecutive chunks per xorb read.
func ReconstructFileRange(ctx context.Context, registry *xetstore.Registry, xorbs *XorbStore, fileHash string, byteRange reconstruct.ByteRange) (io.ReadCloser, error) {
	if xorbs == nil {
		return nil, fmt.Errorf("xorb store is not configured")
	}
	file, err := registry.GetFile(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	infos, err := xorbInfos(ctx, registry, file.Segments)
	if err != nil {
		return nil, err
	}
	terms, err := reconstruct.MapRange(file, xorbChunks(infos), byteRange)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	go func() {
		writer.CloseWithError(writeTerms(ctx, xorbs, infos, terms, writer))
	}()
	return reader, nil
}

func writeTerms(ctx context.Context, xorbs *XorbStore, infos map[string]parsedXorbInfo, terms []reconstruct.Term, w io.Writer) error {
	for start := 0; start < len(terms); {
		end := start + 1
		for end < len(terms) && terms[end].XorbHash == terms[start].XorbHash && terms[end].ChunkIndex == terms[end-1].ChunkIndex+1 {
			end++
		}
		if err := writeTermRun(ctx, xorbs, infos[terms[start].XorbHash], terms[start:end], w); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// writeTermRun writes terms that cover consecutive chunks of one xorb.
func writeTermRun(ctx context.Context, xorbs *XorbStore, info parsedXorbInfo, terms []reconstruct.Term, w io.Writer) error {
	byteRange, err := xorbByteRange(info, reconstruct.IndexRange{Start: terms[0].ChunkIndex, End: terms[len(terms)-1].ChunkIndex + 1})
	if err != nil {
		return err
	}
	reader, err := xorbs.adapter.GetRange(ctx, xorbs.objectPointer(terms[0].XorbHash), int64(byteRange.Start), int64(byteRange.End))
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	for _, term := range terms {
		_, chunk, err := readXorbChunk(reader)
		if err != nil {
			return err
		}
		end := term.ChunkOffset + term.Length
		if end > uint64(len(chunk)) {
			return fmt.Errorf("term exceeds chunk bounds")
		}
		if _, err := w.Write(chunk[term.ChunkOffset:end]); err != nil {
			return err
		}
	}
	return nil
}

// xorbInfos loads the stored footer of every xorb the segments reference.
func xorbInfos(ctx context.Context, registry *xetstore.Registry, segments []xetstore.ShardFileSegment) (map[string]parsedXorbInfo, error) {
	infos := make(map[string]parsedXorbInfo)
	for _, segment := range segments {
		if _, ok := infos[segment.XorbHash]; ok {
			continue
		}
		footer, err := registry.GetXorbInfo(ctx, segment.XorbHash)
		if err != nil {
			return nil, fmt.Errorf("xorb %s: %w", segment.XorbHash, err)
		}
		info, _, err := parseXorbInfo(footer)
		if err != nil {
			return nil, fmt.Errorf("xorb %s: %w", segment.XorbHash, err)
		}
		infos[segment.XorbHash] = info
	}
	return infos, nil
}

func xorbChunks(infos map[string]parsedXorbInfo) map[string][]xetstore.ShardChunkInfo {
	chunks := make(map[string][]xetstore.ShardChunkInfo, len(infos))
	for hash, info := range infos {
		chunks[hash] = shardChunksFromParsedXorb(info)
	}
	return chunks
}
