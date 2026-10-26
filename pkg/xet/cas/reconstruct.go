package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/invergent-ai/surogate-hub/pkg/xet/reconstruct"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
)

func ReconstructFileRange(ctx context.Context, registry *xetstore.Registry, xorbs *XorbStore, fileHash string, byteRange reconstruct.ByteRange) (io.ReadCloser, error) {
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

	xorbChunks := make(map[string][][]byte)
	var out bytes.Buffer
	for _, term := range terms {
		chunks, ok := xorbChunks[term.XorbHash]
		if !ok {
			chunks, err = readXorbChunks(ctx, xorbs, term.XorbHash)
			if err != nil {
				return nil, err
			}
			xorbChunks[term.XorbHash] = chunks
		}
		if int(term.ChunkIndex) >= len(chunks) {
			return nil, fmt.Errorf("xorb %s missing chunk %d", term.XorbHash, term.ChunkIndex)
		}
		chunk := chunks[term.ChunkIndex]
		end := term.ChunkOffset + term.Length
		if end > uint64(len(chunk)) {
			return nil, fmt.Errorf("term exceeds chunk bounds")
		}
		out.Write(chunk[int(term.ChunkOffset):int(end)])
	}
	return io.NopCloser(bytes.NewReader(out.Bytes())), nil
}

func readXorbChunks(ctx context.Context, xorbs *XorbStore, xorbHash string) ([][]byte, error) {
	if xorbs == nil {
		return nil, fmt.Errorf("xorb store is not configured")
	}
	reader, err := xorbs.Get(ctx, xorbPrefix, xorbHash)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	info, footerStart, err := parseXorbInfo(data)
	if err != nil {
		return nil, err
	}
	return decodeXorbChunks(data[:footerStart], info)
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
