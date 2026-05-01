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
	shard, err := registry.GetShardByFileHash(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	info, err := xetstore.ParseShardInfo(shard)
	if err != nil {
		return nil, err
	}
	terms, err := reconstruct.MapRange(info, fileHash, byteRange)
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
	reader, err := xorbs.Get(ctx, "default", xorbHash)
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
