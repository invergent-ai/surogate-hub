package cas

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

const (
	xorbInfoIdent            = "XETBLOB"
	xorbHashSectionIdent     = "XBLBHSH"
	xorbBoundarySectionIdent = "XBLBBND"
	xorbChunkHeaderSize      = 8
)

type parsedXorbInfo struct {
	XorbHash        string
	ChunkHashes     []string
	ChunkBoundaries []uint32
	UnpackedOffsets []uint32
}

func validateSerializedXorb(expectedHash string, data []byte) error {
	info, footerStart, err := parseXorbInfo(data)
	if err != nil {
		return err
	}

	chunks, err := validateXorbChunks(data[:footerStart], info)
	if err != nil {
		return err
	}
	computedHash, err := xetstore.ComputeXorbMerkleHash(chunks)
	if err != nil {
		return err
	}
	if computedHash != expectedHash || info.XorbHash != expectedHash {
		return fmt.Errorf("xorb hash does not match body")
	}
	return nil
}

func parseXorbInfo(data []byte) (parsedXorbInfo, int, error) {
	if len(data) < 4 {
		return parsedXorbInfo{}, 0, fmt.Errorf("invalid xorb body")
	}
	infoLength := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	footerStart := len(data) - 4 - infoLength
	if infoLength <= 0 || footerStart < 0 {
		return parsedXorbInfo{}, 0, fmt.Errorf("invalid xorb footer length")
	}

	reader := bytes.NewReader(data[footerStart : len(data)-4])
	if err := readIdent(reader, xorbInfoIdent); err != nil {
		return parsedXorbInfo{}, 0, err
	}
	version, err := reader.ReadByte()
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb version: %w", err)
	}
	if version != 1 {
		return parsedXorbInfo{}, 0, fmt.Errorf("unsupported xorb version %d", version)
	}
	xorbHash, err := readXorbMDBHash(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb hash: %w", err)
	}

	hashSectionOffset := infoLength - reader.Len()
	if err := readIdent(reader, xorbHashSectionIdent); err != nil {
		return parsedXorbInfo{}, 0, err
	}
	hashVersion, err := reader.ReadByte()
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb hash section version: %w", err)
	}
	if hashVersion != 0 {
		return parsedXorbInfo{}, 0, fmt.Errorf("unsupported xorb hash section version %d", hashVersion)
	}
	numChunks, err := readXorbU32(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb hash count: %w", err)
	}
	chunkHashes := make([]string, 0, numChunks)
	for i := uint32(0); i < numChunks; i++ {
		chunkHash, err := readXorbMDBHash(reader)
		if err != nil {
			return parsedXorbInfo{}, 0, fmt.Errorf("read xorb chunk hash: %w", err)
		}
		chunkHashes = append(chunkHashes, chunkHash)
	}

	boundarySectionOffset := infoLength - reader.Len()
	if err := readIdent(reader, xorbBoundarySectionIdent); err != nil {
		return parsedXorbInfo{}, 0, err
	}
	boundaryVersion, err := reader.ReadByte()
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb boundary section version: %w", err)
	}
	if boundaryVersion != 1 {
		return parsedXorbInfo{}, 0, fmt.Errorf("unsupported xorb boundary section version %d", boundaryVersion)
	}
	boundaryChunks, err := readXorbU32(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb boundary count: %w", err)
	}
	if boundaryChunks != numChunks {
		return parsedXorbInfo{}, 0, fmt.Errorf("xorb inconsistent chunk count")
	}
	chunkBoundaries, err := readXorbU32s(reader, numChunks)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb chunk boundaries: %w", err)
	}
	unpackedOffsets, err := readXorbU32s(reader, numChunks)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb unpacked offsets: %w", err)
	}
	finalNumChunks, err := readXorbU32(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb final chunk count: %w", err)
	}
	if finalNumChunks != numChunks {
		return parsedXorbInfo{}, 0, fmt.Errorf("xorb inconsistent final chunk count")
	}
	hashOffsetFromEnd, err := readXorbU32(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb hash offset: %w", err)
	}
	boundaryOffsetFromEnd, err := readXorbU32(reader)
	if err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb boundary offset: %w", err)
	}
	if int(hashOffsetFromEnd) != infoLength-hashSectionOffset || int(boundaryOffsetFromEnd) != infoLength-boundarySectionOffset {
		return parsedXorbInfo{}, 0, fmt.Errorf("invalid xorb section offsets")
	}
	if _, err := io.CopyN(io.Discard, reader, 16); err != nil {
		return parsedXorbInfo{}, 0, fmt.Errorf("read xorb footer buffer: %w", err)
	}
	if reader.Len() != 0 {
		return parsedXorbInfo{}, 0, fmt.Errorf("xorb footer has trailing bytes")
	}

	return parsedXorbInfo{
		XorbHash:        xorbHash,
		ChunkHashes:     chunkHashes,
		ChunkBoundaries: chunkBoundaries,
		UnpackedOffsets: unpackedOffsets,
	}, footerStart, nil
}

func validateXorbChunks(data []byte, info parsedXorbInfo) ([]xetstore.ShardChunkInfo, error) {
	reader := bytes.NewReader(data)
	chunks := make([]xetstore.ShardChunkInfo, 0, len(info.ChunkHashes))
	var compressedOffset uint32
	var unpackedOffset uint32
	for i, expectedHash := range info.ChunkHashes {
		header, err := readXorbChunkHeader(reader)
		if err != nil {
			return nil, err
		}
		if header.compressionScheme != 0 {
			return nil, fmt.Errorf("unsupported xorb chunk compression scheme %d", header.compressionScheme)
		}
		chunk := make([]byte, header.compressedLength)
		if _, err := io.ReadFull(reader, chunk); err != nil {
			return nil, fmt.Errorf("read xorb chunk data: %w", err)
		}
		if header.compressedLength != header.uncompressedLength {
			return nil, fmt.Errorf("xorb uncompressed chunk length mismatch")
		}

		compressedOffset += xorbChunkHeaderSize + header.compressedLength
		unpackedOffset += header.uncompressedLength
		if compressedOffset != info.ChunkBoundaries[i] || unpackedOffset != info.UnpackedOffsets[i] {
			return nil, fmt.Errorf("xorb chunk boundary mismatch")
		}
		computedHash := xetstore.ComputeDataHash(chunk)
		if computedHash != expectedHash {
			return nil, fmt.Errorf("xorb chunk hash mismatch")
		}
		chunks = append(chunks, xetstore.ShardChunkInfo{
			Hash:      computedHash,
			SizeBytes: uint64(header.uncompressedLength),
		})
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("xorb content bytes after chunks")
	}
	return chunks, nil
}

type xorbChunkHeader struct {
	compressedLength   uint32
	compressionScheme  uint8
	uncompressedLength uint32
}

func readXorbChunkHeader(reader io.Reader) (xorbChunkHeader, error) {
	var raw [xorbChunkHeaderSize]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return xorbChunkHeader{}, fmt.Errorf("read xorb chunk header: %w", err)
	}
	if raw[0] != 0 {
		return xorbChunkHeader{}, fmt.Errorf("unsupported xorb chunk header version %d", raw[0])
	}
	return xorbChunkHeader{
		compressedLength:   uint32(raw[1]) | uint32(raw[2])<<8 | uint32(raw[3])<<16,
		compressionScheme:  raw[4],
		uncompressedLength: uint32(raw[5]) | uint32(raw[6])<<8 | uint32(raw[7])<<16,
	}, nil
}

func isXETHash(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readIdent(reader io.Reader, expected string) error {
	raw := make([]byte, len(expected))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return fmt.Errorf("read xorb ident: %w", err)
	}
	if string(raw) != expected {
		return fmt.Errorf("invalid xorb ident")
	}
	return nil
}

func readXorbMDBHash(reader io.Reader) (string, error) {
	var raw [32]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x%016x%016x%016x",
		binary.LittleEndian.Uint64(raw[0:8]),
		binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]),
		binary.LittleEndian.Uint64(raw[24:32]),
	), nil
}

func readXorbU32(reader io.Reader) (uint32, error) {
	var raw [4]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(raw[:]), nil
}

func readXorbU32s(reader io.Reader, count uint32) ([]uint32, error) {
	values := make([]uint32, 0, count)
	for i := uint32(0); i < count; i++ {
		value, err := readXorbU32(reader)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
