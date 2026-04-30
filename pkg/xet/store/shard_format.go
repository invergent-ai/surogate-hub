package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"lukechampine.com/blake3"
)

const (
	mdbShardHeaderVersion = uint64(2)
	mdbShardFooterVersion = uint64(1)
	mdbShardFooterSize    = uint64(200)

	mdbFileFlagWithVerification = uint32(1 << 31)
	mdbFileFlagWithMetadataExt  = uint32(1 << 30)
	mdbShardEntrySize           = uint64(48)
)

var mdbShardHeaderTag = [32]byte{
	'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0, 85,
	105, 103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
}

var internalNodeHashKey = [32]byte{
	1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230,
	93, 221, 83, 111, 55, 199, 109, 210, 248, 99, 82, 230, 74, 83, 113, 63,
}

var dataHashKey = [32]byte{
	102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28,
	157, 228, 33, 16, 155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41,
}

type ShardInfo struct {
	Files       []ShardFileInfo
	XorbHashes  []string
	ChunkHashes []string
	Summary     ShardSummary
}

type ShardFileInfo struct {
	FileHash  string             `json:"file_hash"`
	SizeBytes uint64             `json:"size_bytes"`
	Segments  []ShardFileSegment `json:"segments,omitempty"`
}

type ShardFileSegment struct {
	XorbHash        string `json:"xorb_hash"`
	SizeBytes       uint64 `json:"size_bytes"`
	ChunkIndexStart uint32 `json:"chunk_index_start"`
	ChunkIndexEnd   uint32 `json:"chunk_index_end"`
}

type ShardChunkInfo struct {
	Hash      string
	SizeBytes uint64
}

type shardXorbInfo struct {
	Hash   string
	Chunks []ShardChunkInfo
}

type ShardSummary struct {
	CreatedAt uint64 `json:"created_at"`
	SizeBytes uint64 `json:"size"`
	NumXorbs  int    `json:"num_xorbs"`
	NumChunks int    `json:"num_chunks"`
}

func ParseShardInfo(data []byte) (ShardInfo, error) {
	reader := bytes.NewReader(data)
	if err := readShardHeader(reader); err != nil {
		return ShardInfo{}, err
	}

	files, err := readShardFiles(reader)
	if err != nil {
		return ShardInfo{}, err
	}
	xorbHashes, chunkHashes, err := readShardXorbs(reader)
	if err != nil {
		return ShardInfo{}, err
	}
	if err := verifyShardFileHashes(files, xorbHashes); err != nil {
		return ShardInfo{}, err
	}
	summary, err := readShardFooter(reader)
	if err != nil {
		return ShardInfo{}, err
	}
	summary.NumXorbs = len(xorbHashes)
	summary.NumChunks = len(chunkHashes)
	if summary.SizeBytes == 0 {
		for _, file := range files {
			summary.SizeBytes += file.SizeBytes
		}
	}

	return ShardInfo{
		Files:       files,
		XorbHashes:  shardXorbHashes(xorbHashes),
		ChunkHashes: chunkHashes,
		Summary:     summary,
	}, nil
}

func readShardHeader(reader io.Reader) error {
	var tag [32]byte
	if _, err := io.ReadFull(reader, tag[:]); err != nil {
		return fmt.Errorf("read shard header tag: %w", err)
	}
	if tag != mdbShardHeaderTag {
		return fmt.Errorf("invalid shard header tag")
	}
	version, err := readU64(reader)
	if err != nil {
		return fmt.Errorf("read shard header version: %w", err)
	}
	if version != mdbShardHeaderVersion {
		return fmt.Errorf("unsupported shard header version %d", version)
	}
	footerSize, err := readU64(reader)
	if err != nil {
		return fmt.Errorf("read shard footer size: %w", err)
	}
	if footerSize != mdbShardFooterSize {
		return fmt.Errorf("unsupported shard footer size %d", footerSize)
	}
	return nil
}

func readShardFiles(reader io.Reader) ([]ShardFileInfo, error) {
	var files []ShardFileInfo
	for {
		fileHash, isBookend, err := readMDBHash(reader)
		if err != nil {
			return nil, fmt.Errorf("read shard file hash: %w", err)
		}
		flags, err := readU32(reader)
		if err != nil {
			return nil, fmt.Errorf("read shard file flags: %w", err)
		}
		numEntries, err := readU32(reader)
		if err != nil {
			return nil, fmt.Errorf("read shard file entry count: %w", err)
		}
		if _, err := readU64(reader); err != nil {
			return nil, fmt.Errorf("read shard file unused field: %w", err)
		}
		if isBookend {
			break
		}

		var sizeBytes uint64
		segments := make([]ShardFileSegment, 0, numEntries)
		for i := uint32(0); i < numEntries; i++ {
			xorbHash, _, err := readMDBHash(reader)
			if err != nil {
				return nil, fmt.Errorf("read shard file xorb hash: %w", err)
			}
			if err := skipU32s(reader, 1); err != nil {
				return nil, fmt.Errorf("read shard file xorb flags: %w", err)
			}
			unpackedSegmentBytes, err := readU32(reader)
			if err != nil {
				return nil, fmt.Errorf("read shard file segment size: %w", err)
			}
			sizeBytes += uint64(unpackedSegmentBytes)
			chunkIndexStart, err := readU32(reader)
			if err != nil {
				return nil, fmt.Errorf("read shard file chunk range start: %w", err)
			}
			chunkIndexEnd, err := readU32(reader)
			if err != nil {
				return nil, fmt.Errorf("read shard file chunk range end: %w", err)
			}
			segments = append(segments, ShardFileSegment{
				XorbHash:        xorbHash,
				SizeBytes:       uint64(unpackedSegmentBytes),
				ChunkIndexStart: chunkIndexStart,
				ChunkIndexEnd:   chunkIndexEnd,
			})
		}
		if flags&mdbFileFlagWithVerification != 0 {
			if err := skipBytes(reader, uint64(numEntries)*mdbShardEntrySize); err != nil {
				return nil, fmt.Errorf("read shard file verification entries: %w", err)
			}
		}
		if flags&mdbFileFlagWithMetadataExt != 0 {
			if err := skipBytes(reader, mdbShardEntrySize); err != nil {
				return nil, fmt.Errorf("read shard file metadata extension: %w", err)
			}
		}
		files = append(files, ShardFileInfo{
			FileHash:  fileHash,
			SizeBytes: sizeBytes,
			Segments:  segments,
		})
	}
	return files, nil
}

func readShardXorbs(reader io.Reader) ([]shardXorbInfo, []string, error) {
	var xorbs []shardXorbInfo
	var chunkHashes []string
	for {
		xorbHash, isBookend, err := readMDBHash(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("read shard xorb hash: %w", err)
		}
		if err := skipU32s(reader, 1); err != nil {
			return nil, nil, fmt.Errorf("read shard xorb flags: %w", err)
		}
		numEntries, err := readU32(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("read shard xorb entry count: %w", err)
		}
		if err := skipU32s(reader, 2); err != nil {
			return nil, nil, fmt.Errorf("read shard xorb byte counts: %w", err)
		}
		if isBookend {
			break
		}
		chunks := make([]ShardChunkInfo, 0, numEntries)
		for i := uint32(0); i < numEntries; i++ {
			chunkHash, _, err := readMDBHash(reader)
			if err != nil {
				return nil, nil, fmt.Errorf("read shard chunk hash: %w", err)
			}
			chunkHashes = append(chunkHashes, chunkHash)
			if err := skipU32s(reader, 1); err != nil {
				return nil, nil, fmt.Errorf("read shard chunk byte range start: %w", err)
			}
			unpackedSegmentBytes, err := readU32(reader)
			if err != nil {
				return nil, nil, fmt.Errorf("read shard chunk segment size: %w", err)
			}
			if err := skipU32s(reader, 2); err != nil {
				return nil, nil, fmt.Errorf("read shard chunk flags: %w", err)
			}
			chunks = append(chunks, ShardChunkInfo{
				Hash:      chunkHash,
				SizeBytes: uint64(unpackedSegmentBytes),
			})
		}
		xorbs = append(xorbs, shardXorbInfo{Hash: xorbHash, Chunks: chunks})
	}
	return xorbs, chunkHashes, nil
}

func verifyShardFileHashes(files []ShardFileInfo, xorbs []shardXorbInfo) error {
	xorbByHash := make(map[string]shardXorbInfo, len(xorbs))
	for _, xorb := range xorbs {
		xorbByHash[xorb.Hash] = xorb
	}
	for _, file := range files {
		var chunks []ShardChunkInfo
		for _, segment := range file.Segments {
			xorb, ok := xorbByHash[segment.XorbHash]
			if !ok {
				return fmt.Errorf("file %s references missing xorb %s", file.FileHash, segment.XorbHash)
			}
			if segment.ChunkIndexEnd < segment.ChunkIndexStart || int(segment.ChunkIndexEnd) > len(xorb.Chunks) {
				return fmt.Errorf("file %s references invalid chunk range %d-%d in xorb %s", file.FileHash, segment.ChunkIndexStart, segment.ChunkIndexEnd, segment.XorbHash)
			}
			chunks = append(chunks, xorb.Chunks[segment.ChunkIndexStart:segment.ChunkIndexEnd]...)
		}
		computed, err := ComputeFileMerkleHash(chunks)
		if err != nil {
			return err
		}
		if computed != file.FileHash {
			return fmt.Errorf("file hash mismatch: shard has %s, computed %s", file.FileHash, computed)
		}
	}
	return nil
}

func shardXorbHashes(xorbs []shardXorbInfo) []string {
	hashes := make([]string, 0, len(xorbs))
	for _, xorb := range xorbs {
		hashes = append(hashes, xorb.Hash)
	}
	return hashes
}

func ComputeFileMerkleHash(chunks []ShardChunkInfo) (string, error) {
	if len(chunks) == 0 {
		return "0000000000000000000000000000000000000000000000000000000000000000", nil
	}
	xorbHash, err := ComputeXorbMerkleHash(chunks)
	if err != nil {
		return "", err
	}
	rawHash, err := mdbHexToRaw(xorbHash)
	if err != nil {
		return "", err
	}
	return keyedHashHex(make([]byte, 32), rawHash), nil
}

func ComputeXorbMerkleHash(chunks []ShardChunkInfo) (string, error) {
	if len(chunks) == 0 {
		return "0000000000000000000000000000000000000000000000000000000000000000", nil
	}
	hashes := append([]ShardChunkInfo(nil), chunks...)
	for len(hashes) > 1 {
		writeIndex := 0
		readIndex := 0
		for readIndex != len(hashes) {
			nextCut := readIndex + nextMergeCut(hashes[readIndex:])
			merged, err := mergedHashOfSequence(hashes[readIndex:nextCut])
			if err != nil {
				return "", err
			}
			hashes[writeIndex] = merged
			writeIndex++
			readIndex = nextCut
		}
		hashes = hashes[:writeIndex]
	}
	rawHash, err := mdbHexToRaw(hashes[0].Hash)
	if err != nil {
		return "", err
	}
	return mdbHexFromRaw(rawHash), nil
}

func ComputeDataHash(data []byte) string {
	return keyedHashHex(dataHashKey[:], data)
}

func nextMergeCut(hashes []ShardChunkInfo) int {
	if len(hashes) <= 2 {
		return len(hashes)
	}
	end := min(9, len(hashes))
	for i := 2; i < end; i++ {
		if hashModulo(hashes[i].Hash, 4) == 0 {
			return i + 1
		}
	}
	return end
}

func mergedHashOfSequence(chunks []ShardChunkInfo) (ShardChunkInfo, error) {
	var buf bytes.Buffer
	var totalLen uint64
	for _, chunk := range chunks {
		if _, err := mdbHexToRaw(chunk.Hash); err != nil {
			return ShardChunkInfo{}, err
		}
		fmt.Fprintf(&buf, "%s : %d\n", chunk.Hash, chunk.SizeBytes)
		totalLen += chunk.SizeBytes
	}
	return ShardChunkInfo{
		Hash:      keyedHashHex(internalNodeHashKey[:], buf.Bytes()),
		SizeBytes: totalLen,
	}, nil
}

func hashModulo(hash string, divisor uint64) uint64 {
	if len(hash) < 16 {
		return 0
	}
	value, err := strconv.ParseUint(hash[48:64], 16, 64)
	if err != nil {
		return 0
	}
	return value % divisor
}

func keyedHashHex(key, data []byte) string {
	hasher := blake3.New(32, key)
	_, _ = hasher.Write(data)
	return mdbHexFromRaw(hasher.Sum(nil))
}

func mdbHexToRaw(hash string) ([]byte, error) {
	if len(hash) != 64 {
		return nil, fmt.Errorf("invalid merkle hash %q", hash)
	}
	raw := make([]byte, 32)
	for i := 0; i < 4; i++ {
		value, err := strconv.ParseUint(hash[i*16:(i+1)*16], 16, 64)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint64(raw[i*8:(i+1)*8], value)
	}
	return raw, nil
}

func mdbHexFromRaw(raw []byte) string {
	var dst bytes.Buffer
	for i := 0; i < 4; i++ {
		for j := 7; j >= 0; j-- {
			dst.WriteString(hex.EncodeToString(raw[i*8+j : i*8+j+1]))
		}
	}
	return dst.String()
}

func readShardFooter(reader io.Reader) (ShardSummary, error) {
	version, err := readU64(reader)
	if err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer version: %w", err)
	}
	if version != mdbShardFooterVersion {
		return ShardSummary{}, fmt.Errorf("unsupported shard footer version %d", version)
	}
	if err := skipBytes(reader, 8*8); err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer offsets: %w", err)
	}
	if err := skipBytes(reader, 32); err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer hmac key: %w", err)
	}
	createdAt, err := readU64(reader)
	if err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer creation timestamp: %w", err)
	}
	if err := skipBytes(reader, 8+6*8+8); err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer size prelude: %w", err)
	}
	sizeBytes, err := readU64(reader)
	if err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer materialized bytes: %w", err)
	}
	if err := skipBytes(reader, 8+8); err != nil {
		return ShardSummary{}, fmt.Errorf("read shard footer tail: %w", err)
	}
	return ShardSummary{
		CreatedAt: createdAt,
		SizeBytes: sizeBytes,
	}, nil
}

func readMDBHash(reader io.Reader) (string, bool, error) {
	var raw [32]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return "", false, err
	}
	isBookend := true
	for _, b := range raw {
		if b != 0xff {
			isBookend = false
			break
		}
	}
	return fmt.Sprintf("%016x%016x%016x%016x",
		binary.LittleEndian.Uint64(raw[0:8]),
		binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]),
		binary.LittleEndian.Uint64(raw[24:32]),
	), isBookend, nil
}

func skipHash(reader io.Reader) error {
	var raw [32]byte
	_, err := io.ReadFull(reader, raw[:])
	return err
}

func readU32(reader io.Reader) (uint32, error) {
	var raw [4]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(raw[:]), nil
}

func readU64(reader io.Reader) (uint64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(raw[:]), nil
}

func skipU32s(reader io.Reader, count int) error {
	return skipBytes(reader, uint64(count)*4)
}

func skipBytes(reader io.Reader, count uint64) error {
	_, err := io.CopyN(io.Discard, reader, int64(count))
	return err
}
