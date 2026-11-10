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

// verificationKey is xet-core's VERIFICATION_KEY for shard range hashes.
var verificationKey = [32]byte{
	127, 24, 87, 214, 206, 86, 237, 102, 18, 127, 249, 19, 231, 165, 195, 243,
	164, 205, 38, 213, 181, 219, 73, 230, 65, 36, 152, 127, 40, 251, 148, 195,
}

var dataHashKey = [32]byte{
	102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28,
	157, 228, 33, 16, 155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41,
}

type ShardFileInfo struct {
	FileHash  string             `json:"file_hash"`
	SizeBytes uint64             `json:"size_bytes"`
	Segments  []ShardFileSegment `json:"segments,omitempty"`
	// RegisteredAt is when the record was stored (Unix seconds); zero in parsed
	// shards and in records stored before it existed.
	RegisteredAt int64 `json:"registered_at,omitempty"`
	// RangeHashes are the shard's verification entries, one per segment; never stored.
	RangeHashes []string `json:"-"`
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

func ParseShardFiles(data []byte) ([]ShardFileInfo, error) {
	reader := bytes.NewReader(data)
	if _, err := readShardHeader(reader); err != nil {
		return nil, err
	}
	return readShardFiles(reader)
}

func readShardHeader(reader io.Reader) (uint64, error) {
	var tag [32]byte
	if _, err := io.ReadFull(reader, tag[:]); err != nil {
		return 0, fmt.Errorf("read shard header tag: %w", err)
	}
	if tag != mdbShardHeaderTag {
		return 0, fmt.Errorf("invalid shard header tag")
	}
	version, err := readU64(reader)
	if err != nil {
		return 0, fmt.Errorf("read shard header version: %w", err)
	}
	if version != mdbShardHeaderVersion {
		return 0, fmt.Errorf("unsupported shard header version %d", version)
	}
	footerSize, err := readU64(reader)
	if err != nil {
		return 0, fmt.Errorf("read shard footer size: %w", err)
	}
	if footerSize != 0 && footerSize != mdbShardFooterSize {
		return 0, fmt.Errorf("unsupported shard footer size %d", footerSize)
	}
	return footerSize, nil
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
		// numEntries is untrusted; append grows only as far as the data goes.
		var segments []ShardFileSegment
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
		var rangeHashes []string
		if flags&mdbFileFlagWithVerification != 0 {
			for range segments {
				rangeHash, _, err := readMDBHash(reader)
				if err != nil {
					return nil, fmt.Errorf("read shard file verification entries: %w", err)
				}
				if err := skipBytes(reader, mdbShardEntrySize-32); err != nil {
					return nil, fmt.Errorf("read shard file verification entries: %w", err)
				}
				rangeHashes = append(rangeHashes, rangeHash)
			}
		}
		if flags&mdbFileFlagWithMetadataExt != 0 {
			if err := skipBytes(reader, mdbShardEntrySize); err != nil {
				return nil, fmt.Errorf("read shard file metadata extension: %w", err)
			}
		}
		files = append(files, ShardFileInfo{
			FileHash:    fileHash,
			SizeBytes:   sizeBytes,
			Segments:    segments,
			RangeHashes: rangeHashes,
		})
	}
	return files, nil
}

// VerifyFile checks a shard's file entry against the chunks of the xorbs it
// references and returns it with sizes taken from those chunks. Each segment's
// range hash proves the uploader has the real chunk hashes: without it a whole-
// xorb file could be registered from the xorb hash alone, and then read.
func VerifyFile(file ShardFileInfo, xorbChunks map[string][]ShardChunkInfo) (ShardFileInfo, error) {
	if len(file.RangeHashes) != len(file.Segments) {
		return ShardFileInfo{}, fmt.Errorf("file %s has no verification entries", file.FileHash)
	}
	var chunks []ShardChunkInfo
	file.SizeBytes = 0
	for i, segment := range file.Segments {
		xorb := xorbChunks[segment.XorbHash]
		if segment.ChunkIndexEnd < segment.ChunkIndexStart || int(segment.ChunkIndexEnd) > len(xorb) {
			return ShardFileInfo{}, fmt.Errorf("file %s references invalid chunk range %d-%d in xorb %s", file.FileHash, segment.ChunkIndexStart, segment.ChunkIndexEnd, segment.XorbHash)
		}
		rangeHash, err := ComputeRangeHash(xorb[segment.ChunkIndexStart:segment.ChunkIndexEnd])
		if err != nil {
			return ShardFileInfo{}, err
		}
		if rangeHash != file.RangeHashes[i] {
			return ShardFileInfo{}, fmt.Errorf("file %s fails verification of segment %d", file.FileHash, i)
		}
		file.Segments[i].SizeBytes = 0
		for _, chunk := range xorb[segment.ChunkIndexStart:segment.ChunkIndexEnd] {
			file.Segments[i].SizeBytes += chunk.SizeBytes
		}
		file.SizeBytes += file.Segments[i].SizeBytes
		chunks = append(chunks, xorb[segment.ChunkIndexStart:segment.ChunkIndexEnd]...)
	}
	computed, err := ComputeFileMerkleHash(chunks)
	if err != nil {
		return ShardFileInfo{}, err
	}
	if computed != file.FileHash {
		return ShardFileInfo{}, fmt.Errorf("file hash mismatch for %s", file.FileHash)
	}
	return file, nil
}

// ComputeRangeHash is xet-core's range_hash_from_chunks: the keyed hash of the
// chunks' raw hashes, concatenated.
func ComputeRangeHash(chunks []ShardChunkInfo) (string, error) {
	var buf bytes.Buffer
	for _, chunk := range chunks {
		raw, err := mdbHexToRaw(chunk.Hash)
		if err != nil {
			return "", err
		}
		buf.Write(raw)
	}
	return keyedHashHex(verificationKey[:], buf.Bytes()), nil
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
	if len(hash) != 64 {
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
