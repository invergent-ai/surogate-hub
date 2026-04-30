package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

type ShardInfo struct {
	Files       []ShardFileInfo
	XorbHashes  []string
	ChunkHashes []string
	Summary     ShardSummary
}

type ShardFileInfo struct {
	FileHash  string
	SizeBytes uint64
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
		XorbHashes:  xorbHashes,
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
		for i := uint32(0); i < numEntries; i++ {
			if err := skipHash(reader); err != nil {
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
			if err := skipU32s(reader, 2); err != nil {
				return nil, fmt.Errorf("read shard file chunk range: %w", err)
			}
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
		})
	}
	return files, nil
}

func readShardXorbs(reader io.Reader) ([]string, []string, error) {
	var xorbHashes []string
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
		xorbHashes = append(xorbHashes, xorbHash)
		for i := uint32(0); i < numEntries; i++ {
			chunkHash, _, err := readMDBHash(reader)
			if err != nil {
				return nil, nil, fmt.Errorf("read shard chunk hash: %w", err)
			}
			chunkHashes = append(chunkHashes, chunkHash)
			if err := skipU32s(reader, 4); err != nil {
				return nil, nil, fmt.Errorf("read shard chunk metadata: %w", err)
			}
		}
	}
	return xorbHashes, chunkHashes, nil
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
