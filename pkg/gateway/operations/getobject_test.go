package operations

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	xetcas "github.com/invergent-ai/surogate-hub/pkg/xet/cas"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestGetObjectXETPhysicalAddressRange(t *testing.T) {
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.BaseConfig{}
	cfg, err := config.NewConfig("", cfg)
	require.NoError(t, err)
	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvtest.GetStore(ctx, t),
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	repo := "repo-a"
	const branch = "main"
	const path = "models/checkpoint.bin"
	_, err = c.CreateRepository(ctx, repo, "", "mem://repo-a", branch, false)
	require.NoError(t, err)
	chunk := []byte("hello world!")
	xorbHash, xorbBytes := testGatewaySerializedXorb(t, chunk)
	chunkHash := xetstore.ComputeDataHash(chunk)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)
	xorbStore := xetcas.NewXorbStore(c.BlockAdapter, "mem://_lakefs_xet")
	_, err = xorbStore.Put(ctx, "default", xorbHash, int64(len(xorbBytes)), bytes.NewReader(xorbBytes))
	require.NoError(t, err)
	_, err = xetstore.NewRegistry(c.KVStore).RegisterShard(ctx, xetstore.RegisterShardParams{
		FileHash: fileHash,
		Shard:    testGatewayXETBinaryShard(t, fileHash, xorbHash, chunkHash, uint32(len(chunk))),
	})
	require.NoError(t, err)
	err = c.CreateEntry(ctx, repo, branch, catalog.DBEntry{
		Path:            path,
		PhysicalAddress: "xet://" + fileHash,
		AddressType:     catalog.AddressTypeFull,
		CreationDate:    time.Now(),
		Size:            int64(len(chunk)),
		Checksum:        "checksum-a",
	})
	require.NoError(t, err)
	_, err = c.GetEntry(ctx, repo, branch, path, catalog.GetEntryParams{})
	require.NoError(t, err)
	repository, err := c.GetRepository(ctx, repo)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/"+repo+"/"+path, nil)
	req.Header.Set("Range", "bytes=3-8")
	rec := httptest.NewRecorder()
	(&GetObject{}).Handle(rec, req, &PathOperation{
		RefOperation: &RefOperation{
			RepoOperation: &RepoOperation{
				AuthorizedOperation: &AuthorizedOperation{
					Operation: &Operation{
						Region:     "us-east-1",
						Catalog:    c,
						BlockStore: c.BlockAdapter,
						Incr:       func(action, userID, repository, ref string) {},
					},
					Principal: "user-a",
				},
				Repository: repository,
			},
			Reference: branch,
		},
		Path: path,
	})

	require.Equal(t, http.StatusPartialContent, rec.Code)
	require.Equal(t, []byte("lo wor"), rec.Body.Bytes())
}

func testGatewaySerializedXorb(t *testing.T, chunk []byte) (string, []byte) {
	t.Helper()
	chunkHash := xetstore.ComputeDataHash(chunk)
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)

	var b bytes.Buffer
	b.WriteByte(0)
	writeGatewayThreeByteLE(&b, uint32(len(chunk)))
	b.WriteByte(0)
	writeGatewayThreeByteLE(&b, uint32(len(chunk)))
	b.Write(chunk)
	chunkBoundary := uint32(b.Len())

	var footer bytes.Buffer
	footer.WriteString("XETBLOB")
	footer.WriteByte(1)
	testGatewayWriteHash(t, &footer, xorbHash)
	hashSectionOffset := footer.Len()
	footer.WriteString("XBLBHSH")
	footer.WriteByte(0)
	testGatewayWriteU32(&footer, 1)
	testGatewayWriteHash(t, &footer, chunkHash)
	boundarySectionOffset := footer.Len()
	footer.WriteString("XBLBBND")
	footer.WriteByte(1)
	testGatewayWriteU32(&footer, 1)
	testGatewayWriteU32(&footer, chunkBoundary)
	testGatewayWriteU32(&footer, uint32(len(chunk)))
	testGatewayWriteU32(&footer, 1)
	infoLengthWithoutOffsets := footer.Len()
	testGatewayWriteU32(&footer, uint32(infoLengthWithoutOffsets-hashSectionOffset+24))
	testGatewayWriteU32(&footer, uint32(infoLengthWithoutOffsets-boundarySectionOffset+24))
	footer.Write(make([]byte, 16))

	b.Write(footer.Bytes())
	testGatewayWriteU32(&b, uint32(footer.Len()))
	return xorbHash, b.Bytes()
}

func testGatewayXETBinaryShard(t *testing.T, fileHash, xorbHash, chunkHash string, size uint32) []byte {
	t.Helper()
	var b bytes.Buffer
	b.Write([]byte{'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0, 85,
		105, 103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169})
	testGatewayWriteU64(&b, 2)
	testGatewayWriteU64(&b, 200)
	testGatewayWriteHash(t, &b, fileHash)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 1)
	testGatewayWriteU64(&b, 0)
	testGatewayWriteHash(t, &b, xorbHash)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, size)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 1)
	b.Write(bytes.Repeat([]byte{0xff}, 32))
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU64(&b, 0)
	testGatewayWriteHash(t, &b, xorbHash)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 1)
	testGatewayWriteU32(&b, size)
	testGatewayWriteU32(&b, size-2)
	testGatewayWriteHash(t, &b, chunkHash)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, size)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 0)
	b.Write(bytes.Repeat([]byte{0xff}, 32))
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 0)
	testGatewayWriteU32(&b, 0)
	footerOffset := uint64(b.Len())
	testGatewayWriteU64(&b, 1)
	testGatewayWriteU64(&b, 48)
	testGatewayWriteU64(&b, 192)
	testGatewayWriteU64(&b, footerOffset)
	testGatewayWriteU64(&b, 0)
	testGatewayWriteU64(&b, footerOffset)
	testGatewayWriteU64(&b, 0)
	testGatewayWriteU64(&b, footerOffset)
	testGatewayWriteU64(&b, 0)
	b.Write(make([]byte, 32))
	testGatewayWriteU64(&b, 0)
	testGatewayWriteU64(&b, ^uint64(0))
	for i := 0; i < 6; i++ {
		testGatewayWriteU64(&b, 0)
	}
	testGatewayWriteU64(&b, uint64(size-2))
	testGatewayWriteU64(&b, uint64(size))
	testGatewayWriteU64(&b, uint64(size))
	testGatewayWriteU64(&b, footerOffset)
	return b.Bytes()
}

func testGatewayWriteHash(t *testing.T, b *bytes.Buffer, value string) {
	t.Helper()
	raw, err := hex.DecodeString(value)
	require.NoError(t, err)
	require.Len(t, raw, 32)
	for i := 0; i < 4; i++ {
		for j := 7; j >= 0; j-- {
			b.WriteByte(raw[i*8+j])
		}
	}
}

func testGatewayWriteU32(b *bytes.Buffer, value uint32) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func testGatewayWriteU64(b *bytes.Buffer, value uint64) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func writeGatewayThreeByteLE(b *bytes.Buffer, value uint32) {
	b.WriteByte(byte(value))
	b.WriteByte(byte(value >> 8))
	b.WriteByte(byte(value >> 16))
}
