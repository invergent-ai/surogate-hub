package esti

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

func TestXETShardRegistrationDedupProbe(t *testing.T) {
	chunkID := fmt.Sprintf("chunk-%d", time.Now().UnixNano())
	xorbID := fmt.Sprintf("xorb-%d", time.Now().UnixNano())
	shard := fmt.Sprintf("raw-shard-%d", time.Now().UnixNano())
	fileHash := xetShimFileHash(shard)

	putXETXorb(t, xorbID, "xorb-bytes")
	registerXETShard(t, fileHash, shard, []string{chunkID}, []string{xorbID})

	chunkReq, err := http.NewRequest(http.MethodGet, xetRootEndpoint()+"/xet/v1/chunks/default/"+chunkID, nil)
	require.NoError(t, err)
	authorizeXETRequest(t, chunkReq)

	chunkResp, err := http.DefaultClient.Do(chunkReq)
	require.NoError(t, err)
	defer chunkResp.Body.Close()
	require.Equal(t, http.StatusOK, chunkResp.StatusCode)
	require.Equal(t, "application/octet-stream", chunkResp.Header.Get("Content-Type"))

	body, err := io.ReadAll(chunkResp.Body)
	require.NoError(t, err)
	require.Equal(t, []byte(shard), body)
}

func TestXETLinkPhysicalAddress(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	shard := fmt.Sprintf("raw-shard-%d", time.Now().UnixNano())
	fileHash := xetShimFileHash(shard)
	registerXETShard(t, fileHash, shard, nil, nil)

	physicalAddress := "xet://" + fileHash
	resp, err := client.LinkPhysicalAddressWithResponse(ctx, repo, mainBranch, &apigen.LinkPhysicalAddressParams{
		Path: "models/checkpoint.bin",
	}, apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "checksum-a",
		SizeBytes: 9,
		Staging: apigen.StagingLocation{
			PhysicalAddress: &physicalAddress,
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, physicalAddress, resp.JSON200.PhysicalAddress)

	missingAddress := "xet://missing-" + fileHash
	missingResp, err := client.LinkPhysicalAddressWithResponse(ctx, repo, mainBranch, &apigen.LinkPhysicalAddressParams{
		Path: "models/missing.bin",
	}, apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "checksum-a",
		SizeBytes: 9,
		Staging: apigen.StagingLocation{
			PhysicalAddress: &missingAddress,
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, missingResp.StatusCode())
}

func TestXETSmartClientSmoke(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	chunk := []byte("hello world!")
	chunkHash := xetstore.ComputeDataHash(chunk)
	xorbHash, xorbBytes := xetSerializedXorb(t, chunk)
	fileHash, err := xetstore.ComputeFileMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)
	shard := xetBinaryShard(t, fileHash, xorbHash, chunkHash)

	postXETXorb(t, xorbHash, xorbBytes)
	registerXETBinaryShard(t, shard)

	path := "models/smoke-checkpoint.bin"
	physicalAddress := "xet://" + fileHash
	linkResp, err := client.LinkPhysicalAddressWithResponse(ctx, repo, mainBranch, &apigen.LinkPhysicalAddressParams{
		Path: path,
	}, apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  fileHash,
		SizeBytes: int64(len(chunk)),
		Staging: apigen.StagingLocation{
			PhysicalAddress: &physicalAddress,
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, linkResp.StatusCode())

	readResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &apigen.GetObjectParams{Path: path})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, readResp.StatusCode())
	require.Equal(t, chunk, readResp.Body)

	dedupShard := getXETDedupShard(t, chunkHash)
	require.Equal(t, shard, dedupShard)
}

func putXETXorb(t *testing.T, xorbID, body string) {
	t.Helper()
	inserted := postXETXorb(t, xorbID, []byte(body))
	require.True(t, inserted)
}

func postXETXorb(t *testing.T, xorbID string, body []byte) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/xorbs/default/"+xorbID, bytes.NewReader(body))
	require.NoError(t, err)
	authorizeXETRequest(t, req)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		WasInserted bool `json:"was_inserted"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	return result.WasInserted
}

func registerXETShard(t *testing.T, fileHash, shard string, chunkIDs, xorbIDs []string) {
	t.Helper()
	requestBody, err := json.Marshal(map[string]any{
		"file_hash": fileHash,
		"shard":     shard,
		"chunk_ids": chunkIDs,
		"xorb_ids":  xorbIDs,
	})
	require.NoError(t, err)

	registerReq, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/shards", bytes.NewReader(requestBody))
	require.NoError(t, err)
	registerReq.Header.Set("Content-Type", "application/json")
	authorizeXETRequest(t, registerReq)

	registerResp, err := http.DefaultClient.Do(registerReq)
	require.NoError(t, err)
	defer registerResp.Body.Close()
	require.Equal(t, http.StatusOK, registerResp.StatusCode)

	var registerResult struct {
		FileHash    string `json:"file_hash"`
		WasInserted bool   `json:"was_inserted"`
	}
	err = json.NewDecoder(registerResp.Body).Decode(&registerResult)
	require.NoError(t, err)
	require.Equal(t, fileHash, registerResult.FileHash)
	require.True(t, registerResult.WasInserted)
}

func registerXETBinaryShard(t *testing.T, shard []byte) {
	t.Helper()
	registerReq, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/shards", bytes.NewReader(shard))
	require.NoError(t, err)
	registerReq.Header.Set("Content-Type", "application/octet-stream")
	authorizeXETRequest(t, registerReq)

	registerResp, err := http.DefaultClient.Do(registerReq)
	require.NoError(t, err)
	defer registerResp.Body.Close()
	require.Equal(t, http.StatusOK, registerResp.StatusCode)

	var registerResult struct {
		Result int `json:"result"`
	}
	err = json.NewDecoder(registerResp.Body).Decode(&registerResult)
	require.NoError(t, err)
	require.Contains(t, []int{0, 1}, registerResult.Result)
}

func getXETDedupShard(t *testing.T, chunkHash string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, xetRootEndpoint()+"/xet/v1/chunks/default/"+chunkHash, nil)
	require.NoError(t, err)
	authorizeXETRequest(t, req)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body
}

func xetRootEndpoint() string {
	return strings.TrimSuffix(endpointURL, apiutil.BaseURL)
}

func authorizeXETRequest(t *testing.T, req *http.Request) {
	t.Helper()
	req.Header.Set("Authorization", "Bearer "+xetBearerToken(t))
}

func xetBearerToken(t *testing.T) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/token", nil)
	require.NoError(t, err)
	req.SetBasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var token struct {
		AccessToken string `json:"access_token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&token)
	require.NoError(t, err)
	require.NotEmpty(t, token.AccessToken)
	return token.AccessToken
}

func xetShimFileHash(shard string) string {
	sum := sha256.Sum256([]byte(shard))
	return hex.EncodeToString(sum[:])
}

func xetBinaryShard(t *testing.T, fileHash, xorbHash, chunkHash string) []byte {
	t.Helper()
	var b bytes.Buffer
	b.Write([]byte{'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0, 85,
		105, 103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169})
	xetWriteU64(&b, 2)
	xetWriteU64(&b, 200)

	xetWriteHash(t, &b, fileHash)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 1)
	xetWriteU64(&b, 0)

	xetWriteHash(t, &b, xorbHash)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 12)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 1)

	b.Write(bytes.Repeat([]byte{0xff}, 32))
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 0)
	xetWriteU64(&b, 0)

	xetWriteHash(t, &b, xorbHash)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 1)
	xetWriteU32(&b, 12)
	xetWriteU32(&b, 10)

	xetWriteHash(t, &b, chunkHash)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 12)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 0)

	b.Write(bytes.Repeat([]byte{0xff}, 32))
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 0)
	xetWriteU32(&b, 0)

	footerOffset := uint64(b.Len())
	xetWriteU64(&b, 1)
	xetWriteU64(&b, 48)
	xetWriteU64(&b, 192)
	xetWriteU64(&b, footerOffset)
	xetWriteU64(&b, 0)
	xetWriteU64(&b, footerOffset)
	xetWriteU64(&b, 0)
	xetWriteU64(&b, footerOffset)
	xetWriteU64(&b, 0)
	b.Write(make([]byte, 32))
	xetWriteU64(&b, 0)
	xetWriteU64(&b, ^uint64(0))
	for i := 0; i < 6; i++ {
		xetWriteU64(&b, 0)
	}
	xetWriteU64(&b, 10)
	xetWriteU64(&b, 12)
	xetWriteU64(&b, 12)
	xetWriteU64(&b, footerOffset)

	return b.Bytes()
}

func xetSerializedXorb(t *testing.T, chunk []byte) (string, []byte) {
	t.Helper()
	chunkHash := xetstore.ComputeDataHash(chunk)
	xorbHash, err := xetstore.ComputeXorbMerkleHash([]xetstore.ShardChunkInfo{{
		Hash:      chunkHash,
		SizeBytes: uint64(len(chunk)),
	}})
	require.NoError(t, err)

	var b bytes.Buffer
	b.WriteByte(0)
	xetWriteThreeByteLE(&b, uint32(len(chunk)))
	b.WriteByte(0)
	xetWriteThreeByteLE(&b, uint32(len(chunk)))
	b.Write(chunk)

	chunkBoundary := uint32(b.Len())
	b.WriteString("XETBLOB")
	b.WriteByte(1)
	xetWriteHash(t, &b, xorbHash)

	b.WriteString("XBLBHSH")
	b.WriteByte(0)
	xetWriteU32(&b, 1)
	xetWriteHash(t, &b, chunkHash)

	b.WriteString("XBLBBND")
	b.WriteByte(1)
	xetWriteU32(&b, 1)
	xetWriteU32(&b, chunkBoundary)
	xetWriteU32(&b, uint32(len(chunk)))
	xetWriteU32(&b, 1)
	xetWriteU32(&b, 92)
	xetWriteU32(&b, 48)
	b.Write(make([]byte, 16))
	xetWriteU32(&b, 132)

	return xorbHash, b.Bytes()
}

func xetWriteHash(t *testing.T, b *bytes.Buffer, value string) {
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

func xetWriteU32(b *bytes.Buffer, value uint32) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func xetWriteU64(b *bytes.Buffer, value uint64) {
	_ = binary.Write(b, binary.LittleEndian, value)
}

func xetWriteThreeByteLE(b *bytes.Buffer, value uint32) {
	b.WriteByte(byte(value))
	b.WriteByte(byte(value >> 8))
	b.WriteByte(byte(value >> 16))
}
