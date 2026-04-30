package esti

import (
	"bytes"
	"crypto/sha256"
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
	chunkReq.SetBasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

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

func putXETXorb(t *testing.T, xorbID, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/xorbs/default/"+xorbID, strings.NewReader(body))
	require.NoError(t, err)
	req.SetBasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		WasInserted bool `json:"was_inserted"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	require.True(t, result.WasInserted)
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
	registerReq.SetBasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

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

func xetRootEndpoint() string {
	return strings.TrimSuffix(endpointURL, apiutil.BaseURL)
}

func xetShimFileHash(shard string) string {
	sum := sha256.Sum256([]byte(shard))
	return hex.EncodeToString(sum[:])
}
