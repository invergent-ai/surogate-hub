package esti

import (
	"bytes"
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
	fileHash := fmt.Sprintf("file-%d", time.Now().UnixNano())
	chunkID := fmt.Sprintf("chunk-%d", time.Now().UnixNano())
	shard := fmt.Sprintf("raw-shard-%d", time.Now().UnixNano())

	registerXETShard(t, fileHash, shard, []string{chunkID})

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

	fileHash := fmt.Sprintf("file-%d", time.Now().UnixNano())
	registerXETShard(t, fileHash, "raw-shard", nil)

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

func registerXETShard(t *testing.T, fileHash, shard string, chunkIDs []string) {
	t.Helper()
	requestBody, err := json.Marshal(map[string]any{
		"file_hash": fileHash,
		"shard":     shard,
		"chunk_ids": chunkIDs,
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
