package esti

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/api/apiutil"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestXETSmartClientSmoke(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	chunk := []byte("hello world!")
	xorbHash, xorbBytes := xettest.SerializedXorb(t, chunk)
	fileHash := xettest.FileHash(t, chunk)

	path := "models/smoke-checkpoint.bin"
	token := xetToken(t, "write", repo, mainBranch, path)
	postXETXorb(t, token, xorbHash, xorbBytes)
	registerXETShard(t, token, xettest.Shard(t, fileHash, xorbHash, 0, 1, uint32(len(chunk))))

	physicalAddress := "xet://" + fileHash
	linkResp, err := client.LinkPhysicalAddressWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), mainBranch, &apigen.LinkPhysicalAddressParams{
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

	readResp, err := client.GetObjectWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), mainBranch, &apigen.GetObjectParams{Path: path})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, readResp.StatusCode())
	require.Equal(t, chunk, readResp.Body)

	missingAddress := "xet://" + xettest.FileHash(t, []byte("never uploaded"))
	missingResp, err := client.LinkPhysicalAddressWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), mainBranch, &apigen.LinkPhysicalAddressParams{
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

func postXETXorb(t *testing.T, token, xorbID string, body []byte) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/xorbs/default/"+xorbID, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

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

func registerXETShard(t *testing.T, token string, shard []byte) {
	t.Helper()
	registerReq, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/shards", bytes.NewReader(shard))
	require.NoError(t, err)
	registerReq.Header.Set("Content-Type", "application/octet-stream")
	registerReq.Header.Set("Authorization", "Bearer "+token)

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

func xetRootEndpoint() string {
	return strings.TrimSuffix(endpointURL, apiutil.BaseURL)
}

func xetToken(t *testing.T, mode, repo, ref, path string) string {
	t.Helper()
	query := url.Values{"mode": {mode}, "repository": {repo}, "ref": {ref}, "path": {path}}
	req, err := http.NewRequest(http.MethodPost, xetRootEndpoint()+"/xet/v1/token?"+query.Encode(), nil)
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
