package api_test

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/stretchr/testify/require"
)

// uploadObjectMultipart uploads `content` as a multipart/form-data field named "content" to the
// given path. `repoID` must be the full "owner/repo" string the catalog was created with; tests
// pass it as both URL path parameters per the existing controller_test.go convention.
// Returns the response status code.
func uploadObjectMultipart(t *testing.T, ctx context.Context, clt apigen.ClientWithResponsesInterface, repoID, branch, path, content string) int {
	t.Helper()
	contentType, buf := writeMultipart("content", "file", content)
	b, err := clt.UploadObjectWithBodyWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), branch, &apigen.UploadObjectParams{Path: path}, contentType, buf)
	require.NoError(t, err)
	return b.StatusCode()
}

func readInt64KV(t *testing.T, store kv.Store, key []byte) int64 {
	t.Helper()
	got, err := store.Get(context.Background(), stats.StoragePartition, key)
	if errors.Is(err, kv.ErrNotFound) {
		return 0
	}
	require.NoError(t, err)
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	require.NoError(t, err)
	return n
}

func TestUploadObject_IncrementsStorageCounter(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj1", "hello world")
	require.Equal(t, http.StatusCreated, status)

	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, int64(11), readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
	require.Equal(t, int64(11), readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

func TestUploadObject_AccumulatesAcrossUploads(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	for _, body := range []string{"aa", "bbbb", "cccccc"} {
		status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj-"+body, body)
		require.Equal(t, http.StatusCreated, status)
	}
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, int64(12), readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
	require.Equal(t, int64(12), readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

func TestUploadObject_NoAccountantDoesNotPanic(t *testing.T) {
	// Default fixture without withStorageAccountant must still allow uploads.
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t)
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj1", "hello world")
	require.Equal(t, http.StatusCreated, status)
}

func TestCopyObject_DoesNotDoubleCount(t *testing.T) {
	// CopyObject reuses the source physical address inside the same repo, so the per-repo
	// allocated-bytes counter must not increase.
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "src", "hello world")
	require.Equal(t, http.StatusCreated, status)
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	beforeCopy := readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName))
	require.Equal(t, int64(11), beforeCopy)

	resp, err := clt.CopyObjectWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), branch, &apigen.CopyObjectParams{DestPath: "dst"}, apigen.CopyObjectJSONRequestBody{
		SrcPath: "src",
		SrcRef:  swag.String(branch),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, beforeCopy, readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
}
