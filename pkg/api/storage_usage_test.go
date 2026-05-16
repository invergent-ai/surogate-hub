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

func TestCreateRepository_InitializesRepoCounter(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName = "alice", "training"
	repoID := owner + "/" + repoName

	resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		Name:             repoID,
		StorageNamespace: onBlock(deps, repoID),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoName))
	require.NoError(t, err, "InitRepo must materialize the repo counter key, not leave it absent")
	require.Equal(t, "0", string(got.Value))
}

func TestDeleteRepository_DropsCounterAndDecrementsUserTotal(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	// Seed a non-zero repo + user counter so we can verify decrement.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoName), []byte("500")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey(owner), []byte("500")))

	resp, err := clt.DeleteRepositoryWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), &apigen.DeleteRepositoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	_, err = deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoName))
	require.ErrorIs(t, err, kv.ErrNotFound)
	require.Equal(t, int64(0), readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

func TestGetUserStorage_SelfReadsOwnCounter(t *testing.T) {
	ctx := context.Background()
	adminClt, deps := setupClientWithAdmin(t, withStorageAccountant())
	aliceClt := clientAs(t, adminClt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("1234")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("1000")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "evals"), []byte("234")))

	resp, err := aliceClt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, int64(1234), resp.JSON200.BytesUsed)
	require.Len(t, resp.JSON200.Repositories, 2)
	require.True(t, resp.JSON200.IsEstimate, "no reconciler pass yet")
}

func TestGetUserStorage_NonSelfRequiresReadUser(t *testing.T) {
	ctx := context.Background()
	adminClt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, adminClt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("100")))
	bobClt := clientAs(t, adminClt, deps, "bob")

	resp, err := bobClt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	// Surogate Hub's authorize() returns 401 for "authenticated but lacks permission" (see
	// TestController_BranchProtectionRules in controller_test.go and authorizeCallback).
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode())
}

func TestGetUserStorage_AdminCanReadOthers(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("100")))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.Equal(t, int64(100), resp.JSON200.BytesUsed)
}

func TestGetUserStorage_IncludesQuotaWhenSet(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("700")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("1000")))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200.QuotaBytes)
	require.Equal(t, int64(1000), *resp.JSON200.QuotaBytes)
	require.NotNil(t, resp.JSON200.BytesRemaining)
	require.Equal(t, int64(300), *resp.JSON200.BytesRemaining)
}

func TestGetUserStorage_RemainingClampsAtZero(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("2000")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("1000")))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200.BytesRemaining)
	require.Equal(t, int64(0), *resp.JSON200.BytesRemaining)
}

func TestGetUserStorage_UnknownUserReturns404(t *testing.T) {
	ctx := context.Background()
	clt, _ := setupClientWithAdmin(t, withStorageAccountant())
	resp, err := clt.GetUserStorageWithResponse(ctx, "ghost")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode())
}

func TestSetUserQuota_AdminCanSet(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")

	resp, err := clt.SetUserQuotaWithResponse(ctx, "alice", apigen.SetUserQuotaJSONRequestBody{QuotaBytes: 12345})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"))
	require.NoError(t, err)
	require.Equal(t, "12345", string(got.Value))
}

func TestSetUserQuota_NegativeRejected(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	resp, err := clt.SetUserQuotaWithResponse(ctx, "alice", apigen.SetUserQuotaJSONRequestBody{QuotaBytes: -1})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
}

func TestSetUserQuota_NonAdminForbidden(t *testing.T) {
	ctx := context.Background()
	adminClt, deps := setupClientWithAdmin(t, withStorageAccountant())
	aliceClt := clientAs(t, adminClt, deps, "alice")
	resp, err := aliceClt.SetUserQuotaWithResponse(ctx, "alice", apigen.SetUserQuotaJSONRequestBody{QuotaBytes: 1000})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode())
}

func TestSetUserQuota_UnknownUserReturns404(t *testing.T) {
	ctx := context.Background()
	clt, _ := setupClientWithAdmin(t, withStorageAccountant())
	resp, err := clt.SetUserQuotaWithResponse(ctx, "ghost", apigen.SetUserQuotaJSONRequestBody{QuotaBytes: 1000})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode())
}

func TestDeleteUserQuota_Removes(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("1000")))

	resp, err := clt.DeleteUserQuotaWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	_, err = deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"))
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestDeleteUserQuota_AbsentIsIdempotent(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, clt, deps, "alice")
	resp, err := clt.DeleteUserQuotaWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())
}

func TestUploadObject_RejectedOverQuota(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	// quota=10, already used 8.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey(owner), []byte("10")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey(owner), []byte("8")))

	status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj1", "hello") // 5 bytes ⇒ 13 > 10
	require.Equal(t, http.StatusRequestEntityTooLarge, status)

	// Counter unchanged because upload rejected before WriteBlob.
	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey(owner))
	require.NoError(t, err)
	require.Equal(t, "8", string(got.Value))
}

func TestUploadObject_AllowedUnderQuota(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	// The HTTP path uses Content-Length of the multipart envelope (which is larger than the file
	// payload itself); use a generous quota so the envelope still fits.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey(owner), []byte("10000")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey(owner), []byte("0")))

	status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj1", "hello world")
	require.Equal(t, http.StatusCreated, status)
}

func TestUploadObject_UnlimitedWhenNoQuotaSet(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
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
