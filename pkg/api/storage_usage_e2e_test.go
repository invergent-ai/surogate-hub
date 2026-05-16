package api_test

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/stretchr/testify/require"
)

// TestStorageUsage_EndToEnd uploads several objects, then verifies that GET /auth/users/{userId}/storage
// reflects the accumulated byte count via both the per-user total and the per-repo breakdown.
func TestStorageUsage_EndToEnd(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	_ = clientAs(t, clt, deps, owner) // GetUserStorage verifies the owner user exists.
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	expected := int64(0)
	for i, body := range []string{"aa", "bbbb", "cccccc"} {
		status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj-"+strconv.Itoa(i), body)
		require.Equal(t, http.StatusCreated, status)
		expected += int64(len(body))
	}
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	// Through the public API: GET /auth/users/alice/storage.
	resp, err := clt.GetOwnerStorageWithResponse(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.Equal(t, expected, resp.JSON200.BytesUsed)
	require.Len(t, resp.JSON200.Repositories, 1)
	require.Equal(t, repoName, resp.JSON200.Repositories[0].Name)
	require.Equal(t, expected, resp.JSON200.Repositories[0].BytesUsed)
	require.True(t, resp.JSON200.IsEstimate, "reconciler has not run yet")
}

// TestStorageUsage_ReconcilerCorrectsDrift seeds an intentionally-wrong counter, runs the
// reconciler against an empty namespace, and verifies the counter is overwritten to the truth.
func TestStorageUsage_ReconcilerCorrectsDrift(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)

	// Seed a deliberately-wrong repo counter — bytes_used pretends 999999 bytes are stored.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoName), []byte("999999")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey(owner), []byte("999999")))

	require.NoError(t, deps.storageReconciler.RunOnce(ctx))

	// Empty namespace ⇒ reconciler should overwrite the repo counter to 0.
	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoName))
	require.NoError(t, err)
	require.Equal(t, "0", string(got.Value))

	// User total recomputed from sum of repo counters.
	gotUser, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey(owner))
	require.NoError(t, err)
	require.Equal(t, "0", string(gotUser.Value))

	// last_reconciled_at was written.
	gotMeta, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey(owner))
	require.NoError(t, err)
	require.NotEmpty(t, gotMeta.Value)
}

// TestStorageUsage_RepoDeleteDecrementsUserTotal verifies the end-to-end repo-delete flow drops
// the per-repo counter and decrements the per-user total by exactly the repo's bytes.
func TestStorageUsage_RepoDeleteDecrementsUserTotal(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoA, repoB, branch = "alice", "training", "evals", "main"
	_ = clientAs(t, clt, deps, owner)

	for _, repoName := range []string{repoA, repoB} {
		repoID := owner + "/" + repoName
		_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
		require.NoError(t, err)
		status := uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj1", repoName) // body == repo name
		require.Equal(t, http.StatusCreated, status)
	}
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	repoABytes := int64(len(repoA))
	repoBBytes := int64(len(repoB))

	// Sanity: per-user counter is the sum of both repo counters.
	gotUser, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey(owner))
	require.NoError(t, err)
	n, _ := strconv.ParseInt(string(gotUser.Value), 10, 64)
	require.Equal(t, repoABytes+repoBBytes, n)

	// Delete repo A via the HTTP API.
	repoIDA := owner + "/" + repoA
	resp, err := clt.DeleteRepositoryWithResponse(ctx, apigen.RepositoryOwner(repoIDA), apigen.RepositoryName(repoIDA), &apigen.DeleteRepositoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	// Repo A's key is gone.
	_, err = deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey(owner, repoA))
	require.ErrorIs(t, err, kv.ErrNotFound)

	// User total decremented by exactly repoABytes.
	gotUser2, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey(owner))
	require.NoError(t, err)
	n2, _ := strconv.ParseInt(string(gotUser2.Value), 10, 64)
	require.Equal(t, repoBBytes, n2)
}

// TestStorageUsage_MultiRepoBreakdown verifies that GetUserStorage returns one entry per repo
// owned by the user, with the per-repo byte counts.
func TestStorageUsage_MultiRepoBreakdown(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, branch = "alice", "main"
	_ = clientAs(t, clt, deps, owner)
	repos := map[string]string{
		"training": "training-data",
		"evals":    "evals-data-much-bigger",
	}
	for repoName, body := range repos {
		repoID := owner + "/" + repoName
		_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, uploadObjectMultipart(t, ctx, clt, repoID, branch, "obj", body))
	}
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	resp, err := clt.GetOwnerStorageWithResponse(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.Len(t, resp.JSON200.Repositories, 2)
	byRepo := map[string]int64{}
	for _, r := range resp.JSON200.Repositories {
		byRepo[r.Name] = r.BytesUsed
	}
	for repoName, body := range repos {
		require.Equal(t, int64(len(body)), byRepo[repoName], "repo %s", repoName)
	}
}

// TestStorageUsage_AfterReconcilerIsEstimateFalse verifies that a successful reconciler pass sets
// last_reconciled_at, so the API response flips is_estimate to false.
func TestStorageUsage_AfterReconcilerIsEstimateFalse(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	_ = clientAs(t, clt, deps, owner)
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	require.NoError(t, deps.storageReconciler.RunOnce(ctx))

	resp, err := clt.GetOwnerStorageWithResponse(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.False(t, resp.JSON200.IsEstimate)
	require.NotNil(t, resp.JSON200.LastReconciledAt)
}

// Sentinel to keep the imports honest if a future edit drops one.
var _ = errors.New
