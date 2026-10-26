package api_test

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
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

// seedXETFile stores a xet file of len(content) bytes outside every repository and returns its hash.
func seedXETFile(t *testing.T, ctx context.Context, deps *dependencies, content string) string {
	t.Helper()
	fileHash, _, _ := xettest.SeedFile(t, ctx, xetstore.NewRegistry(deps.kvStore), deps.blocks, "mem://_hub_xet", []byte(content))
	return fileHash
}

func linkXETFile(t *testing.T, ctx context.Context, clt apigen.ClientWithResponsesInterface, repoID, branch, path, fileHash string, size int64) int {
	t.Helper()
	physicalAddress := "xet://" + fileHash
	resp, err := clt.LinkPhysicalAddressWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), branch, &apigen.LinkPhysicalAddressParams{Path: path}, apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "checksum-" + path,
		SizeBytes: size,
		Staging:   apigen.StagingLocation{PhysicalAddress: &physicalAddress},
	})
	require.NoError(t, err)
	return resp.StatusCode()
}

// TestStorageUsage_LinkXETOverQuotaLeavesNoState verifies that linking a xet file larger than the
// owner's remaining quota is rejected before anything is written.
func TestStorageUsage_LinkXETOverQuotaLeavesNoState(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	content := "twelve bytes"
	fileHash := seedXETFile(t, ctx, deps, content)
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey(owner), []byte("10")))

	status := linkXETFile(t, ctx, clt, repoID, branch, "model.bin", fileHash, int64(len(content)))
	require.Equal(t, http.StatusRequestEntityTooLarge, status)

	_, getErr := deps.catalog.GetEntry(ctx, repoID, branch, "model.bin", catalog.GetEntryParams{})
	require.Error(t, getErr, "rejected link must not produce a catalog entry")
	held, err := xetstore.NewRegistry(deps.kvStore).HasFileRef(ctx, repoID, fileHash)
	require.NoError(t, err)
	require.False(t, held, "rejected link must not leave a file ref")
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, int64(0), readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
}

// TestStorageUsage_LinkXETChargesEachRepositoryOnce verifies that a repository is charged a xet
// file's size once however many paths point at it, that a held file needs no quota headroom to
// link again, and that a second repository holding the same file is charged too.
func TestStorageUsage_LinkXETChargesEachRepositoryOnce(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoA, repoB, branch = "alice", "training", "evals", "main"
	content := "twelve bytes"
	size := int64(len(content))
	fileHash := seedXETFile(t, ctx, deps, content)
	for _, repoName := range []string{repoA, repoB} {
		repoID := owner + "/" + repoName
		_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
		require.NoError(t, err)
	}
	// Room for exactly one copy of the file, so a second charge would be rejected.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey(owner), []byte(strconv.FormatInt(size, 10))))

	repoAID := owner + "/" + repoA
	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, repoAID, branch, "one.bin", fileHash, size))
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, repoAID, branch, "two.bin", fileHash, size))
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoA)))
	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))

	// The second repository is charged for its own copy, so lift the quota first.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey(owner), []byte(strconv.FormatInt(2*size, 10))))
	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, owner+"/"+repoB, branch, "one.bin", fileHash, size))
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoB)))
	require.Equal(t, 2*size, readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

// TestStorageUsage_LinkXETFailedEntryWriteStaysChargedOnce verifies that a link whose entry write
// fails keeps its file ref and charge, and that linking the same file again adds nothing.
func TestStorageUsage_LinkXETFailedEntryWriteStaysChargedOnce(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	content := "twelve bytes"
	size := int64(len(content))
	fileHash := seedXETFile(t, ctx, deps, content)
	require.NoError(t, deps.catalog.CreateEntry(ctx, repoID, branch, catalog.DBEntry{Path: "model.bin", PhysicalAddress: onBlock(deps, "existing"), CreationDate: time.Now(), Size: 1, Checksum: "existing"}))

	physicalAddress := "xet://" + fileHash
	ifNoneMatch := apigen.IfNoneMatch("*")
	resp, err := clt.LinkPhysicalAddressWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), branch, &apigen.LinkPhysicalAddressParams{Path: "model.bin", IfNoneMatch: &ifNoneMatch}, apigen.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "checksum-model.bin",
		SizeBytes: size,
		Staging:   apigen.StagingLocation{PhysicalAddress: &physicalAddress},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode())
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))

	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, repoID, branch, "other.bin", fileHash, size))
	require.NoError(t, deps.storageAccountant.Flush(ctx))
	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
	require.Equal(t, size, readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

// TestStorageUsage_ReconcilerCountsXETFiles verifies that the reconciler, which rewrites each
// repo counter from scratch, still includes the xet files the repo holds.
func TestStorageUsage_ReconcilerCountsXETFiles(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	content := "twelve bytes"
	fileHash := seedXETFile(t, ctx, deps, content)
	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, repoID, branch, "model.bin", fileHash, int64(len(content))))

	require.NoError(t, deps.storageReconciler.RunOnce(ctx))

	require.Equal(t, int64(len(content)), readInt64KV(t, deps.kvStore, stats.StorageRepoKey(owner, repoName)))
	require.Equal(t, int64(len(content)), readInt64KV(t, deps.kvStore, stats.StorageUserKey(owner)))
}

// TestDeleteRepository_RemovesXETFileRefs verifies that deleting a repository drops the file refs
// it held, so a repository recreated under the same name starts without them.
func TestDeleteRepository_RemovesXETFileRefs(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	const owner, repoName, branch = "alice", "training", "main"
	repoID := owner + "/" + repoName
	_, err := deps.catalog.CreateRepository(ctx, repoID, "", onBlock(deps, repoID), branch, false)
	require.NoError(t, err)
	content := "twelve bytes"
	fileHash := seedXETFile(t, ctx, deps, content)
	require.Equal(t, http.StatusOK, linkXETFile(t, ctx, clt, repoID, branch, "model.bin", fileHash, int64(len(content))))
	registry := xetstore.NewRegistry(deps.kvStore)
	held, err := registry.HasFileRef(ctx, repoID, fileHash)
	require.NoError(t, err)
	require.True(t, held)

	resp, err := clt.DeleteRepositoryWithResponse(ctx, apigen.RepositoryOwner(repoID), apigen.RepositoryName(repoID), &apigen.DeleteRepositoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	held, err = registry.HasFileRef(ctx, repoID, fileHash)
	require.NoError(t, err)
	require.False(t, held)
	xetBytes, err := registry.RepoXetBytes(ctx, repoID)
	require.NoError(t, err)
	require.Equal(t, int64(0), xetBytes)
}

// TestCreateRepository_DropsStaleXETFileRefs verifies that a repository created under the name of
// a deleted one starts without file refs the delete failed to remove.
func TestCreateRepository_DropsStaleXETFileRefs(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	registry := xetstore.NewRegistry(deps.kvStore)
	for _, bare := range []bool{false, true} {
		repoID := "alice/training"
		if bare {
			repoID = "alice/training-bare"
		}
		t.Run("bare="+strconv.FormatBool(bare), func(t *testing.T) {
			created, err := registry.AddFileRef(ctx, xetstore.FileRef{FileHash: "stale-file", Repo: repoID, SizeBytes: 12})
			require.NoError(t, err)
			require.True(t, created)

			resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{Bare: &bare}, apigen.CreateRepositoryJSONRequestBody{
				Name:             repoID,
				StorageNamespace: onBlock(deps, repoID),
			})
			verifyResponseOK(t, resp, err)

			held, err := registry.HasFileRef(ctx, repoID, "stale-file")
			require.NoError(t, err)
			require.False(t, held)
			xetBytes, err := registry.RepoXetBytes(ctx, repoID)
			require.NoError(t, err)
			require.Equal(t, int64(0), xetBytes)
		})
	}
}

// Sentinel to keep the imports honest if a future edit drops one.
var _ = errors.New
