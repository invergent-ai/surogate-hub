package operations

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/gateway/multipart"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// gatewayTestStore returns a kv.Store, accountant, catalog, and *PathOperation pre-wired against a
// single in-memory store, so tests can both drive the gateway handler and read the resulting
// counters from the same place.
func gatewayTestStore(t *testing.T, owner, name string, withAccountant bool) (kv.Store, *stats.StorageAccountant, *PathOperation) {
	t.Helper()
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.BaseConfig{}
	cfg, err := config.NewConfig("", cfg)
	require.NoError(t, err)
	store := kvtest.GetStore(ctx, t)
	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	fullRepo := owner + "/" + name
	_, err = c.CreateRepository(ctx, fullRepo, "", "mem://"+fullRepo, "main", false)
	require.NoError(t, err)
	repo, err := c.GetRepository(ctx, fullRepo)
	require.NoError(t, err)

	var accountant *stats.StorageAccountant
	if withAccountant {
		accountant = stats.NewStorageAccountant(store)
	}

	op := &PathOperation{
		RefOperation: &RefOperation{
			RepoOperation: &RepoOperation{
				AuthorizedOperation: &AuthorizedOperation{Operation: &Operation{
					Region:            "us-east-1",
					Catalog:           c,
					BlockStore:        c.BlockAdapter,
					MultipartTracker:  multipart.NewTracker(store),
					Incr:              func(action, userID, repository, ref string) {},
					PathProvider:      upload.DefaultPathProvider,
					StorageAccountant: accountant,
				}, Principal: "user-a"},
				Repository: repo,
			},
			Reference: "main",
		},
		Path: "obj",
	}
	return store, accountant, op
}

func readGatewayCounter(t *testing.T, store kv.Store, key []byte) int64 {
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

func putPayload(t *testing.T, op *PathOperation, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/"+op.Repository.Name+"/main/"+op.Path, bytes.NewReader(payload))
	req.ContentLength = int64(len(payload))
	handlePut(rec, req, op)
	return rec
}

func TestGatewayHandlePut_IncrementsStorageCounter(t *testing.T) {
	const owner, name = "alice", "training"
	store, accountant, op := gatewayTestStore(t, owner, name, true)

	payload := []byte("hello gateway")
	rec := putPayload(t, op, payload)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())

	require.NoError(t, accountant.Flush(context.Background()))
	require.Equal(t, int64(len(payload)), readGatewayCounter(t, store, stats.StorageRepoKey(owner, name)))
	require.Equal(t, int64(len(payload)), readGatewayCounter(t, store, stats.StorageUserKey(owner)))
}

func TestGatewayHandlePut_AccumulatesAcrossPuts(t *testing.T) {
	const owner, name = "alice", "training"
	store, accountant, op := gatewayTestStore(t, owner, name, true)

	var total int64
	for i, payload := range [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")} {
		op.Path = "obj-" + strconv.Itoa(i)
		rec := putPayload(t, op, payload)
		require.Equal(t, http.StatusOK, rec.Code)
		total += int64(len(payload))
	}
	require.NoError(t, accountant.Flush(context.Background()))
	require.Equal(t, total, readGatewayCounter(t, store, stats.StorageRepoKey(owner, name)))
	require.Equal(t, total, readGatewayCounter(t, store, stats.StorageUserKey(owner)))
}

func TestGatewayHandlePut_NoAccountantStillSucceeds(t *testing.T) {
	const owner, name = "alice", "training"
	_, _, op := gatewayTestStore(t, owner, name, false)
	rec := putPayload(t, op, []byte("hello"))
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
}

func TestGatewayHandlePut_RejectsOverQuota(t *testing.T) {
	const owner, name = "alice", "training"
	store, accountant, op := gatewayTestStore(t, owner, name, true)
	// Set quota=8, usage=5, then attempt to upload 10 more bytes (5+10 > 8).
	ctx := context.Background()
	quoter := stats.NewQuotaChecker(store)
	require.NoError(t, quoter.SetQuota(ctx, owner, 8))
	require.NoError(t, store.Set(ctx, stats.StoragePartition, stats.StorageUserKey(owner), []byte("5")))
	// Replace the operation's QuotaChecker so it reads from the same store.
	op.QuotaChecker = quoter

	rec := putPayload(t, op, []byte("0123456789"))
	require.Equal(t, http.StatusBadRequest, rec.Code, "expected 4xx quota error, body=%s", rec.Body.String())
	// Verify nothing landed in the counter (no flush either since the request was rejected).
	require.NoError(t, accountant.Flush(ctx))
	require.Equal(t, int64(5), readGatewayCounter(t, store, stats.StorageUserKey(owner)))
}
