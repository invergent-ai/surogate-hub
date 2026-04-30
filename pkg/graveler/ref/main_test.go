package ref_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/batch"
	"github.com/invergent-ai/surogate-hub/pkg/graveler"
	"github.com/invergent-ai/surogate-hub/pkg/graveler/ref"
	"github.com/invergent-ai/surogate-hub/pkg/ident"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"go.uber.org/ratelimit"
)

var (
	testRepoCacheConfig = ref.CacheConfig{
		Size:   1000,
		Expiry: 20 * time.Millisecond,
	}

	testCommitCacheConfig = ref.CacheConfig{
		Size:   5000,
		Expiry: 20 * time.Millisecond,
	}
)

func testRefManager(t testing.TB) (graveler.RefManager, kv.Store) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               kvStore,
		KVStoreLimited:        kv.NewStoreLimiter(kvStore, ratelimit.NewUnlimited()),
		AddressProvider:       ident.NewHexAddressProvider(),
		RepositoryCacheConfig: testRepoCacheConfig,
		CommitCacheConfig:     testCommitCacheConfig,
	}
	return ref.NewRefManager(cfg), kvStore
}

func testRefManagerWithAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, kv.Store) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               kvStore,
		AddressProvider:       addressProvider,
		RepositoryCacheConfig: testRepoCacheConfig,
		CommitCacheConfig:     testCommitCacheConfig,
	}
	return ref.NewRefManager(cfg), kvStore
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}

	code := m.Run()
	os.Exit(code)
}
