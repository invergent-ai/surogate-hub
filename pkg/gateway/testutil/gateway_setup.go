package testutil

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/auth/model"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/gateway"
	"github.com/invergent-ai/surogate-hub/pkg/gateway/multipart"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	_ "github.com/invergent-ai/surogate-hub/pkg/kv/mem"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/testutil"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	"github.com/spf13/viper"
)

type Dependencies struct {
	blocks            block.Adapter
	auth              *FakeAuthService
	catalog           *catalog.Catalog
	KVStore           kv.Store
	StorageAccountant *stats.StorageAccountant
	QuotaChecker      *stats.QuotaChecker
}

// SetupOption mutates the dependencies before the gateway handler is constructed. Use
// WithStorageAccountant to opt-in to per-user byte counting and quota enforcement during tests.
type SetupOption func(*Dependencies)

// WithStorageAccountant installs a real StorageAccountant and QuotaChecker on the dependencies so
// the gateway handler will count uploads and enforce quotas.
func WithStorageAccountant() SetupOption {
	return func(d *Dependencies) {
		d.StorageAccountant = stats.NewStorageAccountant(d.KVStore)
		d.QuotaChecker = stats.NewQuotaChecker(d.KVStore)
	}
}

func GetBasicHandler(t *testing.T, authService *FakeAuthService, repoName string, opts ...SetupOption) (http.Handler, *Dependencies) {
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)

	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	testutil.MustDo(t, "open kv store", err)
	t.Cleanup(store.Close)
	multipartTracker := multipart.NewTracker(store)

	blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	blockAdapter := testutil.NewBlockAdapterByType(t, blockstoreType)

	conf := &config.BaseConfig{}
	conf, err = config.NewConfig("", conf)
	testutil.MustDo(t, "config", err)

	c, err := catalog.New(ctx, catalog.Config{
		Config:       conf,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)
	t.Cleanup(func() {
		_ = c.Close()
	})

	storageNamespace := os.Getenv("USE_STORAGE_NAMESPACE")
	if storageNamespace == "" {
		storageNamespace = "replay"
	}

	_, err = c.CreateRepository(ctx, repoName, "", storageNamespace, "main", false)
	testutil.Must(t, err)

	deps := &Dependencies{
		blocks:  blockAdapter,
		auth:    authService,
		catalog: c,
		KVStore: store,
	}
	for _, opt := range opts {
		opt(deps)
	}

	handler := gateway.NewHandler(authService.Region, c, multipartTracker, blockAdapter, authService, []string{authService.BareDomain}, &stats.NullCollector{}, upload.DefaultPathProvider, nil, config.DefaultLoggingAuditLogLevel, true, false, false, deps.StorageAccountant, deps.QuotaChecker)

	return handler, deps
}

type FakeAuthService struct {
	BareDomain      string `json:"bare_domain"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"access_secret_key"`
	UserID          string `json:"user_id"`
	Region          string `json:"region"`
}

func (m *FakeAuthService) GetCredentials(_ context.Context, accessKey string) (*model.Credential, error) {
	if accessKey != m.AccessKeyID {
		logging.ContextUnavailable().Fatal("access key in recording different than configuration")
	}
	aCred := new(model.Credential)
	aCred.AccessKeyID = accessKey
	aCred.SecretAccessKey = m.SecretAccessKey
	aCred.Username = m.UserID
	return aCred, nil
}

func (m *FakeAuthService) GetUser(_ context.Context, _ string) (*model.User, error) {
	return &model.User{
		CreatedAt: time.Now(),
		Username:  "user",
	}, nil
}

func (m *FakeAuthService) Authorize(_ context.Context, _ *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{Allowed: true}, nil
}
