package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/go-openapi/swag"
	"github.com/invergent-ai/surogate-hub/pkg/actions"
	"github.com/invergent-ai/surogate-hub/pkg/api"
	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/api/apiutil"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/auth/acl"
	"github.com/invergent-ai/surogate-hub/pkg/auth/crypt"
	authmodel "github.com/invergent-ai/surogate-hub/pkg/auth/model"
	authparams "github.com/invergent-ai/surogate-hub/pkg/auth/params"
	"github.com/invergent-ai/surogate-hub/pkg/authentication"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/cache"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/graveler/settings"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/kv/mem"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/stats/storagewiring"
	"github.com/invergent-ai/surogate-hub/pkg/testutil"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	"github.com/invergent-ai/surogate-hub/pkg/version"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	ServerTimeout = 30 * time.Second
)

type dependencies struct {
	blocks            block.Adapter
	catalog           *catalog.Catalog
	authService       auth.Service
	collector         *memCollector
	server            *httptest.Server
	kvStore           kv.Store
	storageAccountant *stats.StorageAccountant
	quotaChecker      *stats.QuotaChecker
	storageReconciler *stats.StorageReconciler
}

// setupOption tweaks the dependencies before api.Serve is constructed.
type setupOption func(*dependencies)

// emptyNamespaceAdapter reports every storage namespace as empty. It stands in for a walkable
// block adapter, because the mem block adapter does not support GetWalker.
type emptyNamespaceAdapter struct {
	block.Adapter
}

func (emptyNamespaceAdapter) GetWalker(string, block.WalkerOptions) (block.Walker, error) {
	return emptyWalker{}, nil
}

type emptyWalker struct{}

func (emptyWalker) Walk(context.Context, *url.URL, block.WalkOptions, func(block.ObjectStoreEntry) error) error {
	return nil
}

func (emptyWalker) Marker() block.Mark { return block.Mark{} }

func (emptyWalker) GetSkippedEntries() []block.ObjectStoreEntry { return nil }

// withStorageAccountant installs a real StorageAccountant, QuotaChecker, and StorageReconciler on
// the test dependencies, so the controller will count uploads, enforce quotas, and the test can
// drive RunOnce to verify drift correction. The reconciler sees every storage namespace as empty,
// so the only bytes it counts are the xet files the repositories hold.
func withStorageAccountant() setupOption {
	return func(d *dependencies) {
		d.storageAccountant = stats.NewStorageAccountant(d.kvStore)
		d.quotaChecker = stats.NewQuotaChecker(d.kvStore)
		d.storageReconciler = stats.NewStorageReconciler(
			d.kvStore,
			&storagewiring.CatalogRepoLister{Catalog: d.catalog},
			&storagewiring.RepoSizer{Adapter: emptyNamespaceAdapter{d.blocks}, XET: xetstore.NewRegistry(d.kvStore)},
			d.storageAccountant,
		)
	}
}

// memCollector in-memory collector stores events and metadata sent
type memCollector struct {
	Metrics        []*stats.Metric
	Metadata       []*stats.Metadata
	InstallationID string
	mu             sync.Mutex
}

func (m *memCollector) CollectEvents(ev stats.Event, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metrics = append(m.Metrics, &stats.Metric{Event: ev, Value: count})
}

func (m *memCollector) CollectEvent(ev stats.Event) {
	m.CollectEvents(ev, 1)
}

func (m *memCollector) CollectMetadata(accountMetadata *stats.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metadata = append(m.Metadata, accountMetadata)
}

func (m *memCollector) SetInstallationID(installationID string) {
	m.InstallationID = installationID
}

func (m *memCollector) CollectCommPrefs(_ stats.CommPrefs) {
}

func (m *memCollector) Close() {}

func createDefaultAdminUser(t testing.TB, clt apigen.ClientWithResponsesInterface) *authmodel.BaseCredential {
	t.Helper()
	res, err := clt.SetupWithResponse(context.Background(), apigen.SetupJSONRequestBody{
		Username: "admin",
	})
	testutil.Must(t, err)
	if res.JSON200 == nil {
		t.Fatal("Failed run setup env", res.HTTPResponse.StatusCode, res.HTTPResponse.Status)
	}
	return &authmodel.BaseCredential{
		IssuedDate:      time.Unix(res.JSON200.CreationDate, 0),
		AccessKeyID:     res.JSON200.AccessKeyId,
		SecretAccessKey: res.JSON200.SecretAccessKey,
	}
}

func createUserWithDefaultGroup(t testing.TB, clt apigen.ClientWithResponsesInterface) *authmodel.BaseCredential {
	t.Helper()
	// create the user
	createUsrRes, err := clt.CreateUserWithResponse(context.Background(), apigen.CreateUserJSONRequestBody{
		Id:         "test@example.com",
		InviteUser: swag.Bool(false),
	})
	testutil.Must(t, err)
	if createUsrRes.JSON201 == nil {
		t.Fatal("Failed to create user", createUsrRes.HTTPResponse.StatusCode, createUsrRes.HTTPResponse.Status)
	}

	// create credentials for the user
	createCredsRes, err := clt.CreateCredentialsWithResponse(context.Background(), createUsrRes.JSON201.Id)
	testutil.Must(t, err)
	if createCredsRes.JSON201 == nil {
		t.Fatal("Failed to create credentials", createCredsRes.HTTPResponse.StatusCode, createCredsRes.HTTPResponse.Status)
	}

	return &authmodel.BaseCredential{
		IssuedDate:      time.Unix(createCredsRes.JSON201.CreationDate, 0),
		AccessKeyID:     createCredsRes.JSON201.AccessKeyId,
		SecretAccessKey: createCredsRes.JSON201.SecretAccessKey,
	}
}

func setupHandler(t testing.TB, opts ...setupOption) (http.Handler, *dependencies) {
	t.Helper()
	ctx := context.Background()

	if viper.Get(config.BlockstoreTypeKey) == nil {
		viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	}
	viper.Set("database.type", mem.DriverName)
	viper.Set("committed.local_cache.dir", t.TempDir())
	viper.Set("auth.encrypt.secret_key", "some secret")

	collector := &memCollector{}
	cfg := &config.BaseConfig{}
	cfg, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)
	kvStore := kvtest.GetStore(ctx, t)
	actionsStore := actions.NewActionsKVStore(kvStore)
	idGen := &actions.DecreasingIDGenerator{}
	authService := acl.NewAuthService(kvStore, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	})
	meta := auth.NewKVMetadataManager("serve_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore)

	// Do not validate invalid config (missing required fields).
	c, err := catalog.New(ctx, catalog.Config{
		Config:                cfg,
		KVStore:               kvStore,
		SettingsManagerOption: settings.WithCache(cache.NoCache),
		PathProvider:          upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)

	// wire actions
	actionsConfig := actions.Config{Enabled: true}
	actionsConfig.Lua.NetHTTPEnabled = true
	actionsService := actions.NewService(
		ctx,
		actionsStore,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		idGen,
		collector,
		actionsConfig,
		"",
	)

	c.SetHooksHandler(actionsService)

	authenticator := auth.NewBuiltinAuthenticator(authService)
	kvParams, err := kvparams.NewConfig(&cfg.Database)
	testutil.Must(t, err)
	migrator := kv.NewDatabaseMigrator(kvParams)

	t.Cleanup(func() {
		actionsService.Stop()
		_ = c.Close()
	})

	auditChecker := version.NewDefaultAuditChecker(cfg.Security.AuditCheckURL, "", nil)

	authenticationService := authentication.NewDummyService()

	deps := &dependencies{
		blocks:      c.BlockAdapter,
		authService: authService,
		catalog:     c,
		collector:   collector,
		kvStore:     kvStore,
	}
	for _, opt := range opts {
		opt(deps)
	}

	handler := api.Serve(cfg, c, authenticator, authService, authenticationService, c.BlockAdapter, meta, migrator, collector, nil, actionsService, auditChecker, logging.ContextUnavailable(), nil, upload.DefaultPathProvider, stats.DefaultUsageReporter, deps.storageAccountant, deps.quotaChecker)
	return handler, deps
}

func setupClientByEndpoint(t testing.TB, endpointURL string, accessKeyID, secretAccessKey string, opts ...apigen.ClientOption) apigen.ClientWithResponsesInterface {
	t.Helper()

	if accessKeyID != "" {
		basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
		if err != nil {
			t.Fatal("basic auth security provider", err)
		}
		opts = append(opts, apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
	}
	clt, err := apigen.NewClientWithResponses(endpointURL+apiutil.BaseURL, opts...)
	if err != nil {
		t.Fatal("failed to create hub api client:", err)
	}
	return clt
}

func setupServer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()
	if shouldUseServerTimeout() {
		handler = http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func shouldUseServerTimeout() bool {
	withServerTimeoutVal := os.Getenv("TEST_WITH_SERVER_TIMEOUT")
	if withServerTimeoutVal == "" {
		return true // default
	}
	withServerTimeout, err := strconv.ParseBool(withServerTimeoutVal)
	if err != nil {
		panic(fmt.Errorf("invalid TEST_WITH_SERVER_TIMEOUT value: %w", err))
	}
	return withServerTimeout
}

func setupClientWithAdmin(t testing.TB, opts ...setupOption) (apigen.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	handler, deps := setupHandler(t, opts...)
	server := setupServer(t, handler)
	deps.server = server
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	return clt, deps
}

// clientAs creates a non-admin user with the given username via the admin HTTP API and returns
// an HTTP client authenticated as that user. The user is attached to whatever default group the
// admin setup created, which carries no fs:*/auth:* permissions — so the user can read its own
// resources via the self-serve short-circuit but cannot read other users' or perform admin
// actions.
//
// adminClt must be the client returned by setupClientWithAdmin.
func clientAs(t testing.TB, adminClt apigen.ClientWithResponsesInterface, deps *dependencies, username string) apigen.ClientWithResponsesInterface {
	t.Helper()
	ctx := context.Background()
	createUsrRes, err := adminClt.CreateUserWithResponse(ctx, apigen.CreateUserJSONRequestBody{
		Id:         username,
		InviteUser: swag.Bool(false),
	})
	require.NoError(t, err)
	require.NotNilf(t, createUsrRes.JSON201, "CreateUser returned status=%s body=%s", createUsrRes.HTTPResponse.Status, string(createUsrRes.Body))
	createCredsRes, err := adminClt.CreateCredentialsWithResponse(ctx, createUsrRes.JSON201.Id)
	require.NoError(t, err)
	require.NotNilf(t, createCredsRes.JSON201, "CreateCredentials returned status=%s body=%s", createCredsRes.HTTPResponse.Status, string(createCredsRes.Body))
	return setupClientByEndpoint(t, deps.server.URL, createCredsRes.JSON201.AccessKeyId, createCredsRes.JSON201.SecretAccessKey)
}

func setupXETHandler(t testing.TB) (http.Handler, *dependencies) {
	t.Helper()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	return setupHandler(t)
}

func TestServeXETXorbUploadRoute(t *testing.T) {
	handler, _ := setupXETHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	token := issueXETTokenForTest(t, server.URL, cred, "write", "admin/repo", "main", "model.bin")

	req, err := http.NewRequest(http.MethodPost, server.URL+"/xet/v1/xorbs/default/xorb-a", strings.NewReader("xorb-bytes"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "invalid xorb address")
}

func TestServeXETReconstructionChecksTheTokenObject(t *testing.T) {
	handler, deps := setupXETHandler(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	const branch = "main"
	const path = "models/checkpoint.bin"
	_, err := deps.catalog.CreateRepository(ctx, repo, "", onBlock(deps, "bucket/prefix"), branch, false)
	require.NoError(t, err)
	chunk := []byte("hello world!")
	fileHash, _, _ := xettest.SeedFile(t, ctx, xetstore.NewRegistry(deps.catalog.KVStore), deps.blocks, "mem://_hub_xet", chunk)
	err = deps.catalog.CreateEntry(ctx, repo, branch, catalog.DBEntry{
		Path:            path,
		PhysicalAddress: "xet://" + fileHash,
		AddressType:     catalog.AddressTypeFull,
		CreationDate:    time.Now(),
		Size:            int64(len(chunk)),
		Checksum:        "checksum-a",
	})
	require.NoError(t, err)
	commit, err := deps.catalog.Commit(ctx, repo, branch, "add checkpoint", "admin", nil, nil, nil, false)
	require.NoError(t, err)
	require.NoError(t, deps.catalog.DeleteEntry(ctx, repo, branch, path))

	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	for _, tc := range []struct {
		ref, path string
		status    int
	}{
		{ref: commit.Reference, path: "models/other.bin", status: http.StatusNotFound},
		{ref: branch, path: path, status: http.StatusNotFound},
		{ref: commit.Reference, path: path, status: http.StatusOK},
	} {
		token := issueXETTokenForTest(t, server.URL, cred, "read", repo, tc.ref, tc.path)
		req, err := http.NewRequest(http.MethodGet, server.URL+"/xet/v2/reconstructions/"+fileHash, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()

		require.Equal(t, tc.status, resp.StatusCode, "%s %s", tc.ref, tc.path)
	}
}

func TestServeXETWriteTokenRequiresWritePermission(t *testing.T) {
	handler, _ := setupXETHandler(t)
	server := setupServer(t, handler)
	adminClt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, adminClt)
	adminClt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	ctx := context.Background()
	userRes, err := adminClt.CreateUserWithResponse(ctx, apigen.CreateUserJSONRequestBody{Id: "xet-reader", InviteUser: swag.Bool(false)})
	require.NoError(t, err)
	require.NotNil(t, userRes.JSON201)
	credsRes, err := adminClt.CreateCredentialsWithResponse(ctx, userRes.JSON201.Id)
	require.NoError(t, err)
	require.NotNil(t, credsRes.JSON201)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/xet/v1/token?mode=write&repository=someone%2Frepo&ref=main&path=model.bin", nil)
	require.NoError(t, err)
	req.SetBasicAuth(credsRes.JSON201.AccessKeyId, credsRes.JSON201.SecretAccessKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func issueXETTokenForTest(t testing.TB, serverURL string, cred *authmodel.BaseCredential, mode, repo, ref, path string) string {
	t.Helper()
	query := url.Values{"mode": {mode}, "repository": {repo}, "ref": {ref}, "path": {path}}
	req, err := http.NewRequest(http.MethodPost, serverURL+"/xet/v1/token?"+query.Encode(), nil)
	require.NoError(t, err)
	req.SetBasicAuth(cred.AccessKeyID, cred.SecretAccessKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.NotEmpty(t, body.AccessToken)
	return body.AccessToken
}

func TestInvalidRoute(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)

	// setup client with invalid endpoint base url
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(cred.AccessKeyID, cred.SecretAccessKey)
	if err != nil {
		t.Fatal("basic auth security provider", err)
	}
	clt, err = apigen.NewClientWithResponses(server.URL+apiutil.BaseURL+"//", apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
	if err != nil {
		t.Fatal("failed to create api client:", err)
	}

	ctx := context.Background()
	resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{})
	if err != nil {
		t.Fatalf("failed to get hub server version")
	}
	if resp.JSONDefault == nil {
		t.Fatalf("client api call expected default error, got nil")
	}
	expectedErrMsg := api.ErrInvalidAPIEndpoint.Error()
	errMsg := resp.JSONDefault.Message
	if errMsg != expectedErrMsg {
		t.Fatalf("client response error message: %s, expected: %s", errMsg, expectedErrMsg)
	}
}
