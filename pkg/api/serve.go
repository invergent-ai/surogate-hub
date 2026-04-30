package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/invergent-ai/surogate-hub/pkg/api/apiutil"
	"github.com/invergent-ai/surogate-hub/pkg/auth"
	"github.com/invergent-ai/surogate-hub/pkg/authentication"
	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
	"github.com/invergent-ai/surogate-hub/pkg/cloud"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"github.com/invergent-ai/surogate-hub/pkg/graveler"
	"github.com/invergent-ai/surogate-hub/pkg/httputil"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/invergent-ai/surogate-hub/pkg/permissions"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/invergent-ai/surogate-hub/pkg/upload"
	xetcas "github.com/invergent-ai/surogate-hub/pkg/xet/cas"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	LoggerServiceName = "rest_api"

	extensionValidationExcludeBody = "x-validation-exclude-body"
)

func Serve(cfg config.Config, catalog *catalog.Catalog, middlewareAuthenticator auth.Authenticator, authService auth.Service, authenticationService authentication.Service, blockAdapter block.Adapter, metadataManager auth.MetadataManager, migrator Migrator, collector stats.Collector, cloudMetadataProvider cloud.MetadataProvider, actions actionsHandler, auditChecker AuditChecker, logger logging.Logger, gatewayDomains []string, pathProvider upload.PathProvider, usageReporter stats.UsageReporterOperations) http.Handler {
	logger.Info("initialize OpenAPI server")
	swagger, err := apigen.GetSwagger()

	if err != nil {
		panic(err)
	}
	sessionStore := sessions.NewCookieStore(authService.SecretStore().SharedSecret())
	oidcConfig := OIDCConfig(cfg.GetBaseConfig().Auth.OIDC)
	cookieAuthConfig := CookieAuthConfig(cfg.GetBaseConfig().Auth.CookieAuthVerification)
	r := chi.NewRouter()

	r.Use(func(next http.Handler) http.Handler {
		return CORSMiddleware(next, cfg)
	})

	apiRouter := r.With(
		OapiRequestValidatorWithOptions(swagger, &openapi3filter.Options{
			AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
		}),
		httputil.LoggingMiddleware(
			httputil.RequestIDHeaderName,
			logging.Fields{logging.ServiceNameFieldKey: LoggerServiceName},
			cfg.GetBaseConfig().Logging.AuditLogLevel,
			cfg.GetBaseConfig().Logging.TraceRequestHeaders,
			false),
		AuthMiddleware(logger, swagger, middlewareAuthenticator, authService, sessionStore, &oidcConfig, &cookieAuthConfig),
		MetricsMiddleware(swagger),
	)
	controller := NewController(cfg, catalog, middlewareAuthenticator, authService, authenticationService, blockAdapter, metadataManager, migrator, collector, cloudMetadataProvider, actions, auditChecker, logger, sessionStore, pathProvider, usageReporter)
	apigen.HandlerFromMuxWithBaseURL(controller, apiRouter, apiutil.BaseURL)
	xetSecurityRequirements := openapi3.SecurityRequirements{
		{"jwt_token": []string{}},
		{"basic_auth": []string{}},
		{"cookie_auth": []string{}},
		{"oidc_auth": []string{}},
	}
	xetTokenIssuerAuthMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/v1/token") {
				next.ServeHTTP(w, r)
				return
			}
			user, err := checkSecurityRequirements(r, xetSecurityRequirements, logger, middlewareAuthenticator, authService, sessionStore, &oidcConfig, &cookieAuthConfig)
			if err != nil {
				writeError(w, r, http.StatusUnauthorized, err)
				return
			}
			if user == nil {
				writeError(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
				return
			}
			if user != nil {
				ctx := logging.AddFields(r.Context(), logging.Fields{logging.UserFieldKey: user.Username})
				r = r.WithContext(auth.WithUser(ctx, user))
			}
			next.ServeHTTP(w, r)
		})
	}

	r.Mount("/_health", httputil.ServeHealth())
	r.Mount("/metrics", promhttp.Handler())
	r.Mount("/_pprof/", httputil.ServePPROF("/_pprof/"))
	r.Mount("/openapi.json", http.HandlerFunc(swaggerSpecHandler))
	xetRegistry := xetstore.NewRegistry(catalog.KVStore)
	r.Mount("/xet", xetTokenIssuerAuthMiddleware(xetcas.NewHandler(
		xetRegistry,
		xetcas.WithXorbStore(xetcas.NewXorbStore(blockAdapter, xetStorageNamespace(cfg, blockAdapter))),
		xetcas.WithVerifyMaxConcurrent(cfg.GetBaseConfig().XET.Verify.MaxConcurrent),
		xetcas.WithProxyGrantKey([]byte(cfg.GetBaseConfig().Auth.Encrypt.SecretKey.SecureValue())),
		xetcas.WithTokenSigningKey([]byte(cfg.GetBaseConfig().Auth.Encrypt.SecretKey.SecureValue())),
		xetcas.WithTokenAuthRequired(),
		xetcas.WithReconstructionCapabilityChecker(xetReconstructionCapabilityChecker(
			catalog,
			authService,
			xetRegistry,
			cfg.GetBaseConfig().XET.Read.CapabilityScanBatchSize,
		)),
	)))
	r.Mount(apiutil.BaseURL, http.HandlerFunc(InvalidAPIEndpointHandler))
	r.Mount("/logout", NewLogoutHandler(sessionStore, logger, cfg.GetBaseConfig().Auth.LogoutRedirectURL))

	r.Mount("/", NewS3GatewayEndpointErrorHandler(gatewayDomains))

	return r
}

func xetStorageNamespace(cfg config.Config, blockAdapter block.Adapter) string {
	if storage := cfg.StorageConfig().GetStorageByID(config.SingleBlockstoreID); storage != nil {
		if prefix := storage.GetDefaultNamespacePrefix(); prefix != nil && *prefix != "" {
			return strings.TrimRight(*prefix, "/") + "/_lakefs_xet"
		}
	}
	return blockAdapter.BlockstoreType() + "://_lakefs_xet"
}

func xetReconstructionCapabilityChecker(cat *catalog.Catalog, authService auth.Service, registry *xetstore.Registry, scanBatchSize int) xetcas.ReconstructionCapabilityChecker {
	return func(ctx context.Context, fileHash string, logical xetcas.ReconstructionLogicalContext) error {
		if logical.Repo != "" && logical.Ref != "" && logical.Path != "" {
			if err := checkXETReconstructionCandidate(ctx, cat, authService, fileHash, logical); err != nil {
				return err
			}
			_ = registry.PutFileRef(ctx, xetstore.FileRef{
				FileHash: fileHash,
				Repo:     logical.Repo,
				Ref:      logical.Ref,
				Path:     logical.Path,
			})
			return nil
		}
		refs, err := registry.ListFileRefs(ctx, fileHash, scanBatchSize)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			err := checkXETReconstructionCandidate(ctx, cat, authService, fileHash, xetcas.ReconstructionLogicalContext{
				Repo: ref.Repo,
				Ref:  ref.Ref,
				Path: ref.Path,
			})
			if err == nil {
				return nil
			}
			if !errors.Is(err, xetcas.ErrReconstructionCapabilityNotFound) {
				return err
			}
		}
		return xetcas.ErrReconstructionCapabilityNotFound
	}
}

func checkXETReconstructionCandidate(ctx context.Context, cat *catalog.Catalog, authService auth.Service, fileHash string, logical xetcas.ReconstructionLogicalContext) error {
	if logical.Repo == "" || logical.Ref == "" || logical.Path == "" {
		return xetcas.ErrReconstructionCapabilityNotFound
	}
	user, err := auth.GetUser(ctx)
	if err != nil {
		return xetcas.ErrReconstructionCapabilityNotFound
	}
	resp, err := authService.Authorize(ctx, &auth.AuthorizationRequest{
		Username: user.Username,
		RequiredPermissions: permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(logical.Repo, logical.Path),
			},
		},
	})
	if err != nil {
		return err
	}
	if resp.Error != nil || !resp.Allowed {
		return xetcas.ErrReconstructionCapabilityNotFound
	}
	entry, err := cat.GetEntry(ctx, logical.Repo, logical.Ref, logical.Path, catalog.GetEntryParams{})
	if errors.Is(err, graveler.ErrNotFound) {
		return xetcas.ErrReconstructionCapabilityNotFound
	}
	if err != nil {
		return err
	}
	if entry.PhysicalAddress != "xet://"+fileHash {
		return xetcas.ErrReconstructionCapabilityNotFound
	}
	return nil
}

func swaggerSpecHandler(w http.ResponseWriter, _ *http.Request) {
	reader, err := apigen.GetSwaggerSpecReader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, reader)
}

// OapiRequestValidatorWithOptions Creates middleware to validate request by swagger spec.
// This middleware is good for net/http either since go-chi is 100% compatible with net/http.
// The original implementation can be found at https://github.com/deepmap/oapi-codegen/blob/master/pkg/chi-middleware/oapi_validate.go
// Use our own implementation in order to:
//  1. Use the latest version kin-openapi (can switch back when oapi-codegen will be updated)
//  2. For file upload wanted to skip body validation for two reasons:
//     a. didn't find a way for the validator to accept any file content type
//     b. didn't want the validator to read the complete request body for the specific request
func OapiRequestValidatorWithOptions(swagger *openapi3.Swagger, options *openapi3filter.Options) func(http.Handler) http.Handler {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// find route
			route, m, err := router.FindRoute(r)
			if err != nil {
				// We failed to find a matching route for the request.
				writeError(w, r, http.StatusBadRequest, err.Error())
				return
			}

			// include operation id from route in the context for logging
			r = r.WithContext(logging.AddFields(r.Context(), logging.Fields{"operation_id": route.Operation.OperationID}))

			// validate request
			statusCode, err := validateRequest(r, route, m, options)
			if err != nil {
				writeError(w, r, statusCode, err.Error())
				return
			}
			// serve
			next.ServeHTTP(w, r)
		})
	}
}

func validateRequest(r *http.Request, route *routers.Route, pathParams map[string]string, options *openapi3filter.Options) (int, error) {
	// Extension - validation exclude body
	if _, ok := route.Operation.Extensions[extensionValidationExcludeBody]; ok {
		o := *options
		o.ExcludeRequestBody = true
		options = &o
	}

	// Validate request
	requestValidationInput := &openapi3filter.RequestValidationInput{
		Request:    r,
		PathParams: pathParams,
		Route:      route,
		Options:    options,
	}
	if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
		var reqErr *openapi3filter.RequestError
		if errors.As(err, &reqErr) {
			return http.StatusBadRequest, err
		}
		var seqErr *openapi3filter.SecurityRequirementsError
		if errors.As(err, &seqErr) {
			return http.StatusUnauthorized, err
		}
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

// InvalidAPIEndpointHandler returns ErrInvalidAPIEndpoint, and is currently being used to ensure
// that routes under the pattern it is used with in chi.Router.Mount (i.e. /api/v1) are
// not accessible.
func InvalidAPIEndpointHandler(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusInternalServerError, ErrInvalidAPIEndpoint)
}
