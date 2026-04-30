VERSION=1.0.0
GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)/bin
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))

UID_GID := $(shell id -u):$(shell id -g)

# https://openapi-generator.tech
OPENAPI_LEGACY_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v5.3.0
OPENAPI_LEGACY_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_LEGACY_GENERATOR_IMAGE)
OPENAPI_GENERATOR_IMAGE=treeverse/openapi-generator-cli:v7.0.1.1
OPENAPI_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)
PY_OPENAPI_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v7.20.0
PY_OPENAPI_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(PY_OPENAPI_GENERATOR_IMAGE)

GOLANGCI_LINT_VERSION=v1.63.1
BUF_CLI_VERSION=v1.28.1

PYTHON_IMAGE=python:3

export PATH:= $(PATH):$(GOBINPATH)

GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean
GOTOOL=$(GOCMD) tool
GOGENERATE=$(GOCMD) generate
GOTEST=$(GOCMD) test
GOTESTRACE=$(GOTEST) -race
GOGET=$(GOCMD) get
GOFMT=$(GOCMD)fmt

GOTEST_PARALLELISM=4

SGHUB_BINARY_NAME=sghub
LAKECTL_BINARY_NAME=lakectl

UI_DIR=webui
UI_BUILD_DIR=$(UI_DIR)/dist

DOCKER_IMAGE=lakefs
DOCKER_TAG=$(VERSION)

ifndef PACKAGE_VERSION
	PACKAGE_VERSION=$(VERSION)
endif

export VERSION

# This cannot detect whether untracked files have yet to be added.
# That is sort-of a git feature, but can be a limitation here.
DIRTY=$(shell git diff-index --quiet HEAD -- || echo '.with.local.changes')
GIT_REF=$(shell git rev-parse --short HEAD --)
REVISION=$(GIT_REF)$(DIRTY)
export REVISION

.PHONY: all clean esti lint test gen help
all: build

clean:
	@rm -rf \
		$(LAKECTL_BINARY_NAME) \
		$(SGHUB_BINARY_NAME) \
		$(UI_BUILD_DIR) \
		$(UI_DIR)/node_modules \
		pkg/api/apigen/lakefs.gen.go \
		pkg/auth/*.gen.go

check-licenses: check-licenses-go-mod check-licenses-npm

check-licenses-go-mod:
	$(GOCMD) install github.com/google/go-licenses@latest
	$(GOBINPATH)/go-licenses check ./cmd/$(SGHUB_BINARY_NAME)
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKECTL_BINARY_NAME)

check-licenses-npm:
	$(GOCMD) install github.com/senseyeio/diligent/cmd/diligent@latest
	# The -i arg is a workaround to ignore NPM scoped packages until https://github.com/senseyeio/diligent/issues/77 is fixed
	$(GOBINPATH)/diligent check -w permissive -i ^@[^/]+?/[^/]+ $(UI_DIR)

tools: ## Install tools
	$(GOCMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GOCMD) install github.com/bufbuild/buf/cmd/buf@$(BUF_CLI_VERSION)

client-python: sdk-python

sdk-python: api/swagger.yml  ## Generate SDK for Python client - openapi generator version 7.20.0
	# Wipe everything generated from last run, except:
	#   * files we hand-maintain (Gemfile, .openapi-generator-ignore, mustache templates, etc.)
	#   * anything under surogate_hub_sdk/ (the generator rewrites its own files and .openapi-generator-ignore
	#     protects our hand-written subpackages surogate_hub_sdk/stats and surogate_hub_sdk/parquet)
	#   * our hand-written SDK tests (test_stats.py, test_parquet.py)
	# NOTE: do NOT use `-depth` + `-name surogate_hub_sdk -prune` here — GNU find's `-delete` auto-enables
	# `-depth`, which disables `-prune`, so descendants get deleted anyway. We guard with `-path` instead.
	rm -rf clients/python/build; cd clients/python && \
		find . -type f ! \( -path './surogate_hub_sdk/*' -or -name Gemfile -or -name Gemfile.lock -or -name _config.yml -or -name .openapi-generator-ignore -or -name pyproject.mustache -or -name setup.mustache -or -name client.mustache -or -name requirements.mustache -or -name pydantic.sh -or -name python-codegen-config.yaml -or -name 'test_xet_*.py' -or -name test_stats.py -or -name test_parquet.py \) -delete
	$(PY_OPENAPI_GENERATOR) generate \
		-i /mnt/$< \
		-g python \
		-t /mnt/clients/python/templates \
		--package-name surogate_hub_sdk \
		--http-user-agent "surogate-hub-python-sdk/$(PACKAGE_VERSION)" \
		--additional-properties=infoName=Invergent,infoEmail=contact@invergent.ai,packageVersion=$(PACKAGE_VERSION),projectName=surogate-hub-sdk,pydanticV2=true \
		-c /mnt/clients/python/python-codegen-config.yaml \
		-o /mnt/clients/python \
		--ignore-file-override /mnt/clients/python/.openapi-generator-ignore
	# Fix circular import: api_client.py imports "from surogate_hub_sdk import rest"
	# which triggers __init__.py (which re-imports api_client transitively)
	sed -i 's/^from surogate_hub_sdk import rest$$/import surogate_hub_sdk.rest as rest/' clients/python/surogate_hub_sdk/api_client.py
	# Remove ca_cert_data kwarg unsupported by urllib3 <2
	sed -i '/"ca_cert_data":/d' clients/python/surogate_hub_sdk/rest.py

client-java: api/swagger.yml api/java-gen-ignore  ## Generate SDK for Java (and Scala) client
	rm -rf clients/java
	mkdir -p clients/java
	cp api/java-gen-ignore clients/java/.openapi-generator-ignore
	$(OPENAPI_GENERATOR) generate \
		-i /mnt/api/swagger.yml \
		-g java \
		--invoker-package org.surogate.hub.clients.sdk \
		--http-user-agent "surogate-hub-java-sdk/$(PACKAGE_VERSION)-v1" \
		--additional-properties disallowAdditionalPropertiesIfNotPresent=false,useSingleRequestParameter=true,hideGenerationTimestamp=true,artifactVersion=$(PACKAGE_VERSION),parentArtifactId=surogate-hub-parent,parentGroupId=org.surogate.hub,parentVersion=0,groupId=org.surogate.hub,artifactId='sdk',artifactDescription='Surogate Hub OpenAPI Java client',artifactUrl=https://github.com/invergent-ai/surogate-hub,apiPackage=org.surogate.hub.clients.sdk,modelPackage=org.surogate.hub.clients.sdk.model,mainPackage=org.surogate.hub.clients.sdk,developerEmail=contact@invergent.ai,developerName='Invergent Surogate Hub dev',developerOrganization='invergent.ai',developerOrganizationUrl='https://invergent.ai',licenseName=apache2,licenseUrl=http://www.apache.org/licenses/ \
		-o /mnt/clients/java

.PHONY: clients client-python sdk-python client-java
clients: client-python client-java

package-python: package-python-client package-python-sdk

package-python-sdk: sdk-python
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package: package-python

.PHONY: gen-api
gen-api: docs/assets/js/swagger.yml ## Run the swagger code generator
	$(GOGENERATE) ./pkg/api/apigen ./pkg/auth ./pkg/authentication

.PHONY: gen-code
gen-code: gen-api ## Run the generator for inline commands
	$(GOGENERATE) \
		./pkg/auth/acl \
		./pkg/authentication \
		./pkg/distributed \
		./pkg/graveler \
		./pkg/graveler/committed \
		./pkg/graveler/sstable \
		./pkg/kv \
		./pkg/permissions \
		./pkg/pyramid \
		./tools/wrapgen/testcode

LD_FLAGS := "-X github.com/invergent-ai/surogate-hub/pkg/version.Version=$(VERSION)-$(REVISION)"
build-go:
	$(GOBUILD) -o $(SGHUB_BINARY_NAME) -gcflags "all=-N -l" -ldflags $(LD_FLAGS) -v ./cmd/$(SGHUB_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

 ## Download dependencies and build the default binary
build: gen build-go

lint: ## Lint code
	$(GOCMD) run github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run $(GOLANGCI_LINT_FLAGS)
	npx eslint@8.57.0 $(UI_DIR)/src --ext .js,.jsx,.ts,.tsx

test: test-go  ## Run tests for the project

test-go: gen-api			# Run parallelism > num_cores: most of our slow tests are *not* CPU-bound.
	$(GOTEST) -count=1 -coverprofile=cover.out -race -cover -failfast -parallel="$(GOTEST_PARALLELISM)" ./...

run-test:  ## Run tests without generating anything (faster if already generated)
	$(GOTEST) -count=1 -coverprofile=cover.out -race -short -cover -failfast ./...

fast-test:  ## Run tests without race detector (faster)
	$(GOTEST) -count=1 -coverprofile=cover.out -short -cover -failfast ./...

test-html: test  ## Run tests with HTML for the project
	$(GOTOOL) cover -html=cover.out

system-tests: # Run system tests locally
	./esti/scripts/runner.sh -r all

build-docker: build ## Build Docker image file (Docker required)
	$(DOCKER) build --target lakefs -t ghcr.io/invergent-ai/$(DOCKER_IMAGE):$(DOCKER_TAG) -t ghcr.io/invergent-ai/$(DOCKER_IMAGE):latest .

gofmt:  ## gofmt code formating
	@echo Running go formating with the following command:
	$(GOFMT) -e -s -w .

validate-fmt:  ## Validate go format
	@echo checking gofmt...
	@res=$$($(GOFMT) -d -e -s $$(find . -type d \( -path ./pkg/api/gen \) -prune -o \( -path ./pkg/permissions/*.gen.go \) -prune -o -name '*.go' -print)); \
	if [ -n "$${res}" ]; then \
		echo checking gofmt fail... ; \
		echo "$${res}"; \
		exit 1; \
	else \
		echo Your code formatting is according to gofmt standards; \
	fi

.PHONY: validate-proto
validate-proto: gen-proto  ## build proto and check if diff found
	git diff --quiet -- pkg/actions/actions.pb.go || (echo "Modification verification failed! pkg/actions/actions.pb.go"; false)
	git diff --quiet -- pkg/auth/model/model.pb.go || (echo "Modification verification failed! pkg/auth/model/model.pb.go"; false)
	git diff --quiet -- pkg/catalog/catalog.pb.go || (echo "Modification verification failed! pkg/catalog/catalog.pb.go"; false)
	git diff --quiet -- pkg/gateway/multipart/multipart.pb.go || (echo "Modification verification failed! pkg/gateway/multipart/multipart.pb.go"; false)
	git diff --quiet -- pkg/graveler/graveler.pb.go || (echo "Modification verification failed! pkg/graveler/graveler.pb.go"; false)
	git diff --quiet -- pkg/graveler/committed/committed.pb.go || (echo "Modification verification failed! pkg/graveler/committed/committed.pb.go"; false)
	git diff --quiet -- pkg/graveler/settings/test_settings.pb.go || (echo "Modification verification failed! pkg/graveler/settings/test_settings.pb.go"; false)
	git diff --quiet -- pkg/kv/secondary_index.pb.go || (echo "Modification verification failed! pkg/kv/secondary_index.pb.go"; false)
	git diff --quiet -- pkg/kv/kvtest/test_model.pb.go || (echo "Modification verification failed! pkg/kv/kvtest/test_model.pb.go"; false)

.PHONY: validate-mockgen
validate-mockgen: gen-code
	git diff --quiet -- pkg/actions/mock/mock_actions.go || (echo "Modification verification failed! pkg/actions/mock/mock_actions.go"; false)
	git diff --quiet -- pkg/auth/mock/mock_auth_client.go || (echo "Modification verification failed! pkg/auth/mock/mock_auth_client.go"; false)
	git diff --quiet -- pkg/authentication/api/mock_authentication_client.go || (echo "Modification verification failed! pkg/authentication/api/mock_authentication_client.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/batch_write_closer.go || (echo "Modification verification failed! pkg/graveler/committed/mock/batch_write_closer.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/meta_range.go || (echo "Modification verification failed! pkg/graveler/committed/mock/meta_range.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/range_manager.go || (echo "Modification verification failed! pkg/graveler/committed/mock/range_manager.go"; false)
	git diff --quiet -- pkg/graveler/mock/graveler.go || (echo "Modification verification failed! pkg/graveler/mock/graveler.go"; false)
	git diff --quiet -- pkg/kv/mock/store.go || (echo "Modification verification failed! pkg/kv/mock/store.go"; false)
	git diff --quiet -- pkg/pyramid/mock/pyramid.go || (echo "Modification verification failed! pkg/pyramid/mock/pyramid.go"; false)

.PHONY: validate-permissions-gen
validate-permissions-gen: gen-code
	git diff --quiet -- pkg/permissions/actions.gen.go || (echo "Modification verification failed!  pkg/permissions/actions.gen.go"; false)

.PHONY: validate-wrapper
validate-wrapper: gen-code
	git diff --quiet -- pkg/auth/service_wrapper.gen.go || (echo "Modification verification failed! pkg/auth/service_wrapper.gen.go"; false)
	git diff --quiet -- pkg/auth/service_inviter_wrapper.gen.go || (echo "Modification verification failed! pkg/auth/service_inviter_wrapper.gen.go"; false)

.PHONY: validate-wrapgen-testcode
validate-wrapgen-testcode: gen-code
	git diff --quiet -- ./tools/wrapgen/testcode || (echo "Modification verification failed! tools/wrapgen/testcode"; false)

validate-client-python: validate-python-sdk-legacy validate-python-sdk

validate-python-sdk-legacy:
	git diff --quiet -- clients/python-legacy || (echo "Modification verification failed! python client"; false)

validate-python-sdk:
	git diff --quiet -- clients/python || (echo "Modification verification failed! python client"; false)

validate-client-java:
	git diff --quiet -- clients/java || (echo "Modification verification failed! java client"; false)

# Run all validation/linting steps
checks-validator: lint validate-fmt validate-proto \
	validate-client-python validate-client-java validate-reference \
	validate-mockgen \
	validate-permissions-gen \
	validate-wrapper validate-wrapgen-testcode

$(UI_DIR)/node_modules:
	cd $(UI_DIR) && $(NPM) install

gen-ui: $(UI_DIR)/node_modules  ## Build UI web app
	cd $(UI_DIR) && $(NPM) run build

gen-proto: ## Build Protocol Buffers (proto) files using Buf CLI
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_CLI_VERSION) generate

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-ui gen-api gen-code clients

validate-clients-untracked-files:
	scripts/verify_clients_untracked_files.sh
