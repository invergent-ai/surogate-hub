# Namespaced Repositories Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Surogate Hub repository use a GitHub/Hugging Face style `user/repo` identity.

**Architecture:** Repository identity becomes a first-class two-part value at validation and URI boundaries. Internally the logical ID remains the string `user/repo`, but repository metadata KV keys encode the slash so repository keys stay one KV path segment. The HTTP API becomes `/repositories/{user}/{repository}/...`, and `sg://user/repo/ref/path` parses as repository `user/repo`, ref `ref`, path `path`.

**Tech Stack:** Go, chi/oapi-codegen generated API server, Surogate Hub catalog/graveler layers, existing `hubctl` CLI.

---

### Task 1: Repository ID Validation

**Files:**
- Modify: `pkg/validator/validate.go`
- Modify: `pkg/graveler/validate.go`
- Test: `pkg/graveler/validate_test.go`

- [x] Add failing tests that valid repository IDs must be exactly `user/repo`, and single-segment IDs are invalid.
- [x] Run `go test ./pkg/graveler -run TestValidateRepositoryID -count=1` and confirm failures for `alice/model` and `foo`.
- [x] Update validation to allow one slash separating two valid segments.
- [x] Keep rejecting empty segments, extra segments, spaces, control chars, `..`, trailing `.`, trailing `.lock`, `@{`, `^:?*[\\`, and backslash.
- [x] Re-run `go test ./pkg/graveler -run TestValidateRepositoryID -count=1`.

### Task 2: URI Parsing

**Files:**
- Modify: `pkg/uri/parser.go`
- Test: `pkg/uri/parser_test.go`

- [x] Add failing parser tests:
  - `sg://alice/model/main/data/file.txt` parses to repository `alice/model`, ref `main`, path `data/file.txt`.
  - `sg://alice/model` parses to repository `alice/model` with no ref or path.
  - `sg://alice/model/main/` preserves empty path.
  - `sg://repo/main` is invalid because it lacks `user/repo`.
- [x] Run `go test ./pkg/uri -run TestParse -count=1` and confirm failures.
- [x] Update `Parse` to treat hostname plus the first path segment as repository identity, the second path segment as ref, and the remainder as object path.
- [x] Update `URI.String()` to render `sg://user/repo/ref/path`.
- [x] Update `ValidateRepository`, `ValidateRef`, `ValidateBranch`, and `ValidateFullyQualified` behavior through the existing repository validator.
- [x] Re-run `go test ./pkg/uri -count=1`.

### Task 3: KV Repository Key Encoding

**Files:**
- Modify: `pkg/graveler/model.go`
- Modify: `pkg/graveler/ref/repository_iterator.go`
- Test: `pkg/graveler/ref/repository_iterator_test.go`
- Test: `pkg/graveler/ref/manager_test.go`

- [x] Add tests proving `RepoPath("alice/model")` returns a single path segment under `repos/`, not raw `repos/alice/model`.
- [x] Add create/get/list tests for two repositories under different users with the same repo name, for example `alice/model` and `bob/model`.
- [x] Run the focused tests and confirm failures.
- [x] Encode only repository IDs in repository metadata keys, preferably with `url.PathEscape`.
- [x] Decode repository IDs only from protobuf payloads, not from KV keys, so old key decoding is unnecessary.
- [x] Update repository iterator seek/list start keys to use encoded repo paths.
- [x] Re-run `go test ./pkg/graveler/ref -run 'Repository|CreateRepository|GetRepository|ListRepositories' -count=1`.

### Task 4: API Routes

**Files:**
- Modify: `api/swagger.yml`
- Regenerate: `pkg/api/apigen/sghub.gen.go`
- Modify if needed: `pkg/api/controller.go`
- Test: `pkg/api/controller_test.go`

- [x] Change every repo-scoped OpenAPI path from `/repositories/{repository}` to `/repositories/{user}/{repository}`.
- [x] Keep `POST /repositories` for creation; request body `name` remains canonical `user/repo`.
- [x] Regenerate API code with the repository's existing `go generate` flow.
- [x] Add a small helper in controller code if needed: `namespacedRepository(user, repository string) string`.
- [x] Update controller method signatures and calls to combine route params into `user/repository`.
- [x] Add focused API tests for create/get/delete and one nested route, such as branches, using `alice/model`.
- [x] Run `go test ./pkg/api -run 'Repository|Branch' -count=1`.

### Task 5: hubctl

**Files:**
- Modify: `cmd/hubctl/cmd/root.go`
- Modify: `cmd/hubctl/cmd/repo_create.go`
- Modify: `cmd/hubctl/cmd/repo_delete.go`
- Modify: all generated-client call sites affected by the new `{user}/{repository}` API shape.
- Test: `cmd/hubctl/cmd/*_test.go`

- [x] Update examples from `sg://my-repo` to `sg://my-user/my-repo`.
- [x] Add focused CLI parsing tests for repository, branch, ref, and path URIs using `sg://alice/model`.
- [x] Update API client call sites to pass user and repository name separately where generated clients require it.
- [x] Run `go test ./cmd/hubctl/cmd -count=1`.

### Task 6: Permissions And Docs

**Files:**
- Modify: `pkg/permissions/*` if tests expose assumptions.
- Modify: `docs/assets/js/swagger.yml`
- Modify: `README.md` or CLI docs examples that mention repository URI shape.
- Test: permission tests touched by route/resource changes.

- [ ] Add or update permission tests using resource `alice/model`.
- [ ] Update generated docs swagger copy after API regeneration.
- [x] Replace representative documentation examples with `sg://alice/model/main/path`.
- [x] Run `go test ./pkg/permissions -count=1`.

### Task 7: Integration Verification

**Files:**
- Modify: representative `esti` tests only after unit/API/CLI tests are green.

- [ ] Update one end-to-end flow to create `alice/model`, upload an object to `sg://alice/model/main/file`, commit, list, stat, and delete.
- [ ] Run the narrow esti target if available in the Makefile.
- [x] Run `go test ./pkg/graveler ./pkg/uri ./pkg/catalog ./pkg/api ./cmd/hubctl/cmd -count=1`.
- [ ] Run `go test ./...` if runtime is acceptable.
