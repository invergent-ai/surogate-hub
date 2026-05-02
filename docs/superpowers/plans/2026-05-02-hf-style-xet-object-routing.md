# HF-Style XET Object Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make regular Python SDK `objects_api.upload_object()` and `objects_api.get_object()` use XET by default only for server-selected XET-backed objects, matching Hugging Face's model/dataset upload behavior.

**Architecture:** The server owns the upload-mode decision. The Python SDK asks a preflight endpoint with the object path and content size; the server replies `regular` or `xet`. The SDK uploads regular content through the generated `ObjectsApi` and uploads XET content through `hf_xet` plus `link_physical_address`, while downloads continue to inspect `stat_object().physical_address` and use XET only for `xet://...`.

**Tech Stack:** Go OpenAPI server, generated Go `apigen`, generated Python SDK, hand-written Python `XetObjectsApi`, `hf_xet==1.4.3`, Python `unittest`, Go `testing`, `esti` system tests.

---

## File Structure

- Modify `api/swagger.yml`: add an upload-mode response schema, add `size_bytes` to `uploadObjectPreflight`, and change the success response from only `204` to `200` with a JSON body.
- Regenerate `pkg/api/apigen/sghub.gen.go`: Go server/client types for the changed OpenAPI contract.
- Modify `pkg/config/config.go`: add `xet.upload.min_size_bytes`.
- Modify `pkg/config/defaults.go`: default `xet.upload.min_size_bytes` to `5 * 1024 * 1024`.
- Modify `pkg/api/controller.go`: make `UploadObjectPreflight` return `regular` or `xet`.
- Modify `pkg/api/controller_test.go`: add server-side tests for regular-vs-XET decisions.
- Regenerate `clients/python/surogate_hub_sdk/api/internal_api.py`, `clients/python/surogate_hub_sdk/models/*`, docs, and generated exports via `make sdk-python`.
- Modify `clients/python/surogate_hub_sdk/xet_objects_api.py`: ask preflight before upload and fall back to regular upload unless the server selects XET.
- Modify `clients/python/test/test_xet_objects_api.py`: cover regular fallback, XET selection, unsupported options, and XET download behavior.
- Create `esti/python_xet_sdk_test.go`: end-to-end check that Python SDK uploads small files regularly and large files via XET.

## Task 1: OpenAPI Upload Mode Contract

**Files:**
- Modify: `api/swagger.yml`
- Generated later: `pkg/api/apigen/sghub.gen.go`
- Generated later: `clients/python/surogate_hub_sdk/models/object_upload_mode.py`
- Generated later: `clients/python/surogate_hub_sdk/api/internal_api.py`

- [ ] **Step 1: Add the failing contract expectation**

Run:

```bash
rg -n "ObjectUploadMode|size_bytes|upload_mode" api/swagger.yml pkg/api/apigen/sghub.gen.go clients/python/surogate_hub_sdk/api/internal_api.py
```

Expected: no `ObjectUploadMode`, no `size_bytes` parameter on `uploadObjectPreflight`, and no generated Python model.

- [ ] **Step 2: Update `api/swagger.yml` schemas**

Add this schema next to `ObjectStageCreation`:

```yaml
    ObjectUploadMode:
      type: object
      required:
        - upload_mode
      properties:
        upload_mode:
          type: string
          enum: [regular, xet]
          description: |
            Server-selected upload mode for a regular object upload.
            "regular" means the client should use POST /objects.
            "xet" means the client may upload through XET and link an xet:// physical address.
```

- [ ] **Step 3: Update `uploadObjectPreflight` parameters and response**

In `/repositories/{user}/{repository}/branches/{branch}/objects/stage_allowed`, add this query parameter after `path`:

```yaml
      - in: query
        name: size_bytes
        description: Size of the object content the client plans to upload.
        required: false
        schema:
          type: integer
          format: int64
```

Change the `204` response to:

```yaml
        200:
          description: User has permissions to upload this object and the server-selected upload mode.
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ObjectUploadMode"
```

- [ ] **Step 4: Generate Go API code**

Run:

```bash
make gen-api
```

Expected: `pkg/api/apigen/sghub.gen.go` contains `ObjectUploadMode`, `UploadObjectPreflightParams.SizeBytes`, and `UploadObjectPreflightResponse.JSON200`.

- [ ] **Step 5: Commit**

```bash
git add api/swagger.yml pkg/api/apigen/sghub.gen.go
git commit -m "api: add object upload mode preflight"
```

## Task 2: Server-Side Upload Mode Decision

**Files:**
- Modify: `pkg/config/config.go`
- Modify: `pkg/config/defaults.go`
- Modify: `pkg/api/controller.go`
- Test: `pkg/api/controller_test.go`

- [ ] **Step 1: Write failing controller tests**

Add tests to `pkg/api/controller_test.go`:

```go
func TestController_UploadObjectPreflightReturnsRegularForSmallObject(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	const branch = "main"
	_, err := deps.catalog.CreateRepository(ctx, repo, "", onBlock(deps, "bucket/prefix"), branch, false)
	require.NoError(t, err)

	resp, err := clt.UploadObjectPreflightWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), "main", &apigen.UploadObjectPreflightParams{
		Path:      "small.txt",
		SizeBytes: apiutil.Ptr(int64(1024)),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, "regular", resp.JSON200.UploadMode)
}

func TestController_UploadObjectPreflightReturnsXETForLargeObject(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	const branch = "main"
	_, err := deps.catalog.CreateRepository(ctx, repo, "", onBlock(deps, "bucket/prefix"), branch, false)
	require.NoError(t, err)

	resp, err := clt.UploadObjectPreflightWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), "main", &apigen.UploadObjectPreflightParams{
		Path:      "models/model.bin",
		SizeBytes: apiutil.Ptr(int64(8 * 1024 * 1024)),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, "xet", resp.JSON200.UploadMode)
}

func TestController_UploadObjectPreflightReturnsRegularWhenSizeUnknown(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	const branch = "main"
	_, err := deps.catalog.CreateRepository(ctx, repo, "", onBlock(deps, "bucket/prefix"), branch, false)
	require.NoError(t, err)

	resp, err := clt.UploadObjectPreflightWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), "main", &apigen.UploadObjectPreflightParams{
		Path: "stream.bin",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, "regular", resp.JSON200.UploadMode)
}
```

Run:

```bash
go test ./pkg/api -run 'TestController_UploadObjectPreflightReturns' -count=1
```

Expected: compile failure or test failure because `ObjectUploadMode` handling is not implemented.

- [ ] **Step 2: Add XET upload threshold config**

In `pkg/config/config.go`, extend the `XET` struct:

```go
			Upload struct {
				MinSizeBytes int64 `mapstructure:"min_size_bytes"`
			} `mapstructure:"upload"`
```

In `pkg/config/defaults.go`, add:

```go
	viper.SetDefault("xet.upload.min_size_bytes", int64(5*1024*1024))
```

- [ ] **Step 3: Implement upload-mode selection**

In `pkg/api/controller.go`, add a helper near `UploadObjectPreflight`:

```go
func (c *Controller) objectUploadMode(sizeBytes *int64) string {
	if sizeBytes == nil || *sizeBytes <= 0 {
		return "regular"
	}
	minSize := c.Config.GetBaseConfig().XET.Upload.MinSizeBytes
	if minSize <= 0 {
		return "regular"
	}
	if *sizeBytes >= minSize {
		return "xet"
	}
	return "regular"
}
```

Replace the `204` response in `UploadObjectPreflight` with:

```go
	writeResponse(w, r, http.StatusOK, apigen.ObjectUploadMode{
		UploadMode: c.objectUploadMode(params.SizeBytes),
	})
```

- [ ] **Step 4: Run focused Go tests**

Run:

```bash
go test ./pkg/api -run 'TestController_UploadObjectPreflightReturns' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/config/config.go pkg/config/defaults.go pkg/api/controller.go pkg/api/controller_test.go
git commit -m "api: choose object upload mode during preflight"
```

## Task 3: Regenerate Python SDK

**Files:**
- Modify generated files under `clients/python`
- Preserve hand-written files under `clients/python/surogate_hub_sdk/xet_objects_api.py`, `clients/python/surogate_hub_sdk/xet.py`, `clients/python/test/test_xet_*.py`, `clients/python/templates/*`

- [ ] **Step 1: Generate Python client**

Run:

```bash
make sdk-python
```

Expected: generated Python files include `ObjectUploadMode`; `InternalApi.upload_object_preflight()` accepts `size_bytes`.

- [ ] **Step 2: Verify generated contract**

Run:

```bash
rg -n "ObjectUploadMode|size_bytes|upload_mode" clients/python/surogate_hub_sdk clients/python/docs clients/python/README.md
```

Expected: generated model, docs, and `InternalApi` references exist.

- [ ] **Step 3: Commit**

```bash
git add clients/python
git commit -m "sdk-python: regenerate upload mode preflight"
```

## Task 4: Python `XetObjectsApi` Upload Routing

**Files:**
- Modify: `clients/python/surogate_hub_sdk/xet_objects_api.py`
- Test: `clients/python/test/test_xet_objects_api.py`

- [ ] **Step 1: Write failing Python unit tests**

Add fake preflight support in `clients/python/test/test_xet_objects_api.py`:

```python
class FakeInternalApi:
    def __init__(self, upload_mode="regular"):
        self.upload_mode = upload_mode
        self.calls = []

    def upload_object_preflight(self, repository, branch, path, size_bytes=None, **kwargs):
        self.calls.append(
            {
                "repository": repository,
                "branch": branch,
                "path": path,
                "size_bytes": size_bytes,
                "kwargs": kwargs,
            }
        )
        return SimpleNamespace(upload_mode=self.upload_mode)
```

Update the `api()` helper to attach `FakeInternalApi`:

```python
    def api(self, hf_xet=None, upload_mode="regular"):
        from surogate_hub_sdk.xet_objects_api import XetObjectsApi

        configuration = Configuration(
            host="http://sghub.example/api/v1",
            username="access",
            password="secret",
        )
        api_client = SimpleNamespace(configuration=configuration)
        api = XetObjectsApi(
            api_client,
            hf_xet_module=hf_xet or FakeHfXet(),
            token_info=("token", 4102444800),
            token_refresher=lambda: ("token", 4102444800),
        )
        api._staging_api = FakeStagingApi()
        api._internal_api = FakeInternalApi(upload_mode=upload_mode)
        return api
```

Add tests:

```python
    def test_upload_object_uses_regular_upload_when_preflight_returns_regular(self):
        from unittest.mock import patch
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="regular")

        with patch.object(ObjectsApi, "upload_object", return_value="regular-result") as regular_upload:
            result = api.upload_object("repo", "main", "small.txt", content=b"small")

        self.assertEqual(result, "regular-result")
        regular_upload.assert_called_once()
        self.assertEqual(hf_xet.upload_calls, [])
        self.assertEqual(api._internal_api.calls[0]["size_bytes"], 5)

    def test_upload_object_uses_xet_when_preflight_returns_xet(self):
        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="xet")

        api.upload_object("repo", "main", "models/model.bin", content=b"model bytes")

        self.assertEqual(len(hf_xet.upload_calls), 1)
        self.assertEqual(api._internal_api.calls[0]["size_bytes"], len(b"model bytes"))
        link = api._staging_api.calls[0]
        self.assertEqual(link["staging_metadata"].staging.physical_address, "xet://file-hash")

    def test_upload_object_skips_preflight_for_unsupported_xet_options(self):
        from unittest.mock import patch
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="xet")

        with patch.object(ObjectsApi, "upload_object", return_value="regular-result") as regular_upload:
            result = api.upload_object("repo", "main", "archive.bin", storage_class="STANDARD", content=b"data")

        self.assertEqual(result, "regular-result")
        regular_upload.assert_called_once()
        self.assertEqual(api._internal_api.calls, [])
        self.assertEqual(hf_xet.upload_calls, [])
```

Run:

```bash
cd clients/python && python -m pytest test/test_xet_objects_api.py -q
```

Expected: FAIL because `XetObjectsApi` does not call preflight.

- [ ] **Step 2: Implement preflight and size calculation**

In `clients/python/surogate_hub_sdk/xet_objects_api.py`, import `InternalApi`:

```python
from surogate_hub_sdk.api.internal_api import InternalApi
```

In `__init__`, add:

```python
        self._internal_api = InternalApi(self.api_client)
```

Add helpers:

```python
    def _content_size(self, content, local_path=None):
        if content is None:
            return None
        if local_path is not None:
            try:
                return os.path.getsize(local_path)
            except OSError:
                return None
        if isinstance(content, (bytes, bytearray)):
            return len(content)
        if isinstance(content, tuple) and len(content) == 2 and isinstance(content[1], (bytes, bytearray)):
            return len(content[1])
        return None

    def _server_upload_mode(self, repository, branch, path, size_bytes, _request_timeout=None, _headers=None, _host_index=0):
        mode = self._internal_api.upload_object_preflight(
            repository,
            branch,
            path,
            size_bytes=size_bytes,
            _request_timeout=_request_timeout,
            _headers=_headers,
            _host_index=_host_index,
        )
        return getattr(mode, "upload_mode", "regular")
```

In `upload_object`, after `xet_content` is created and before `_hf_xet()` is loaded:

```python
        local_path, cleanup_path = xet_content
        upload_mode = self._server_upload_mode(
            repository,
            branch,
            path,
            self._content_size(content, local_path),
            _request_timeout=_request_timeout,
            _headers=_headers,
            _host_index=_host_index,
        )
        if upload_mode != "xet":
            if cleanup_path is not None:
                try:
                    os.unlink(cleanup_path)
                except FileNotFoundError:
                    pass
            return super().upload_object(
                repository,
                branch,
                path,
                if_none_match=if_none_match,
                storage_class=storage_class,
                force=force,
                content=content,
                _request_timeout=_request_timeout,
                _request_auth=_request_auth,
                _content_type=_content_type,
                _headers=_headers,
                _host_index=_host_index,
            )
```

Keep the existing XET upload and `finally` cleanup for the `"xet"` path.

- [ ] **Step 3: Run focused Python tests**

Run:

```bash
cd clients/python && python -m pytest test/test_xet_objects_api.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add clients/python/surogate_hub_sdk/xet_objects_api.py clients/python/test/test_xet_objects_api.py
git commit -m "sdk-python: preflight object uploads before xet"
```

## Task 5: Keep Generated Client Defaults Stable

**Files:**
- Modify if needed: `clients/python/templates/client.mustache`
- Modify if needed: `clients/python/surogate_hub_sdk/client.py`
- Test: `clients/python/test/test_xet_objects_api.py`

- [ ] **Step 1: Verify `HubClient` still defaults to `XetObjectsApi`**

Run:

```bash
cd clients/python && python -m pytest test/test_xet_objects_api.py::TestXetObjectsApi::test_hub_client_uses_xet_objects_api_by_default -q
```

Expected: PASS.

- [ ] **Step 2: Verify regeneration keeps the wrapper**

Run:

```bash
make sdk-python
git diff -- clients/python/surogate_hub_sdk/client.py clients/python/templates/client.mustache
```

Expected: `client.py` still imports `XetObjectsApi` and assigns `self.objects_api = XetObjectsApi(self._api)`.

- [ ] **Step 3: Commit if regeneration changed files**

If `git diff -- clients/python` is non-empty:

```bash
git add clients/python
git commit -m "sdk-python: preserve xet objects wrapper after generation"
```

If the diff is empty, skip this commit.

## Task 6: End-To-End `esti` Coverage

**Files:**
- Create: `esti/python_xet_sdk_test.go`

- [ ] **Step 1: Write the failing `esti` test**

Create `esti/python_xet_sdk_test.go`:

```go
package esti

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/api/apigen"
	"github.com/stretchr/testify/require"
)

func TestPythonSDKUploadsRegularAndXETObjects(t *testing.T) {
	ctx := context.Background()
	repo := "repo-python-xet-sdk"
	createRepository(ctx, t, repo, generateUniqueStorageNamespace(repo), false)

	tmp := t.TempDir()
	script := filepath.Join(tmp, "python_xet_upload.py")
	require.NoError(t, os.WriteFile(script, []byte(`
import pathlib
import sys

sys.path.insert(0, "clients/python")

from surogate_hub_sdk.client import HubClient
from surogate_hub_sdk.configuration import Configuration

endpoint, access_key, secret_key, repo, large_file = sys.argv[1:6]

cfg = Configuration(host=endpoint + "/api/v1", username=access_key, password=secret_key)
client = HubClient(configuration=cfg)

small = b"small object"
large_path = pathlib.Path(large_file)
large_path.write_bytes(b"x" * (6 * 1024 * 1024))

client.objects_api.upload_object(repo, "main", "small.txt", content=small)
client.objects_api.upload_object(repo, "main", "large.bin", content=str(large_path))

assert client.objects_api.get_object(repo, "main", "small.txt") == small
assert client.objects_api.get_object(repo, "main", "large.bin") == large_path.read_bytes()
`), 0o600))

	largeFile := filepath.Join(tmp, "large.bin")
	cmd := exec.Command("python", script, endpointURL, DefaultAdminAccessKeyID, DefaultAdminSecretAccessKey, repo, largeFile)
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	smallResp, err := client.StatObjectWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), "main", &apigen.StatObjectParams{Path: "small.txt"})
	require.NoError(t, err)
	require.Equal(t, 200, smallResp.StatusCode(), string(smallResp.Body))
	require.False(t, strings.HasPrefix(smallResp.JSON200.PhysicalAddress, "xet://"))

	largeResp, err := client.StatObjectWithResponse(ctx, apigen.RepositoryOwner(repo), apigen.RepositoryName(repo), "main", &apigen.StatObjectParams{Path: "large.bin"})
	require.NoError(t, err)
	require.Equal(t, 200, largeResp.StatusCode(), string(largeResp.Body))
	require.True(t, strings.HasPrefix(largeResp.JSON200.PhysicalAddress, "xet://"))
}
```

Run:

```bash
go test ./esti -run TestPythonSDKUploadsRegularAndXETObjects -count=1 -system-tests
```

Expected: initially fails if Python dependencies or routing are not wired correctly.

- [ ] **Step 2: Fix test harness details**

If the test cannot import the SDK or `hf_xet`, install the local SDK before running:

```bash
python -m pip install -e clients/python
```

If the `cmd.Dir = ".."` path is wrong for the local test runner, change the script path insertion to compute the repository root from an environment variable:

```go
cmd.Env = append(os.Environ(), "REPO_ROOT=..")
```

and in Python:

```python
import os
sys.path.insert(0, os.path.join(os.environ["REPO_ROOT"], "clients/python"))
```

- [ ] **Step 3: Run focused `esti` test**

Run:

```bash
go test ./esti -run TestPythonSDKUploadsRegularAndXETObjects -count=1 -system-tests
```

Expected: PASS against a running `esti` environment.

- [ ] **Step 4: Commit**

```bash
git add esti/python_xet_sdk_test.go
git commit -m "esti: cover python sdk xet object routing"
```

## Task 7: Verification Sweep

**Files:**
- No new files unless failures reveal missing generated updates.

- [ ] **Step 1: Run Go API tests**

Run:

```bash
go test ./pkg/api ./pkg/xet/... -count=1
```

Expected: PASS.

- [ ] **Step 2: Run Python focused tests**

Run:

```bash
cd clients/python && python -m pytest test/test_xet_objects_api.py test/test_xet_client.py test/test_xet_packaging.py -q
```

Expected: PASS.

- [ ] **Step 3: Run generation drift checks**

Run:

```bash
make gen-api
make sdk-python
git diff --exit-code -- api/swagger.yml pkg/api/apigen/sghub.gen.go clients/python
```

Expected: no diff.

- [ ] **Step 4: Run targeted `esti`**

Run:

```bash
go test ./esti -run 'TestXET|TestPythonSDKUploadsRegularAndXETObjects' -count=1 -system-tests
```

Expected: PASS against the running test environment.

- [ ] **Step 5: Final commit if verification required generated cleanup**

```bash
git status --short
git add api/swagger.yml pkg/api/apigen/sghub.gen.go clients/python pkg/api/controller.go pkg/config/config.go pkg/config/defaults.go pkg/api/controller_test.go esti/python_xet_sdk_test.go
git commit -m "test: verify hf-style xet object routing"
```

Skip this commit if `git status --short` is clean.

## Completed Items

- [x] Hugging Face upload flow reviewed in `study/huggingface_hub`.
- [x] Existing Python XET wrapper located at `clients/python/surogate_hub_sdk/xet_objects_api.py`.
- [x] Existing generated-client template hook located at `clients/python/templates/client.mustache`.
- [x] Existing server XET read/link behavior confirmed in `pkg/api/controller.go`.
- [x] Current gap identified: Python upload currently uses XET for all path/bytes content that does not use unsupported options.

## Remaining Work

- [ ] Add server-visible upload mode to the API contract.
- [ ] Implement server-owned regular-vs-XET decision with a configurable size threshold.
- [ ] Regenerate Go and Python clients.
- [ ] Update `XetObjectsApi.upload_object()` to call preflight and route only `xet` selections through `hf_xet`.
- [ ] Add Python unit tests for both modes.
- [ ] Add `esti` coverage proving small regular upload and large XET upload through the Python SDK.
- [ ] Run focused Go, Python, generation, and `esti` verification.

## Notes And Decisions

- Default threshold is `5 MiB`, matching the Hugging Face client assumption that regular files are small enough to hash/read inline and large content moves out of normal Git-style object handling.
- Unknown-size uploads stay `regular`; this avoids spooling arbitrary streams only to discover they should not be XET-backed.
- `storage_class`, custom `_request_auth`, `range`, `presign`, and conditional read behavior keep using generated `ObjectsApi` paths.
- Downloads do not need a preflight endpoint. The object metadata already carries `physical_address`; `xet://...` is sufficient to select XET.
- The plan intentionally keeps generated `ObjectsApi` untouched. The stable extension point is `XetObjectsApi`, assigned from the generated `HubClient` template.
