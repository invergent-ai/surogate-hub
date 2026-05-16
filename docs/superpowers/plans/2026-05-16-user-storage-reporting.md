# Per-User Storage Reporting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-user storage accounting to Surogate Hub: every block-store write/delete in a repo namespace updates a KV counter for the repo's owner, a periodic reconciler corrects drift by querying the block-store walker, three new HTTP endpoints expose the figure and let admins set per-user quotas, and write paths reject requests that would exceed an existing quota.

**Architecture:** A new `StorageAccountant` (in-process delta cache, periodic KV flusher) and `StorageReconciler` (hourly block-store walker) live in `pkg/stats` alongside `UsageReporter`. KV partition `storage` holds `storage/repo/{owner}/{repo}`, `storage/user/{owner}`, `storage/quota/{owner}`, `storage/meta/{owner}/last_reconciled_at`. A new `QuotaChecker` reads `storage/quota/{owner}` and `storage/user/{owner}` and gates the upload paths in `pkg/api/controller.go` and `pkg/gateway/operations/`. New endpoints are `GET /auth/users/{userId}/storage`, `PUT /auth/users/{userId}/quota`, `DELETE /auth/users/{userId}/quota`.

**Tech Stack:** Go 1.x, surogate-hub `kv.Store`, `block.Adapter`, `oapi-codegen` v1.5.6 for swagger codegen, Viper for config.

**Spec:** [docs/superpowers/specs/2026-05-16-user-storage-reporting-design.md](../specs/2026-05-16-user-storage-reporting-design.md).

---

## Execution Status

- [x] Task 1: KV key helpers (`pkg/stats/storage_keys.go`)
- [x] Task 2: `StorageAccountant` (also folds in `DeleteRepo`, `InitRepo`, and the Task 16.5 doc comment)
- [x] Task 3: `QuotaChecker`
- [x] Task 4: `auth:WriteUser` permission
- [x] Task 5: `storage_usage` config block
- [x] Task 6: `StorageReconciler`
- [x] Task 7: Catalog + block-adapter wiring (moved to `pkg/stats/storagewiring` to avoid import cycle with block/s3 → stats)
- [x] Task 8 + 8.5 + 9: Runtime + test-fixture wiring
- [x] Task 10: Accountant hooks in API upload paths (UploadObject + CompletePresignMultipartUpload; CopyObject deliberately no-op)
- [x] Task 11: Accountant hooks in gateway upload paths (handlePut + HandleCompleteMultipartUpload)
- [x] Task 12: Counter maintenance on repo create/delete
- [x] Task 13: `GET /auth/users/{userId}/storage`
- [x] Task 14: `PUT/DELETE /auth/users/{userId}/quota` (folded into Task 13 commit)
- [x] Task 15: Quota enforcement on upload paths (UploadObject + CompletePresignMultipartUpload + gateway handlePut + HandleCompleteMultipartUpload)
- [x] Task 16: End-to-end and drift tests
- [x] Task 16.5: Document omitted delete hooks *(folded into Task 2)*
- [x] Task 17: User-facing docs (`docs/storage-usage.md` + `pkg/stats/README.md`)
- [x] Acceptance: `go vet ./...` + `go test ./...` green for everything touched (2552 tests pass; the 3 failures are pre-existing: cosmosdb/dockertest need Docker/credentials and `sig.TestUnsignedPayload` fails on `main` too. `go vet` reports two pre-existing issues in `pkg/distributed/mc_owner_test.go` and `pkg/block/gs/main_test.go`, neither modified by this work.)

---

## File Structure

**New files:**

- `pkg/stats/storage_accountant.go` — in-memory delta cache + KV flusher.
- `pkg/stats/storage_accountant_test.go`
- `pkg/stats/storage_reconciler.go` — periodic block-store walker that overwrites repo counters.
- `pkg/stats/storage_reconciler_test.go`
- `pkg/stats/storage_quota.go` — `QuotaChecker.Allow` and quota CRUD helpers.
- `pkg/stats/storage_quota_test.go`
- `pkg/stats/storage_keys.go` — partition constant + key formatters.
- `pkg/stats/storage_keys_test.go`

**Modified files:**

- `api/swagger.yml` — three new operations under `/auth/users/{userId}/...`.
- `pkg/api/apigen/sghub.gen.go` — regenerated.
- `pkg/api/controller.go` — three new controller methods, calls to accountant and quota checker from upload/copy/repository-delete sites, and access to the catalog KV store for storage counters.
- `pkg/api/controller_test.go` — endpoint and enforcement tests.
- `pkg/permissions/actions.go` — add `WriteUserAction = "auth:WriteUser"`.
- `pkg/config/config.go` — new `StorageUsage` block under `BaseConfig`.
- `pkg/config/defaults.go` — defaults for `storage_usage.*`.
- `cmd/sghub/cmd/run.go` — construct accountant + reconciler + quota checker, pass into controller and gateway.
- `pkg/api/serve.go` — accept accountant + quota checker in handler setup.
- `pkg/api/serve_test.go` — wire accountant + quota checker into `setupHandler`.
- `pkg/gateway/handler.go`, `pkg/gateway/middleware.go`, `pkg/gateway/operations/base.go`, `pkg/gateway/operations/putobject.go`, `pkg/gateway/operations/postobject.go` — pass accountant/quota dependencies into gateway operations and call them in `PutObject`, upload part, upload copy part/range, multipart completion, and copy object.

**File responsibility split:**

- `storage_keys.go` is the single source of truth for the KV layout. Every other file imports its constants/helpers — no key-string literals elsewhere.
- `storage_accountant.go` knows nothing about HTTP, repos, or block storage. It exposes `Add(owner, repo, delta)` and runs a flusher.
- `storage_reconciler.go` knows about the block adapter and the catalog (to list repos and get storage namespaces). It calls into the accountant's KV writer to overwrite per-repo counters.
- `storage_quota.go` exposes `Allow(ctx, owner, contentLength)`. It only reads KV; it does not call the accountant.
- The controller and gateway operation context are the integration points: they know the request, owner, repository, and byte count. Repository creation/deletion counter maintenance belongs in the controller handlers, not in `pkg/catalog`, because the catalog package does not own HTTP auth context or per-user API behavior.

---

## Task 1: Set up `pkg/stats/storage_keys.go` — KV layout helpers

**Files:**
- Create: `/work/surogate-hub/pkg/stats/storage_keys.go`
- Test: `/work/surogate-hub/pkg/stats/storage_keys_test.go`

- [ ] **Step 1: Write the failing test**

`/work/surogate-hub/pkg/stats/storage_keys_test.go`:

```go
package stats_test

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestStorageKeyFormatters(t *testing.T) {
	if got, want := string(stats.StoragePartition), "storage"; got != want {
		t.Errorf("StoragePartition = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageRepoKey("alice", "training")), "storage/repo/alice/training"; got != want {
		t.Errorf("StorageRepoKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageUserKey("alice")), "storage/user/alice"; got != want {
		t.Errorf("StorageUserKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageQuotaKey("alice")), "storage/quota/alice"; got != want {
		t.Errorf("StorageQuotaKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageMetaLastReconciledAtKey("alice")), "storage/meta/alice/last_reconciled_at"; got != want {
		t.Errorf("StorageMetaLastReconciledAtKey = %q, want %q", got, want)
	}
	if got, want := string(stats.StorageRepoPrefix("alice")), "storage/repo/alice/"; got != want {
		t.Errorf("StorageRepoPrefix = %q, want %q", got, want)
	}
}

func TestParseStorageRepoKey(t *testing.T) {
	owner, repo, err := stats.ParseStorageRepoKey([]byte("storage/repo/alice/training"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if owner != "alice" || repo != "training" {
		t.Errorf("got (%q, %q), want (alice, training)", owner, repo)
	}
	if _, _, err := stats.ParseStorageRepoKey([]byte("nope")); err == nil {
		t.Errorf("expected error for malformed key")
	}
	if _, _, err := stats.ParseStorageRepoKey([]byte("storage/user/alice")); err == nil {
		t.Errorf("expected error for non-repo key")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageKey -v
```

Expected: FAIL with `undefined: stats.StoragePartition` (or similar).

- [ ] **Step 3: Write the implementation**

`/work/surogate-hub/pkg/stats/storage_keys.go`:

```go
package stats

import (
	"errors"
	"fmt"
	"regexp"
)

// StoragePartition is the KV partition that holds per-user/per-repo storage counters and quotas.
var StoragePartition = []byte("storage")

// ErrInvalidStorageRepoKey is returned by ParseStorageRepoKey when the key does not match the
// expected layout.
var ErrInvalidStorageRepoKey = errors.New("invalid storage repo key")

// StorageRepoKey returns the key holding the bytes-allocated counter for a single repo.
// Layout: storage/repo/{owner}/{repo}
func StorageRepoKey(owner, repo string) []byte {
	return []byte(fmt.Sprintf("storage/repo/%s/%s", owner, repo))
}

// StorageRepoPrefix returns the scan prefix for all repos owned by an owner.
// Layout: storage/repo/{owner}/
func StorageRepoPrefix(owner string) []byte {
	return []byte(fmt.Sprintf("storage/repo/%s/", owner))
}

// StorageUserKey returns the key holding the denormalized per-user total.
// Layout: storage/user/{owner}
func StorageUserKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/user/%s", owner))
}

// StorageQuotaKey returns the key holding the per-user quota. Absence ⇒ unlimited.
// Layout: storage/quota/{owner}
func StorageQuotaKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/quota/%s", owner))
}

// StorageMetaLastReconciledAtKey returns the key holding the timestamp of the last reconciler pass.
// Layout: storage/meta/{owner}/last_reconciled_at
func StorageMetaLastReconciledAtKey(owner string) []byte {
	return []byte(fmt.Sprintf("storage/meta/%s/last_reconciled_at", owner))
}

var storageRepoKeyRegexp = regexp.MustCompile(`^storage/repo/([^/]+)/(.+)$`)

// ParseStorageRepoKey reverses StorageRepoKey. Returns ErrInvalidStorageRepoKey on mismatch.
func ParseStorageRepoKey(key []byte) (owner, repo string, err error) {
	m := storageRepoKeyRegexp.FindSubmatch(key)
	if m == nil {
		return "", "", fmt.Errorf("%w: %q", ErrInvalidStorageRepoKey, key)
	}
	return string(m[1]), string(m[2]), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageKey -v
cd /work/surogate-hub && go test ./pkg/stats/ -run TestParseStorageRepoKey -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_keys.go pkg/stats/storage_keys_test.go
git -C /work/surogate-hub commit -m "stats: add storage KV key helpers"
```

---

## Task 2: `pkg/stats/storage_accountant.go` — in-memory deltas + KV flusher

**Files:**
- Create: `/work/surogate-hub/pkg/stats/storage_accountant.go`
- Test: `/work/surogate-hub/pkg/stats/storage_accountant_test.go`

- [ ] **Step 1: Write the failing test (Add accumulates deltas in memory)**

`/work/surogate-hub/pkg/stats/storage_accountant_test.go`:

```go
package stats_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestStorageAccountant_AddBuffersInMemory(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	a.Add(ctx, "alice", "training", 50)
	a.Add(ctx, "alice", "evals", 200)
	a.Add(ctx, "bob", "main", 7)

	// Without a flush, no KV writes have happened.
	if _, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); err != kv.ErrNotFound {
		t.Errorf("expected ErrNotFound before flush, got %v", err)
	}
}

func TestStorageAccountant_FlushPersistsCounters(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	a.Add(ctx, "alice", "training", 50)
	a.Add(ctx, "alice", "evals", 200)

	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 150)
	requireInt64(t, store, stats.StorageRepoKey("alice", "evals"), 200)
	requireInt64(t, store, stats.StorageUserKey("alice"), 350)
}

func TestStorageAccountant_FlushAccumulatesAcrossFlushes(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)

	a.Add(ctx, "alice", "training", 100)
	mustFlush(t, ctx, a)
	a.Add(ctx, "alice", "training", 25)
	a.Add(ctx, "alice", "training", -50)
	mustFlush(t, ctx, a)

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 75)
	requireInt64(t, store, stats.StorageUserKey("alice"), 75)
}

func TestStorageAccountant_StartTickerFlushes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)
	a.Start(ctx, 5*time.Millisecond, testLogger(t))

	a.Add(ctx, "alice", "training", 42)

	// Wait for at least one tick.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, err := store.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"))
		if err == nil && got != nil {
			n, _ := strconv.ParseInt(string(got.Value), 10, 64)
			if n == 42 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ticker did not flush 42 to KV within deadline")
}

func mustFlush(t *testing.T, ctx context.Context, a *stats.StorageAccountant) {
	t.Helper()
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func requireInt64(t *testing.T, store kv.Store, key []byte, want int64) {
	t.Helper()
	got, err := store.Get(context.Background(), stats.StoragePartition, key)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", key, err)
	}
	if n != want {
		t.Errorf("%s = %d, want %d", key, n, want)
	}
}
```

Add a `testLogger(t *testing.T) logging.Logger` helper in the same test file (or use `logging.Default()` if simpler):

```go
import "github.com/invergent-ai/surogate-hub/pkg/logging"

func testLogger(t *testing.T) logging.Logger {
	t.Helper()
	return logging.Default()
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageAccountant -v
```

Expected: FAIL with `undefined: stats.NewStorageAccountant`.

- [ ] **Step 3: Write the implementation**

`/work/surogate-hub/pkg/stats/storage_accountant.go`:

```go
package stats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
)

// StorageAccountant buffers per-repo byte deltas in memory and periodically flushes them to the
// KV store. It is best-effort: a missed Add call leaks drift that the StorageReconciler corrects
// within one reconciler interval.
type StorageAccountant struct {
	storage kv.Store

	mu     sync.Mutex
	deltas map[accountantKey]int64
}

type accountantKey struct {
	owner string
	repo  string
}

// NewStorageAccountant constructs an accountant backed by the given KV store.
func NewStorageAccountant(storage kv.Store) *StorageAccountant {
	return &StorageAccountant{
		storage: storage,
		deltas:  make(map[accountantKey]int64),
	}
}

// Add records a byte delta for (owner, repo). Negative deltas are valid (deletes).
// Add never blocks on KV; deltas are applied on the next Flush.
// The ctx is accepted for future use (telemetry, structured logging); the current implementation
// only reads the value of the parameters.
func (a *StorageAccountant) Add(ctx context.Context, owner, repo string, delta int64) {
	if delta == 0 || owner == "" || repo == "" {
		return
	}
	a.mu.Lock()
	a.deltas[accountantKey{owner: owner, repo: repo}] += delta
	a.mu.Unlock()
}

// Start launches a goroutine that calls Flush every interval until ctx is cancelled.
// A zero interval disables the ticker (test/dev only).
func (a *StorageAccountant) Start(ctx context.Context, interval time.Duration, logger logging.Logger) {
	if interval == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.Flush(ctx); err != nil {
					logger.WithError(err).Error("failed to flush storage accountant")
				}
			}
		}
	}()
}

// Flush drains the in-memory deltas into the KV store. Each affected repo counter is updated via
// SetIf with predicate retry. The corresponding per-user denormalized total is updated in the same
// flush. If a per-repo write fails after the delta has been removed from memory, the failed delta
// is re-added to the map so it will be retried on the next flush.
func (a *StorageAccountant) Flush(ctx context.Context) error {
	a.mu.Lock()
	if len(a.deltas) == 0 {
		a.mu.Unlock()
		return nil
	}
	pending := a.deltas
	a.deltas = make(map[accountantKey]int64)
	a.mu.Unlock()

	// Aggregate per-user totals for this flush.
	perUser := map[string]int64{}
	var errs []error
	for k, delta := range pending {
		if err := a.applyDelta(ctx, StorageRepoKey(k.owner, k.repo), delta); err != nil {
			// Re-add unflushed delta so the next flush retries it.
			a.mu.Lock()
			a.deltas[k] += delta
			a.mu.Unlock()
			errs = append(errs, fmt.Errorf("repo %s/%s: %w", k.owner, k.repo, err))
			continue
		}
		perUser[k.owner] += delta
	}
	for owner, delta := range perUser {
		if err := a.applyDelta(ctx, StorageUserKey(owner), delta); err != nil {
			errs = append(errs, fmt.Errorf("user %s: %w", owner, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// applyDelta increments the int64 value at key by delta using SetIf + predicate retry.
// If the key does not exist, it is created with the delta value.
func (a *StorageAccountant) applyDelta(ctx context.Context, key []byte, delta int64) error {
	const retryDuration = 200 * time.Millisecond
	bo := NewConstantWithJitterBackOff(retryDuration)
	return backoff.Retry(func() error {
		var predicate kv.Predicate
		current := int64(0)
		got, err := a.storage.Get(ctx, StoragePartition, key)
		if err != nil {
			if !errors.Is(err, kv.ErrNotFound) {
				return backoff.Permanent(err)
			}
		} else {
			predicate = got.Predicate
			n, parseErr := strconv.ParseInt(string(got.Value), 10, 64)
			if parseErr != nil {
				return backoff.Permanent(fmt.Errorf("parse counter %q: %w", key, parseErr))
			}
			current = n
		}
		newValue := strconv.FormatInt(current+delta, 10)
		err = a.storage.SetIf(ctx, StoragePartition, key, []byte(newValue), predicate)
		if err != nil {
			if errors.Is(err, kv.ErrPredicateFailed) {
				return err // retry
			}
			return backoff.Permanent(err)
		}
		return nil
	}, backoff.WithContext(bo, ctx))
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageAccountant -v
```

Expected: PASS for all four `TestStorageAccountant_*` cases.

- [ ] **Step 5: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_accountant.go pkg/stats/storage_accountant_test.go
git -C /work/surogate-hub commit -m "stats: add StorageAccountant for per-user byte deltas"
```

---

## Task 3: `pkg/stats/storage_quota.go` — QuotaChecker

**Files:**
- Create: `/work/surogate-hub/pkg/stats/storage_quota.go`
- Test: `/work/surogate-hub/pkg/stats/storage_quota_test.go`

- [ ] **Step 1: Write the failing test**

`/work/surogate-hub/pkg/stats/storage_quota_test.go`:

```go
package stats_test

import (
	"context"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestQuotaChecker_AbsentMeansUnlimited(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)

	dec, err := q.Allow(ctx, "alice", 999999999)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW with no quota set; got %+v", dec)
	}
	if dec.QuotaSet {
		t.Errorf("expected QuotaSet=false for unlimited user")
	}
}

func TestQuotaChecker_UnderQuotaAllows(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "x", 400)
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dec, err := q.Allow(ctx, "alice", 500)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW (400+500 ≤ 1000); got %+v", dec)
	}
}

func TestQuotaChecker_OverQuotaRejects(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "x", 900)
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dec, err := q.Allow(ctx, "alice", 200)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if dec.Allowed {
		t.Errorf("expected REJECT (900+200 > 1000); got %+v", dec)
	}
	if dec.QuotaBytes != 1000 || dec.BytesUsed != 900 {
		t.Errorf("decision payload = %+v, want quota=1000 used=900", dec)
	}
}

func TestQuotaChecker_UnknownSizeRejectedWhenQuotaSet(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", 1000); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	dec, err := q.Allow(ctx, "alice", -1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if dec.Allowed {
		t.Errorf("expected REJECT for unknown size with quota set")
	}
	if dec.Reason != stats.QuotaReasonUnknownSize {
		t.Errorf("Reason = %v, want QuotaReasonUnknownSize", dec.Reason)
	}
}

func TestQuotaChecker_UnknownSizeAllowedWhenUnlimited(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	dec, err := q.Allow(ctx, "alice", -1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !dec.Allowed {
		t.Errorf("expected ALLOW for unknown size without quota; got %+v", dec)
	}
}

func TestQuotaChecker_SetGetClear(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)

	if _, ok, err := q.GetQuota(ctx, "alice"); err != nil || ok {
		t.Errorf("GetQuota empty: (_, ok=%v, err=%v)", ok, err)
	}
	if err := q.SetQuota(ctx, "alice", 12345); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	v, ok, err := q.GetQuota(ctx, "alice")
	if err != nil || !ok || v != 12345 {
		t.Errorf("GetQuota after set: (%d, ok=%v, err=%v)", v, ok, err)
	}
	if err := q.ClearQuota(ctx, "alice"); err != nil {
		t.Fatalf("ClearQuota: %v", err)
	}
	if _, ok, err := q.GetQuota(ctx, "alice"); err != nil || ok {
		t.Errorf("GetQuota after clear: ok=%v, err=%v", ok, err)
	}
}

func TestQuotaChecker_SetRejectsNegative(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	q := stats.NewQuotaChecker(store)
	if err := q.SetQuota(ctx, "alice", -1); err == nil {
		t.Errorf("expected error for negative quota")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestQuotaChecker -v
```

Expected: FAIL with `undefined: stats.NewQuotaChecker`.

- [ ] **Step 3: Write the implementation**

`/work/surogate-hub/pkg/stats/storage_quota.go`:

```go
package stats

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

// QuotaReason describes why a QuotaDecision is allowed or rejected.
type QuotaReason int

const (
	QuotaReasonUnlimited QuotaReason = iota
	QuotaReasonUnderLimit
	QuotaReasonOverLimit
	QuotaReasonUnknownSize
)

// QuotaDecision is the result of QuotaChecker.Allow.
type QuotaDecision struct {
	Allowed    bool
	QuotaSet   bool
	QuotaBytes int64
	BytesUsed  int64
	Reason     QuotaReason
}

// ErrInvalidQuota is returned by SetQuota when the quota_bytes is negative.
var ErrInvalidQuota = errors.New("quota_bytes must be non-negative")

// QuotaChecker reads (and admin code writes) per-user storage quotas.
type QuotaChecker struct {
	storage kv.Store
}

// NewQuotaChecker constructs a QuotaChecker backed by the given KV store.
func NewQuotaChecker(storage kv.Store) *QuotaChecker {
	return &QuotaChecker{storage: storage}
}

// Allow checks whether the owner has capacity for contentLength more bytes.
//
// If no quota is set for the owner, the request is always allowed.
// If quota is set and contentLength < 0 (unknown size), the request is rejected with
// QuotaReasonUnknownSize, because we cannot guarantee the upload will fit.
// If quota is set and contentLength is known, the request is allowed iff used+contentLength <= quota.
//
// The used figure is read from the last-flushed user counter (soft check, see spec).
func (q *QuotaChecker) Allow(ctx context.Context, owner string, contentLength int64) (QuotaDecision, error) {
	quota, ok, err := q.GetQuota(ctx, owner)
	if err != nil {
		return QuotaDecision{}, err
	}
	if !ok {
		return QuotaDecision{Allowed: true, Reason: QuotaReasonUnlimited}, nil
	}
	if contentLength < 0 {
		return QuotaDecision{
			Allowed:    false,
			QuotaSet:   true,
			QuotaBytes: quota,
			Reason:     QuotaReasonUnknownSize,
		}, nil
	}
	used, err := q.readUserUsage(ctx, owner)
	if err != nil {
		return QuotaDecision{}, err
	}
	dec := QuotaDecision{
		QuotaSet:   true,
		QuotaBytes: quota,
		BytesUsed:  used,
	}
	if used+contentLength > quota {
		dec.Allowed = false
		dec.Reason = QuotaReasonOverLimit
	} else {
		dec.Allowed = true
		dec.Reason = QuotaReasonUnderLimit
	}
	return dec, nil
}

// GetQuota returns the quota for owner. The bool is false when no quota is configured.
func (q *QuotaChecker) GetQuota(ctx context.Context, owner string) (int64, bool, error) {
	got, err := q.storage.Get(ctx, StoragePartition, StorageQuotaKey(owner))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse quota for %q: %w", owner, err)
	}
	return n, true, nil
}

// SetQuota stores a new quota for owner. quotaBytes must be ≥ 0.
func (q *QuotaChecker) SetQuota(ctx context.Context, owner string, quotaBytes int64) error {
	if quotaBytes < 0 {
		return ErrInvalidQuota
	}
	return q.storage.Set(ctx, StoragePartition, StorageQuotaKey(owner), []byte(strconv.FormatInt(quotaBytes, 10)))
}

// ClearQuota removes the quota for owner, reverting them to unlimited. Returns nil if absent.
func (q *QuotaChecker) ClearQuota(ctx context.Context, owner string) error {
	err := q.storage.Delete(ctx, StoragePartition, StorageQuotaKey(owner))
	if errors.Is(err, kv.ErrNotFound) {
		return nil
	}
	return err
}

func (q *QuotaChecker) readUserUsage(ctx context.Context, owner string) (int64, error) {
	got, err := q.storage.Get(ctx, StoragePartition, StorageUserKey(owner))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(got.Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse user usage for %q: %w", owner, err)
	}
	return n, err
}
```

`kv.Store.Delete(ctx, partition, key)` exists and is idempotent for missing keys, so `ClearQuota` should call it directly.

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestQuotaChecker -v
```

Expected: PASS for all `TestQuotaChecker_*` cases.

- [ ] **Step 5: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_quota.go pkg/stats/storage_quota_test.go
git -C /work/surogate-hub commit -m "stats: add QuotaChecker for per-user storage quotas"
```

---

## Task 4: Add `auth:WriteUser` permission

**Files:**
- Modify: `/work/surogate-hub/pkg/permissions/actions.go`

- [ ] **Step 1: Add the constant**

In `/work/surogate-hub/pkg/permissions/actions.go`, inside the `const ( ... )` block (around line 42, immediately after `ReadUserAction`), add:

```go
WriteUserAction                           = "auth:WriteUser"
```

The complete relevant lines after the edit:

```go
ReadUserAction                            = "auth:ReadUser"
WriteUserAction                           = "auth:WriteUser"
CreateUserAction                          = "auth:CreateUser"
```

- [ ] **Step 2: Regenerate actions.gen.go**

```bash
cd /work/surogate-hub && go generate ./pkg/permissions/
```

Expected: file `pkg/permissions/actions.gen.go` updated to include `WriteUserAction`.

- [ ] **Step 3: Build to verify**

```bash
cd /work/surogate-hub && go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git -C /work/surogate-hub add pkg/permissions/actions.go pkg/permissions/actions.gen.go
git -C /work/surogate-hub commit -m "permissions: add auth:WriteUser action"
```

---

## Task 5: Add `storage_usage` config block

**Files:**
- Modify: `/work/surogate-hub/pkg/config/config.go`
- Modify: `/work/surogate-hub/pkg/config/defaults.go`

- [ ] **Step 1: Add the struct in `pkg/config/config.go`**

Open `/work/surogate-hub/pkg/config/config.go`. Find the `UsageReport` field (around line 547). Immediately after the closing `}` of the `UsageReport` block, add:

```go
StorageUsage struct {
    Enabled bool `mapstructure:"enabled"`
    StorageAccountant struct {
        FlushInterval time.Duration `mapstructure:"flush_interval"`
    } `mapstructure:"storage_accountant"`
    StorageReconciler struct {
        Interval    time.Duration `mapstructure:"interval"`
        Concurrency int           `mapstructure:"concurrency"`
    } `mapstructure:"storage_reconciler"`
} `mapstructure:"storage_usage"`
```

The block after the edit looks like:

```go
UsageReport struct {
    Enabled       bool          `mapstructure:"enabled"`
    FlushInterval time.Duration `mapstructure:"flush_interval"`
} `mapstructure:"usage_report"`
StorageUsage struct {
    Enabled bool `mapstructure:"enabled"`
    StorageAccountant struct {
        FlushInterval time.Duration `mapstructure:"flush_interval"`
    } `mapstructure:"storage_accountant"`
    StorageReconciler struct {
        Interval    time.Duration `mapstructure:"interval"`
        Concurrency int           `mapstructure:"concurrency"`
    } `mapstructure:"storage_reconciler"`
} `mapstructure:"storage_usage"`
```

- [ ] **Step 2: Add defaults in `pkg/config/defaults.go`**

Open `/work/surogate-hub/pkg/config/defaults.go`. Find the `usage_report.flush_interval` default. Immediately after it, add:

```go
viper.SetDefault("storage_usage.enabled", false)
viper.SetDefault("storage_usage.storage_accountant.flush_interval", 5*time.Second)
viper.SetDefault("storage_usage.storage_reconciler.interval", time.Hour)
viper.SetDefault("storage_usage.storage_reconciler.concurrency", 4)
```

- [ ] **Step 3: Build**

```bash
cd /work/surogate-hub && go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git -C /work/surogate-hub add pkg/config/config.go pkg/config/defaults.go
git -C /work/surogate-hub commit -m "config: add storage_usage block"
```

---

## Task 6: `pkg/stats/storage_reconciler.go` — block-store walker

**Files:**
- Create: `/work/surogate-hub/pkg/stats/storage_reconciler.go`
- Test: `/work/surogate-hub/pkg/stats/storage_reconciler_test.go`

This task introduces a couple of small interfaces to keep the reconciler testable: a `RepoLister` and a `NamespaceSizer`. The production implementation wraps `catalog.Catalog` and `block.Adapter` (via `GetWalker`).

- [ ] **Step 1: Write the failing test**

`/work/surogate-hub/pkg/stats/storage_reconciler_test.go`:

```go
package stats_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

type fakeRepoLister struct {
	repos []stats.ReconcilerRepo
}

func (f *fakeRepoLister) ListRepos(ctx context.Context, fn func(stats.ReconcilerRepo) error) error {
	for _, r := range f.repos {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

type fakeNamespaceSizer struct {
	sizes map[string]int64
	err   error
}

func (f *fakeNamespaceSizer) NamespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.sizes[storageNamespace], nil
}

func TestStorageReconciler_OverwritesRepoCountersFromWalker(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	// Pre-populate stale counters (drift).
	if err := store.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("999")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "training", StorageID: "s1", StorageNamespace: "s3://alice-training"},
		{Owner: "alice", Name: "evals", StorageID: "s1", StorageNamespace: "s3://alice-evals"},
	}}
	ns := &fakeNamespaceSizer{sizes: map[string]int64{
		"s3://alice-training": 150,
		"s3://alice-evals":    200,
	}}

	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 150)
	requireInt64(t, store, stats.StorageRepoKey("alice", "evals"), 200)
	requireInt64(t, store, stats.StorageUserKey("alice"), 350)

	// last_reconciled_at written.
	got, err := store.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey("alice"))
	if err != nil {
		t.Fatalf("last_reconciled_at missing: %v", err)
	}
	if len(got.Value) == 0 {
		t.Errorf("last_reconciled_at empty")
	}
}

func TestStorageReconciler_FlushesPendingAccountantDeltasBeforeOverwrite(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	a := stats.NewStorageAccountant(store)
	a.Add(ctx, "alice", "training", 50) // pending delta, not flushed

	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "training", StorageID: "s1", StorageNamespace: "s3://alice-training"},
	}}
	ns := &fakeNamespaceSizer{sizes: map[string]int64{"s3://alice-training": 100}}

	r := stats.NewStorageReconciler(store, rl, ns, a)
	if err := r.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Reconciler value (100) wins over accountant delta because flush happens before write.
	requireInt64(t, store, stats.StorageRepoKey("alice", "training"), 100)
	requireInt64(t, store, stats.StorageUserKey("alice"), 100)
}

func TestStorageReconciler_ContinuesOnNamespaceError(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	rl := &fakeRepoLister{repos: []stats.ReconcilerRepo{
		{Owner: "alice", Name: "ok", StorageID: "s1", StorageNamespace: "ok"},
		{Owner: "alice", Name: "bad", StorageID: "s1", StorageNamespace: "bad"},
	}}
	ns := &errInjectingSizer{sizes: map[string]int64{"ok": 7}}
	r := stats.NewStorageReconciler(store, rl, ns, stats.NewStorageAccountant(store))
	if err := r.RunOnce(ctx); err == nil {
		t.Errorf("expected aggregated error from bad repo")
	}
	requireInt64(t, store, stats.StorageRepoKey("alice", "ok"), 7)
}

type errInjectingSizer struct {
	sizes map[string]int64
}

func (e *errInjectingSizer) NamespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error) {
	if v, ok := e.sizes[storageNamespace]; ok {
		return v, nil
	}
	return 0, &testErr{}
}

type testErr struct{}

func (*testErr) Error() string { return "boom" }
```

(`requireInt64` is defined in `storage_accountant_test.go`; in the same package, no re-declaration needed.)

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageReconciler -v
```

Expected: FAIL with `undefined: stats.NewStorageReconciler`.

- [ ] **Step 3: Write the implementation**

`/work/surogate-hub/pkg/stats/storage_reconciler.go`:

```go
package stats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
)

// ReconcilerRepo is the minimal repo description the reconciler needs.
type ReconcilerRepo struct {
	Owner            string
	Name             string
	StorageID        string
	StorageNamespace string
}

// RepoLister enumerates repositories for the reconciler. Implementations may stream or paginate.
type RepoLister interface {
	ListRepos(ctx context.Context, fn func(ReconcilerRepo) error) error
}

// NamespaceSizer reports the total bytes allocated in a repo's storage namespace.
type NamespaceSizer interface {
	NamespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error)
}

// StorageReconciler walks every repo, asks the block adapter how many bytes are allocated, and
// overwrites the per-repo counter accordingly. After all of an owner's repos are processed, the
// per-user denormalized total is rewritten from the sum of repo counters.
type StorageReconciler struct {
	storage     kv.Store
	lister      RepoLister
	sizer       NamespaceSizer
	accountant  *StorageAccountant
	concurrency int

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-repo locks, keyed by "owner/name"
}

// NewStorageReconciler constructs a reconciler. Defaults to single-threaded; set Concurrency to
// fan out across repos.
func NewStorageReconciler(storage kv.Store, lister RepoLister, sizer NamespaceSizer, accountant *StorageAccountant) *StorageReconciler {
	return &StorageReconciler{
		storage:     storage,
		lister:      lister,
		sizer:       sizer,
		accountant:  accountant,
		concurrency: 1,
		locks:       make(map[string]*sync.Mutex),
	}
}

// WithConcurrency sets how many repos to reconcile in parallel. Returns the reconciler for chaining.
func (r *StorageReconciler) WithConcurrency(n int) *StorageReconciler {
	if n < 1 {
		n = 1
	}
	r.concurrency = n
	return r
}

// Start launches a goroutine that calls RunOnce every interval until ctx is cancelled. If a pass
// takes longer than the interval, the next pass starts immediately after the previous one finishes.
func (r *StorageReconciler) Start(ctx context.Context, interval time.Duration, logger logging.Logger) {
	if interval == 0 {
		return
	}
	go func() {
		for {
			start := time.Now()
			if err := r.RunOnce(ctx); err != nil {
				logger.WithError(err).Error("storage reconciler pass failed")
			}
			elapsed := time.Since(start)
			delay := interval - elapsed
			if delay < 0 {
				delay = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}()
}

// RunOnce reconciles every repo once. Errors are collected and returned as a joined error after
// every repo has been attempted.
func (r *StorageReconciler) RunOnce(ctx context.Context) error {
	// Flush accountant deltas before walking, so any pending writes are reflected in the
	// per-user counter we will compute by summing repo counters.
	if err := r.accountant.Flush(ctx); err != nil {
		// Continue — reconciler will overwrite per-repo values anyway.
	}

	type repoResult struct {
		owner string
		err   error
	}
	results := make(chan repoResult, r.concurrency)
	work := make(chan ReconcilerRepo)

	var wg sync.WaitGroup
	for i := 0; i < r.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repo := range work {
				err := r.reconcileRepo(ctx, repo)
				results <- repoResult{owner: repo.Owner, err: err}
			}
		}()
	}

	listerDone := make(chan error, 1)
	go func() {
		listerDone <- r.lister.ListRepos(ctx, func(repo ReconcilerRepo) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case work <- repo:
				return nil
			}
		})
		close(work)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	ownersTouched := make(map[string]struct{})
	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, fmt.Errorf("reconcile %s: %w", res.owner, res.err))
		}
		ownersTouched[res.owner] = struct{}{}
	}
	if err := <-listerDone; err != nil {
		errs = append(errs, fmt.Errorf("list repos: %w", err))
	}

	// Recompute per-user totals and write last_reconciled_at.
	now := time.Now().UTC()
	for owner := range ownersTouched {
		if err := r.recomputeUserTotal(ctx, owner, now); err != nil {
			errs = append(errs, fmt.Errorf("user total %s: %w", owner, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *StorageReconciler) repoLock(owner, name string) *sync.Mutex {
	key := owner + "/" + name
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.locks[key]; ok {
		return m
	}
	m := &sync.Mutex{}
	r.locks[key] = m
	return m
}

func (r *StorageReconciler) reconcileRepo(ctx context.Context, repo ReconcilerRepo) error {
	lock := r.repoLock(repo.Owner, repo.Name)
	lock.Lock()
	defer lock.Unlock()

	size, err := r.sizer.NamespaceSize(ctx, repo.StorageID, repo.StorageNamespace)
	if err != nil {
		return err
	}
	return r.storage.Set(ctx, StoragePartition, StorageRepoKey(repo.Owner, repo.Name), []byte(strconv.FormatInt(size, 10)))
}

func (r *StorageReconciler) recomputeUserTotal(ctx context.Context, owner string, now time.Time) error {
	iter, err := r.storage.Scan(ctx, StoragePartition, kv.ScanOptions{KeyStart: StorageRepoPrefix(owner)})
	if err != nil {
		return err
	}
	defer iter.Close()
	prefix := string(StorageRepoPrefix(owner))
	var total int64
	for iter.Next() {
		ent := iter.Entry()
		if !startsWithBytes(ent.Key, []byte(prefix)) {
			break
		}
		n, parseErr := strconv.ParseInt(string(ent.Value), 10, 64)
		if parseErr != nil {
			return fmt.Errorf("parse %q: %w", ent.Key, parseErr)
		}
		total += n
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if err := r.storage.Set(ctx, StoragePartition, StorageUserKey(owner), []byte(strconv.FormatInt(total, 10))); err != nil {
		return err
	}
	return r.storage.Set(ctx, StoragePartition, StorageMetaLastReconciledAtKey(owner), []byte(now.Format(time.RFC3339)))
}

func startsWithBytes(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}
```

`kv.Store.Scan(..., kv.ScanOptions{KeyStart: prefix})` starts scanning at the prefix; the loop manually checks the prefix to stop when keys leave the namespace.

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /work/surogate-hub && go test ./pkg/stats/ -run TestStorageReconciler -v
```

Expected: PASS for all `TestStorageReconciler_*` cases.

- [ ] **Step 5: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_reconciler.go pkg/stats/storage_reconciler_test.go
git -C /work/surogate-hub commit -m "stats: add StorageReconciler driven by block-store walker"
```

---

## Task 7: Production `RepoLister` and `NamespaceSizer` adapters

These live in `pkg/stats` and wrap the catalog and block adapter. They are thin and depend on existing production types; tests for them are unnecessary because they only delegate.

**Files:**
- Create: `/work/surogate-hub/pkg/stats/storage_adapters.go`

- [ ] **Step 1: Write the adapter**

`/work/surogate-hub/pkg/stats/storage_adapters.go`:

```go
package stats

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/catalog"
)

// CatalogRepoLister implements RepoLister by enumerating repositories from the catalog.
type CatalogRepoLister struct {
	Catalog *catalog.Catalog
}

func (l *CatalogRepoLister) ListRepos(ctx context.Context, fn func(ReconcilerRepo) error) error {
	const pageSize = 100
	var after string
	for {
		repos, hasMore, err := l.Catalog.ListRepositories(ctx, pageSize, "", "", after)
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}
		for _, repo := range repos {
			after = repo.Name
			owner, name, splitErr := splitNamespacedRepo(repo.Name)
			if splitErr != nil {
				// A repo whose id is not in {owner}/{name} form is not part of this scheme.
				continue
			}
			if err := fn(ReconcilerRepo{
				Owner:            owner,
				Name:             name,
				StorageID:        repo.StorageID,
				StorageNamespace: repo.StorageNamespace,
			}); err != nil {
				return err
			}
		}
		if !hasMore {
			return nil
		}
	}
}

func splitNamespacedRepo(id string) (owner, name string, err error) {
	i := strings.IndexByte(id, '/')
	if i <= 0 || i == len(id)-1 {
		return "", "", fmt.Errorf("not an owner/name id: %q", id)
	}
	return id[:i], id[i+1:], nil
}

// SplitNamespacedRepo exposes the owner/name splitter to API and gateway packages.
func SplitNamespacedRepo(id string) (owner, name string, err error) {
	return splitNamespacedRepo(id)
}

// BlockNamespaceSizer implements NamespaceSizer by walking the block adapter.
type BlockNamespaceSizer struct {
	Adapter block.Adapter
}

func (s *BlockNamespaceSizer) NamespaceSize(ctx context.Context, storageID, storageNamespace string) (int64, error) {
	storageURI, err := url.Parse(storageNamespace)
	if err != nil {
		return 0, fmt.Errorf("parse storage namespace %s: %w", storageNamespace, err)
	}
	walker, err := s.Adapter.GetWalker(storageID, block.WalkerOptions{StorageURI: storageURI})
	if err != nil {
		return 0, fmt.Errorf("get walker for %s: %w", storageNamespace, err)
	}
	var total int64
	walkErr := walker.Walk(ctx, storageURI, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
		total += e.Size
		return nil
	})
	if walkErr != nil {
		return 0, fmt.Errorf("walk %s: %w", storageNamespace, walkErr)
	}
	return total, nil
}
```

Add `net/url` to the imports. The current signatures are:

- `catalog.Catalog.ListRepositories(ctx, limit, prefix, searchString, after)`
- `block.WalkerOptions{StorageURI: *url.URL}`
- `Walker.Walk(ctx, storageURI *url.URL, op block.WalkOptions, fn)`

- [ ] **Step 2: Build**

```bash
cd /work/surogate-hub && go build ./pkg/stats/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_adapters.go
git -C /work/surogate-hub commit -m "stats: add catalog + block-adapter wiring for reconciler"
```

---

## Task 8: Prepare runtime wiring in `cmd/sghub/cmd/run.go`

**Files:**
- Modify: `/work/surogate-hub/cmd/sghub/cmd/run.go`

- [ ] **Step 1: Edit the lifecycle block**

In `/work/surogate-hub/cmd/sghub/cmd/run.go`, find the `usage report setup` block (around lines 151–157). Immediately after it, add:

```go
// storage usage setup
var storageAccountant *stats.StorageAccountant
var quotaChecker *stats.QuotaChecker
if baseCfg.StorageUsage.Enabled {
    storageAccountant = stats.NewStorageAccountant(kvStore)
    storageAccountant.Start(ctx, baseCfg.StorageUsage.StorageAccountant.FlushInterval, logger.WithField("service", "storage_accountant"))
    quotaChecker = stats.NewQuotaChecker(kvStore)
    reconciler := stats.NewStorageReconciler(
        kvStore,
        &stats.CatalogRepoLister{Catalog: c},
        &stats.BlockNamespaceSizer{Adapter: blockStore},
        storageAccountant,
    ).WithConcurrency(baseCfg.StorageUsage.StorageReconciler.Concurrency)
    reconciler.Start(ctx, baseCfg.StorageUsage.StorageReconciler.Interval, logger.WithField("service", "storage_reconciler"))
}
```

The current local variable names are `blockStore` for the block adapter and `c` for the catalog.

Do not leave these variables unused. In the same edit batch, perform Task 9 so `api.Serve(...)` and `gateway.NewHandler(...)` accept and receive `storageAccountant` and `quotaChecker`.

- [ ] **Step 2: Build**

```bash
cd /work/surogate-hub && go build ./cmd/sghub/...
```

Expected: no errors after Task 9 is done in the same edit batch.

- [ ] **Step 3: Commit (deferred — combine with Task 9)**

Do not commit on its own; commit together with Task 9.

---

## Task 8.5: Extend the test fixture for storage usage

**Files:**
- Modify: `/work/surogate-hub/pkg/api/serve_test.go`

Add the `withStorageAccountant()` option and surface accountant/reconciler/KV on `*dependencies` so subsequent test tasks can use them. The current fixture has `blocks block.Adapter`, `catalog *catalog.Catalog`, and no KV field, so add `kvStore kv.Store`.

- [ ] **Step 1: Add option fn**

In `serve_test.go`, alongside existing `setupHandler`/`setupClientWithAdmin` helpers, add:

```go
type setupOption func(*dependencies)

func withStorageAccountant() setupOption {
    return func(d *dependencies) {
        d.storageAccountant = stats.NewStorageAccountant(d.kvStore)
        d.quotaChecker = stats.NewQuotaChecker(d.kvStore)
        // Reconciler with a fake lister that drains `d.catalog`; sizer wraps the in-memory block adapter.
        d.storageReconciler = stats.NewStorageReconciler(
            d.kvStore,
            &stats.CatalogRepoLister{Catalog: d.catalog},
            &stats.BlockNamespaceSizer{Adapter: d.blocks},
            d.storageAccountant,
        )
    }
}
```

Extend `*dependencies` to expose:

```go
storageAccountant *stats.StorageAccountant
quotaChecker      *stats.QuotaChecker
storageReconciler *stats.StorageReconciler
kvStore           kv.Store
```

Set `kvStore: kvStore` in the `dependencies` return value from `setupHandler`. Keep using the existing `blocks: c.BlockAdapter` field for the block adapter.

Extend `setupHandler(t, opts ...setupOption)` to apply options after defaults, and pass `d.storageAccountant` and `d.quotaChecker` into the controller constructor.

Extend `setupClientWithAdmin(t, opts ...setupOption)` to forward options through to `setupHandler`.

- [ ] **Step 2: Add `clientAs` helper**

```go
// clientAs creates a non-admin user with the given username and returns a client authenticated as them.
func clientAs(t testing.TB, deps *dependencies, username string) apigen.ClientWithResponsesInterface {
    t.Helper()
    ctx := context.Background()
    _, err := deps.authService.CreateUser(ctx, &authmodel.User{Username: username})
    require.NoError(t, err)
    cred, err := deps.authService.CreateCredentials(ctx, username)
    require.NoError(t, err)
    return setupClientByEndpoint(t, deps.server.URL, cred.AccessKeyID, cred.SecretAccessKey)
}
```

Add `authmodel "github.com/invergent-ai/surogate-hub/pkg/auth/model"` to imports if the file does not already have that alias.

- [ ] **Step 3: Build**

```bash
cd /work/surogate-hub && go build ./pkg/api/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git -C /work/surogate-hub add pkg/api/serve_test.go
git -C /work/surogate-hub commit -m "test: add withStorageAccountant option and clientAs helper"
```

---

## Task 9: Inject accountant + quota checker into the controller

**Files:**
- Modify: `/work/surogate-hub/pkg/api/controller.go`
- Modify: `/work/surogate-hub/pkg/api/serve.go`
- Modify: `/work/surogate-hub/pkg/api/serve_test.go`
- Modify: `/work/surogate-hub/cmd/sghub/cmd/run.go`

- [ ] **Step 1: Add fields and constructor parameters**

In `pkg/api/controller.go`, find the `Controller` struct definition (search `type Controller struct`). Add two fields:

```go
StorageAccountant *stats.StorageAccountant
QuotaChecker      *stats.QuotaChecker
```

(These may be `nil` when `storage_usage.enabled = false`; every call site must guard.)

Add import for `github.com/invergent-ai/surogate-hub/pkg/stats` if not already present.

In `pkg/api/serve.go`, extend `Serve(...)` with two parameters of the same types and pass them to `NewController(...)`. In `pkg/api/controller.go`, extend `NewController(...)` with those parameters and assign them onto the controller fields.

In `cmd/sghub/cmd/run.go`, pass `storageAccountant` and `quotaChecker` into that constructor.

In `pkg/api/serve_test.go`, in `setupHandler`, pass `nil, nil` for now (tests that need the accountant will pass a real one in later tasks).

Also extend the S3 gateway dependency path in the same edit:

- `pkg/gateway/handler.go`: add `storageAccountant *stats.StorageAccountant, quotaChecker *stats.QuotaChecker` to `NewHandler(...)` and `ServerContext`.
- `pkg/gateway/middleware.go`: copy those fields from `ServerContext` into the `operations.Operation` constructed in `EnrichWithOperation`.
- `pkg/gateway/operations/base.go`: add `StorageAccountant *stats.StorageAccountant` and `QuotaChecker *stats.QuotaChecker` to `Operation`.
- `cmd/sghub/cmd/run.go`: pass the same two variables to `gateway.NewHandler(...)`.

- [ ] **Step 2: Build**

```bash
cd /work/surogate-hub && go build ./...
```

Expected: no errors.

- [ ] **Step 3: Sanity-run the existing test suite**

```bash
cd /work/surogate-hub && go test ./pkg/api/... ./pkg/stats/... -count=1
```

Expected: all existing tests still PASS.

- [ ] **Step 4: Commit (with Task 8)**

```bash
git -C /work/surogate-hub add cmd/sghub/cmd/run.go pkg/api/controller.go pkg/api/serve.go pkg/api/serve_test.go
git -C /work/surogate-hub commit -m "api: wire StorageAccountant and QuotaChecker into Controller"
```

---

## Task 10: Hook accountant into single-object upload paths

**Files:**
- Modify: `/work/surogate-hub/pkg/api/controller.go`
- Modify: `/work/surogate-hub/pkg/api/controller_test.go`

Targets: `Controller.UploadObject`, `Controller.CompletePresignMultipartUpload`, and `Controller.CopyObject`.

- [ ] **Step 1: Write the failing test**

In `/work/surogate-hub/pkg/api/controller_test.go`, add:

```go
func TestUploadObject_IncrementsStorageCounter(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)

	body := bytes.NewReader([]byte("hello world"))
	resp, err := clt.UploadObjectWithBody(ctx, "alice", "training", "main", &apigen.UploadObjectParams{Path: "a.txt"}, "application/octet-stream", body)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	require.NoError(t, deps.storageAccountant.Flush(ctx))
	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"))
	require.NoError(t, err)
	require.Equal(t, "11", string(got.Value))
}
```

`withStorageAccountant()` and `deps.storageAccountant` / `deps.kvStore` were added in Task 8.5. Use explicit mem namespaces in tests, for example `mem://alice-training`.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestUploadObject_IncrementsStorageCounter -v
```

Expected: FAIL. The counter is never written because the upload path does not call the accountant yet.

- [ ] **Step 3: Add the hook in `UploadObject`**

In `controller.go` `UploadObject` (around line 3398, after `upload.WriteBlob` returns successfully and before `writeResponse`), add:

```go
if c.StorageAccountant != nil {
    _, repoName, _ := stats.SplitNamespacedRepo(repository)
    c.StorageAccountant.Add(ctx, owner, repoName, blob.Size)
}
```

`repository` at this point is `namespacedRepository(owner, repo)`, i.e. `"alice/training"`.

For the multipart branch (else case) the `blob` is set similarly — perform the same call after the multipart path assigns `blob`.

Do not add a generic accountant hook to `StageObject`: it only creates metadata for a supplied physical address and can be called repeatedly for the same object. Counting there would double count aliases. For presigned multipart uploads, count in `CompletePresignMultipartUpload` using `mpuResp.ContentLength`, immediately after `BlockAdapter.CompleteMultiPartUpload` succeeds and before the entry is created. A failed entry create after completion may leave drift; the reconciler corrects it.

For `CopyObject`: after the catalog `CopyEntry` returns successfully, call the accountant with the source entry's size (available from `srcEntry.Size`).

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestUploadObject_IncrementsStorageCounter -v
```

Expected: PASS.

- [ ] **Step 5: Add equivalent tests for CompletePresignMultipartUpload and CopyObject** following the same template.

Write each, run it (FAIL), add the hook, re-run (PASS).

- [ ] **Step 6: Commit**

```bash
git -C /work/surogate-hub add pkg/api/controller.go pkg/api/controller_test.go pkg/api/serve_test.go pkg/stats/storage_adapters.go
git -C /work/surogate-hub commit -m "api: count bytes from upload, presign multipart, and copy"
```

---

## Task 11: Hook accountant into gateway upload paths

**Files:**
- Modify: `/work/surogate-hub/pkg/gateway/operations/putobject.go`
- Modify: `/work/surogate-hub/pkg/gateway/operations/putobject.go`
- Modify: `/work/surogate-hub/pkg/gateway/operations/postobject.go`
- Modify: tests under `pkg/gateway/operations/` covering the same paths

Same shape as Task 10: write a failing test that PUTs via the S3 gateway and checks the counter incremented, add the hook, repeat for multipart.

- [ ] **Step 1: Identify the entry struct**

The gateway dependencies were added in Task 9: `gateway.ServerContext` carries them into `operations.Operation` through `EnrichWithOperation`.

- [ ] **Step 2: Per-path test + hook**

For each of `handlePut`, `handleUploadPart` body upload, `handleUploadPart` copy/range-copy, `PostObject.HandleCompleteMultipartUpload`, and `handleCopy`:

1. Write a failing gateway integration test that exercises the path and asserts the counter increments by the bytes actually written. Reuse the existing gateway test fixtures in `pkg/gateway/operations/*_test.go`.
2. Add the hook after the block-store call succeeds.
3. Use `block.UploadPartResponse` is silent on size — pass the `sizeBytes` parameter the gateway already received from the request.
4. For `CompleteMultiPartUpload`, use the `ContentLength` field from the response (see `block.CompleteMultiPartUploadResponse.ContentLength`).
5. Run the test (PASS).

- [ ] **Step 3: Commit**

```bash
git -C /work/surogate-hub add pkg/gateway/operations cmd/sghub/cmd/run.go pkg/gateway/...
git -C /work/surogate-hub commit -m "gateway: count bytes from put/multipart/copy operations"
```

---

## Task 12: Hook accountant into repository create and delete

**Files:**
- Modify: `/work/surogate-hub/pkg/api/controller.go` (CreateRepository, DeleteRepository handler methods — search `func (c *Controller) CreateRepository` and `DeleteRepository`)
- Modify: `/work/surogate-hub/pkg/api/controller_test.go`
- Modify: `/work/surogate-hub/pkg/stats/storage_accountant.go`
- Modify: `/work/surogate-hub/pkg/stats/storage_accountant_test.go`

- [ ] **Step 1: Write the failing test (delete-decrements)**

```go
func TestDeleteRepository_DecrementsStorageCounter(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)
	// Seed counter to a non-zero value.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("500")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("500")))

	resp, err := clt.DeleteRepositoryWithResponse(ctx, "alice", "training", &apigen.DeleteRepositoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	if _, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training")); err != kv.ErrNotFound {
		t.Errorf("repo counter should be deleted, got err=%v", err)
	}
	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey("alice"))
	require.NoError(t, err)
	require.Equal(t, "0", string(got.Value))
}
```

- [ ] **Step 2: Run, expect FAIL**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestDeleteRepository_DecrementsStorageCounter -v
```

- [ ] **Step 3: Add the hook in `Controller.DeleteRepository`**

First add this method to `pkg/stats/storage_accountant.go` so repository deletion does not recreate the repo counter with a negative value:

```go
// DeleteRepo removes the repo counter and decrements the denormalized user total by the
// repo's recorded bytes. It does not call Add(owner, repo, -n), because that would recreate
// the repo key on the next flush.
func (a *StorageAccountant) DeleteRepo(ctx context.Context, owner, repo string) error {
    a.mu.Lock()
    delete(a.deltas, accountantKey{owner: owner, repo: repo})
    a.mu.Unlock()

    key := StorageRepoKey(owner, repo)
    got, err := a.storage.Get(ctx, StoragePartition, key)
    if errors.Is(err, kv.ErrNotFound) {
        return nil
    }
    if err != nil {
        return err
    }
    current, err := strconv.ParseInt(string(got.Value), 10, 64)
    if err != nil {
        return fmt.Errorf("parse repo counter %s/%s: %w", owner, repo, err)
    }
    if err := a.storage.Delete(ctx, StoragePartition, key); err != nil {
        return err
    }
    if current == 0 {
        return nil
    }
    return a.applyDelta(ctx, StorageUserKey(owner), -current)
}
```

Add a focused unit test in `storage_accountant_test.go` that seeds `storage/repo/alice/training=500` and `storage/user/alice=500`, calls `DeleteRepo(ctx, "alice", "training")`, and asserts the repo key is gone while the user key is `0`.

Then, after the catalog `DeleteRepository` returns success and before `writeResponse(...204...)`, add:

```go
if c.StorageAccountant != nil {
    owner, repoName, splitErr := stats.SplitNamespacedRepo(repository)
    if splitErr == nil {
        if err := c.StorageAccountant.DeleteRepo(r.Context(), owner, repoName); err != nil {
            writeError(w, r, http.StatusInternalServerError, err.Error())
            return
        }
    }
}
```

No controller-level KV field is needed; storage counter KV access stays encapsulated in `StorageAccountant`.

- [ ] **Step 4: Add a hook in `Controller.CreateRepository`**

After the catalog `CreateRepository` returns success:

```go
if c.StorageAccountant != nil {
    owner, repoName, splitErr := stats.SplitNamespacedRepo(repository)
    if splitErr == nil {
        _ = c.Catalog.KVStore.Set(r.Context(), stats.StoragePartition, stats.StorageRepoKey(owner, repoName), []byte("0"))
    }
}
```

(This initialization is optional — the accountant tolerates a missing key — but it makes `GET /auth/users/{userId}/storage` list the repo with 0 bytes immediately, before any uploads.)

- [ ] **Step 5: Run, expect PASS**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestDeleteRepository_DecrementsStorageCounter -v
cd /work/surogate-hub && go test ./pkg/api/ -run TestCreateRepository -v
```

- [ ] **Step 6: Commit**

```bash
git -C /work/surogate-hub add pkg/api/controller.go pkg/api/controller_test.go pkg/stats/storage_accountant.go pkg/stats/storage_accountant_test.go
git -C /work/surogate-hub commit -m "api: counter init on repo create, decrement on repo delete"
```

---

## Task 13: Add `GET /auth/users/{userId}/storage` endpoint

**Files:**
- Modify: `/work/surogate-hub/api/swagger.yml`
- Regenerate: `/work/surogate-hub/pkg/api/apigen/sghub.gen.go`
- Modify: `/work/surogate-hub/pkg/api/controller.go`
- Modify: `/work/surogate-hub/pkg/api/controller_test.go`

- [ ] **Step 1: Add the operation to swagger**

In `/work/surogate-hub/api/swagger.yml`, add a new path block under `/auth/users/{userId}/storage` next to the other `/auth/users/{userId}/...` paths:

```yaml
  /auth/users/{userId}/storage:
    parameters:
      - in: path
        name: userId
        required: true
        schema:
          type: string
    get:
      tags:
        - auth
      operationId: getUserStorage
      summary: get a user's storage usage
      responses:
        200:
          description: storage usage
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/UserStorage"
        401:
          $ref: "#/components/responses/Unauthorized"
        403:
          $ref: "#/components/responses/Forbidden"
        404:
          $ref: "#/components/responses/NotFound"
        default:
          $ref: "#/components/responses/ServerError"
```

Under `components.schemas`, add:

```yaml
    UserStorage:
      type: object
      required:
        - user
        - bytes_used
        - repositories
        - is_estimate
      properties:
        user:
          type: string
        bytes_used:
          type: integer
          format: int64
        quota_bytes:
          type: integer
          format: int64
          nullable: true
        bytes_remaining:
          type: integer
          format: int64
          nullable: true
        repositories:
          type: array
          items:
            $ref: "#/components/schemas/UserStorageRepo"
        last_reconciled_at:
          type: string
          format: date-time
          nullable: true
        is_estimate:
          type: boolean
    UserStorageRepo:
      type: object
      required:
        - name
        - bytes_used
      properties:
        name:
          type: string
        bytes_used:
          type: integer
          format: int64
```

- [ ] **Step 2: Regenerate codegen**

```bash
cd /work/surogate-hub && go generate ./pkg/api/apigen/...
```

`pkg/api/apigen` is generated by the local `go generate` target; use the command above.

Expected: `pkg/api/apigen/sghub.gen.go` updated.

- [ ] **Step 3: Build**

```bash
cd /work/surogate-hub && go build ./...
```

Expected: compile error in `pkg/api/controller.go` because `GetUserStorage` is now in `ServerInterface` but not implemented.

- [ ] **Step 4: Write the failing handler test**

```go
func TestGetUserStorage_SelfReadsOwnCounter(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	aliceClt := clientAs(t, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("1234")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("1000")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "evals"), []byte("234")))

	resp, err := aliceClt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	require.Equal(t, int64(1234), resp.JSON200.BytesUsed)
	require.Len(t, resp.JSON200.Repositories, 2)
}

func TestGetUserStorage_NonSelfRequiresReadUser(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("100")))
	bobClt := clientAs(t, deps, "bob") // bob has no auth:ReadUser

	resp, err := bobClt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode())
}

func TestGetUserStorage_AdminCanReadOthers(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant()) // clt is admin
	_ = clientAs(t, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("100")))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.Equal(t, int64(100), resp.JSON200.BytesUsed)
}

func TestGetUserStorage_IncludesQuota(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("700")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("1000")))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200.QuotaBytes)
	require.Equal(t, int64(1000), *resp.JSON200.QuotaBytes)
	require.NotNil(t, resp.JSON200.BytesRemaining)
	require.Equal(t, int64(300), *resp.JSON200.BytesRemaining)
}
```

`clientAs(t, deps, "alice")` creates an additional user and returns a client authenticated as them. Build it on top of the existing `createDefaultAdminUser` helper. If there is no such helper for non-admin users, add one to `serve_test.go`.

- [ ] **Step 5: Implement the handler**

Add to `controller.go`:

```go
func (c *Controller) GetUserStorage(w http.ResponseWriter, r *http.Request, userID string) {
    ctx := r.Context()
    callerIsSelf := false
    if u, err := auth.GetUser(ctx); err == nil && u.Username == userID {
        callerIsSelf = true
    }
    if !callerIsSelf {
        if !c.authorize(w, r, permissions.Node{
            Permission: permissions.Permission{
                Action:   permissions.ReadUserAction,
                Resource: permissions.UserArn(userID),
            },
        }) {
            return
        }
    }
    c.LogAction(ctx, "get_user_storage", r, "", "", "")

    // 404 if user does not exist.
    if _, err := c.Auth.GetUser(ctx, userID); errors.Is(err, auth.ErrNotFound) {
        writeError(w, r, http.StatusNotFound, "user not found")
        return
    } else if c.handleAPIError(ctx, w, r, err) {
        return
    }

    // Top-level total.
    var bytesUsed int64
    if got, err := c.Catalog.KVStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey(userID)); err == nil {
        bytesUsed, _ = strconv.ParseInt(string(got.Value), 10, 64)
    } else if !errors.Is(err, kv.ErrNotFound) {
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }

    // Per-repo list.
    var repos []apigen.UserStorageRepo
    iter, err := c.Catalog.KVStore.Scan(ctx, stats.StoragePartition, kv.ScanOptions{KeyStart: stats.StorageRepoPrefix(userID)})
    if err != nil {
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }
    prefix := stats.StorageRepoPrefix(userID)
    for iter.Next() {
        ent := iter.Entry()
        if !bytes.HasPrefix(ent.Key, prefix) {
            break
        }
        _, repoName, perr := stats.ParseStorageRepoKey(ent.Key)
        if perr != nil {
            continue
        }
        n, _ := strconv.ParseInt(string(ent.Value), 10, 64)
        repos = append(repos, apigen.UserStorageRepo{Name: repoName, BytesUsed: n})
    }
    iter.Close()
    if err := iter.Err(); err != nil {
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }

    // Quota / remaining.
    var quotaBytes *int64
    var remaining *int64
    if c.QuotaChecker != nil {
        if v, ok, err := c.QuotaChecker.GetQuota(ctx, userID); err == nil && ok {
            quotaBytes = &v
            rem := v - bytesUsed
            if rem < 0 {
                rem = 0
            }
            remaining = &rem
        }
    }

    // last_reconciled_at.
    var lastReconciledAt *time.Time
    if got, err := c.Catalog.KVStore.Get(ctx, stats.StoragePartition, stats.StorageMetaLastReconciledAtKey(userID)); err == nil {
        if t, perr := time.Parse(time.RFC3339, string(got.Value)); perr == nil {
            lastReconciledAt = &t
        }
    }

    resp := apigen.UserStorage{
        User:             userID,
        BytesUsed:        bytesUsed,
        QuotaBytes:       quotaBytes,
        BytesRemaining:   remaining,
        Repositories:     repos,
        LastReconciledAt: lastReconciledAt,
        IsEstimate:       lastReconciledAt == nil,
    }
    writeResponse(w, r, http.StatusOK, resp)
}
```

Required imports for this handler include `bytes`, `strconv`, `time`, `github.com/invergent-ai/surogate-hub/pkg/kv`, and `github.com/invergent-ai/surogate-hub/pkg/stats`.

- [ ] **Step 6: Run tests to verify they pass**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestGetUserStorage -v
```

Expected: PASS for all four `TestGetUserStorage_*` cases.

- [ ] **Step 7: Commit**

```bash
git -C /work/surogate-hub add api/swagger.yml pkg/api/apigen pkg/api/controller.go pkg/api/controller_test.go pkg/api/serve_test.go
git -C /work/surogate-hub commit -m "api: add GET /auth/users/{userId}/storage"
```

---

## Task 14: Add `PUT/DELETE /auth/users/{userId}/quota` endpoints

**Files:**
- Modify: `/work/surogate-hub/api/swagger.yml`
- Regenerate: `/work/surogate-hub/pkg/api/apigen/sghub.gen.go`
- Modify: `/work/surogate-hub/pkg/api/controller.go`
- Modify: `/work/surogate-hub/pkg/api/controller_test.go`

- [ ] **Step 1: Add operations to swagger**

```yaml
  /auth/users/{userId}/quota:
    parameters:
      - in: path
        name: userId
        required: true
        schema:
          type: string
    put:
      tags:
        - auth
      operationId: setUserQuota
      summary: set a user's storage quota
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/UserQuota"
      responses:
        204:
          description: quota set
        400:
          $ref: "#/components/responses/BadRequest"
        401:
          $ref: "#/components/responses/Unauthorized"
        403:
          $ref: "#/components/responses/Forbidden"
        404:
          $ref: "#/components/responses/NotFound"
        default:
          $ref: "#/components/responses/ServerError"
    delete:
      tags:
        - auth
      operationId: deleteUserQuota
      summary: clear a user's storage quota
      responses:
        204:
          description: quota cleared
        401:
          $ref: "#/components/responses/Unauthorized"
        403:
          $ref: "#/components/responses/Forbidden"
        404:
          $ref: "#/components/responses/NotFound"
        default:
          $ref: "#/components/responses/ServerError"
```

Under `components.schemas`:

```yaml
    UserQuota:
      type: object
      required:
        - quota_bytes
      properties:
        quota_bytes:
          type: integer
          format: int64
          minimum: 0
```

- [ ] **Step 2: Regenerate codegen**

```bash
cd /work/surogate-hub && go generate ./pkg/api/apigen/...
```

- [ ] **Step 3: Write the failing test**

```go
func TestSetUserQuota_AdminCanSet(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, deps, "alice")

	resp, err := clt.SetUserQuotaWithResponse(ctx, "alice", apigen.UserQuota{QuotaBytes: 12345})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"))
	require.NoError(t, err)
	require.Equal(t, "12345", string(got.Value))
}

func TestSetUserQuota_NegativeRejected(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, deps, "alice")
	resp, err := clt.SetUserQuotaWithResponse(ctx, "alice", apigen.UserQuota{QuotaBytes: -1})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
}

func TestSetUserQuota_NonAdminForbidden(t *testing.T) {
	ctx := context.Background()
	_, deps := setupClientWithAdmin(t, withStorageAccountant())
	aliceClt := clientAs(t, deps, "alice")
	resp, err := aliceClt.SetUserQuotaWithResponse(ctx, "alice", apigen.UserQuota{QuotaBytes: 1000})
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode())
}

func TestDeleteUserQuota_Removes(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_ = clientAs(t, deps, "alice")
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("1000")))

	resp, err := clt.DeleteUserQuotaWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	if _, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice")); err != kv.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 4: Implement handlers**

```go
func (c *Controller) SetUserQuota(w http.ResponseWriter, r *http.Request, userID string) {
    if !c.authorize(w, r, permissions.Node{
        Permission: permissions.Permission{
            Action:   permissions.WriteUserAction,
            Resource: permissions.UserArn(userID),
        },
    }) {
        return
    }
    ctx := r.Context()
    var body apigen.UserQuota
    if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
        writeError(w, r, http.StatusBadRequest, "invalid body")
        return
    }
    c.LogAction(ctx, "set_user_quota", r, "", "", "")
    if _, err := c.Auth.GetUser(ctx, userID); errors.Is(err, auth.ErrNotFound) {
        writeError(w, r, http.StatusNotFound, "user not found")
        return
    } else if c.handleAPIError(ctx, w, r, err) {
        return
    }
    if c.QuotaChecker == nil {
        writeError(w, r, http.StatusServiceUnavailable, "storage usage tracking is not enabled")
        return
    }
    if err := c.QuotaChecker.SetQuota(ctx, userID, body.QuotaBytes); err != nil {
        if errors.Is(err, stats.ErrInvalidQuota) {
            writeError(w, r, http.StatusBadRequest, err.Error())
            return
        }
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }
    writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) DeleteUserQuota(w http.ResponseWriter, r *http.Request, userID string) {
    if !c.authorize(w, r, permissions.Node{
        Permission: permissions.Permission{
            Action:   permissions.WriteUserAction,
            Resource: permissions.UserArn(userID),
        },
    }) {
        return
    }
    ctx := r.Context()
    if _, err := c.Auth.GetUser(ctx, userID); errors.Is(err, auth.ErrNotFound) {
        writeError(w, r, http.StatusNotFound, "user not found")
        return
    } else if c.handleAPIError(ctx, w, r, err) {
        return
    }
    if c.QuotaChecker == nil {
        writeError(w, r, http.StatusServiceUnavailable, "storage usage tracking is not enabled")
        return
    }
    if err := c.QuotaChecker.ClearQuota(ctx, userID); err != nil {
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }
    writeResponse(w, r, http.StatusNoContent, nil)
}
```

- [ ] **Step 5: Run tests, expect PASS**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run 'TestSetUserQuota|TestDeleteUserQuota' -v
```

- [ ] **Step 6: Verify admin policy coverage**

No extra default-policy edit is needed for admins: `pkg/auth/setup/setup.go` creates `AuthFullAccess` with action `auth:*`, which covers the new `auth:WriteUser` action after Task 4 regenerates the action list. Keep quota write tests against the admin client to verify this remains true.

- [ ] **Step 7: Commit**

```bash
git -C /work/surogate-hub add api/swagger.yml pkg/api/apigen pkg/api/controller.go pkg/api/controller_test.go pkg/auth/setup
git -C /work/surogate-hub commit -m "api: add PUT/DELETE /auth/users/{userId}/quota"
```

---

## Task 15: Enforce quota at upload paths

**Files:**
- Modify: `/work/surogate-hub/pkg/api/controller.go`
- Modify: `/work/surogate-hub/pkg/api/controller_test.go`
- Modify: `/work/surogate-hub/pkg/gateway/operations/` paths from Task 11

- [ ] **Step 1: Write the failing test**

```go
func TestUploadObject_RejectedOverQuota(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)
	// quota=10, already used 8.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("10")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("8")))

	resp, err := clt.UploadObjectWithBody(ctx, "alice", "training", "main", &apigen.UploadObjectParams{Path: "a.txt"}, "application/octet-stream", bytes.NewReader([]byte("hello"))) // 5 bytes ⇒ 13 > 10
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode())

	// Counter unchanged because upload rejected before WriteBlob.
	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey("alice"))
	require.NoError(t, err)
	require.Equal(t, "8", string(got.Value))
}

func TestUploadObject_AllowedUnderQuota(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageQuotaKey("alice"), []byte("100")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("0")))

	resp, err := clt.UploadObjectWithBody(ctx, "alice", "training", "main", &apigen.UploadObjectParams{Path: "a.txt"}, "application/octet-stream", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())
}
```

- [ ] **Step 2: Run, expect FAIL on the over-quota test**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run 'TestUploadObject_Rejected|TestUploadObject_Allowed' -v
```

- [ ] **Step 3: Add the quota check in `UploadObject`**

After `c.Catalog.GetRepository` returns and `branchExists` checks pass, but **before** `upload.WriteBlob`, add:

```go
if c.QuotaChecker != nil {
    dec, err := c.QuotaChecker.Allow(ctx, owner, r.ContentLength)
    if err != nil {
        writeError(w, r, http.StatusInternalServerError, err.Error())
        return
    }
    if !dec.Allowed {
        writeQuotaRejection(w, r, dec)
        return
    }
}
```

Define `writeQuotaRejection` once at the top of `controller.go`:

```go
func writeQuotaRejection(w http.ResponseWriter, r *http.Request, dec stats.QuotaDecision) {
    status := http.StatusRequestEntityTooLarge
    if dec.Reason == stats.QuotaReasonUnknownSize {
        status = http.StatusLengthRequired
    }
    payload := map[string]any{
        "error":      "storage quota exceeded",
        "quota_bytes": dec.QuotaBytes,
        "bytes_used":  dec.BytesUsed,
    }
    writeResponse(w, r, status, payload)
}
```

- [ ] **Step 4: Repeat for `CopyObject`, `CompletePresignMultipartUpload`, and the gateway operations**

Apply the same check before each path writes or materializes bytes. For copy, the byte count is the source entry's size. For `CompletePresignMultipartUpload`, use the final `mpuResp.ContentLength` immediately after completion; if over quota at that point, return 413 and rely on the reconciler/GC for any already-written bytes. Do not quota-check `StageObject` as a generic write path because it only creates metadata for an existing physical address.

- [ ] **Step 5: Run all related tests, expect PASS**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run 'TestUploadObject|TestCopyObject|TestCompletePresignMultipartUpload' -v
```

- [ ] **Step 6: Commit**

```bash
git -C /work/surogate-hub add pkg/api/controller.go pkg/api/controller_test.go pkg/gateway
git -C /work/surogate-hub commit -m "api+gateway: reject uploads exceeding user storage quota"
```

---

## Task 16: End-to-end and drift tests

**Files:**
- Create: `/work/surogate-hub/pkg/api/storage_usage_e2e_test.go`

- [ ] **Step 1: Write the integration test**

```go
package api_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestStorageUsage_EndToEnd(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)

	// Upload three objects.
	for _, body := range [][]byte{[]byte("aa"), []byte("bbbb"), []byte("cccccc")} {
		resp, err := clt.UploadObjectWithBody(ctx, "alice", "training", "main", &apigen.UploadObjectParams{Path: "obj-" + strconv.Itoa(len(body))}, "application/octet-stream", bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}
	require.NoError(t, deps.storageAccountant.Flush(ctx))

	resp, err := clt.GetUserStorageWithResponse(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, int64(12), resp.JSON200.BytesUsed)
	require.Len(t, resp.JSON200.Repositories, 1)
	require.Equal(t, int64(12), resp.JSON200.Repositories[0].BytesUsed)
}

func TestStorageUsage_ReconcilerCorrectsDrift(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, withStorageAccountant())
	_, err := deps.catalog.CreateRepository(ctx, "alice/training", "", "mem://alice-training", "main", false)
	require.NoError(t, err)

	// Seed deliberately-wrong counter.
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"), []byte("999999")))
	require.NoError(t, deps.kvStore.Set(ctx, stats.StoragePartition, stats.StorageUserKey("alice"), []byte("999999")))

	require.NoError(t, deps.storageReconciler.RunOnce(ctx))

	got, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageRepoKey("alice", "training"))
	require.NoError(t, err)
	require.Equal(t, "0", string(got.Value)) // empty namespace ⇒ 0
	gotUser, err := deps.kvStore.Get(ctx, stats.StoragePartition, stats.StorageUserKey("alice"))
	require.NoError(t, err)
	require.Equal(t, "0", string(gotUser.Value))
}
```

Extend `withStorageAccountant()` to also instantiate a real reconciler against an in-memory block adapter, exposed on `*dependencies` as `storageReconciler *stats.StorageReconciler`.

- [ ] **Step 2: Run, expect PASS**

```bash
cd /work/surogate-hub && go test ./pkg/api/ -run TestStorageUsage -v
```

- [ ] **Step 3: Commit**

```bash
git -C /work/surogate-hub add pkg/api/storage_usage_e2e_test.go pkg/api/serve_test.go
git -C /work/surogate-hub commit -m "test: end-to-end storage usage and reconciler drift recovery"
```

---

## Task 16.5: Document omitted object-delete hooks

The spec acknowledges that `block.Adapter.Remove` does not return the deleted byte count, so object-level delete hooks are skipped in this implementation. We rely on the reconciler to correct the resulting drift within one reconciler interval (default 1 h).

- [ ] **Step 1: Add a short comment to `pkg/stats/storage_accountant.go`**

At the top of the file, just below the package doc, add:

```go
// Object-level deletes are deliberately NOT hooked through the accountant: block.Adapter.Remove
// does not report the size of the deleted object, and rewiring the call sites to look it up first
// would couple every delete path to a metadata read. The StorageReconciler pass corrects the
// resulting drift within one interval (default 1h). Repository-level deletes ARE hooked, because
// the per-repo counter is read once and the user total is updated atomically.
```

- [ ] **Step 2: Commit**

```bash
git -C /work/surogate-hub add pkg/stats/storage_accountant.go
git -C /work/surogate-hub commit -m "stats: document why object-delete hooks are omitted"
```

---

## Task 17: Documentation update

**Files:**
- Modify: `/work/surogate-hub/docs/` index or main reference (find with `ls docs/`).
- Optional: Add a short README section in `pkg/stats/README.md` describing the new components.

- [ ] **Step 1: Add user-facing docs**

In the docs directory tree, add a page that explains:
- The `GET /auth/users/{userId}/storage` response shape.
- That self-serve reads always work; reading others requires `auth:ReadUser`.
- That admins set/clear quotas via `PUT/DELETE /auth/users/{userId}/quota`.
- The semantics of `is_estimate` and `last_reconciled_at`.
- That uploads return **413 Payload Too Large** when over quota.

- [ ] **Step 2: Add a sentence to the existing config docs**

Wherever `usage_report` config is documented, add the nested `storage_usage` block with `enabled`, `storage_accountant.flush_interval`, `storage_reconciler.interval`, and `storage_reconciler.concurrency`.

- [ ] **Step 3: Commit**

```bash
git -C /work/surogate-hub add docs/ pkg/stats/README.md
git -C /work/surogate-hub commit -m "docs: per-user storage reporting and quotas"
```

---

## Acceptance Checklist

- [ ] `go test ./...` is green from the repo root.
- [ ] `go vet ./...` is clean.
- [ ] `GET /auth/users/{userId}/storage` returns expected shape for self, admin, and 403 for other non-admin callers.
- [ ] `PUT /auth/users/{userId}/quota` with negative body returns 400; valid body returns 204; non-admin returns 403.
- [ ] `DELETE /auth/users/{userId}/quota` returns 204 whether or not a quota was set.
- [ ] An upload that would push `bytes_used` over the user's `quota_bytes` returns 413 with `{error, quota_bytes, bytes_used}`.
- [ ] After uploads via the REST API and the S3 gateway, the per-user counter is the sum of stored bytes (within reconciler interval).
- [ ] Deleting a repo decrements the user total by exactly the repo's recorded bytes.
- [ ] Forcing a reconciler pass corrects a deliberately-stale counter.
- [ ] Server starts and serves traffic when `storage_usage.enabled = false` (legacy default).
