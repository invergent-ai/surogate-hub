# Per-User Storage Reporting

**Date:** 2026-05-16
**Status:** Draft

## Goal

Surface, via the public API, how many bytes of object storage each Surogate Hub user is currently consuming, broken down per repository. The top-level user total and quota check must be O(1); the per-repository breakdown may scan that user's repository counters. The figure must be accurate to within one reconciler interval.

## Non-goals

- Billing-grade accuracy of cross-repo (Xet/CAS) deduplication. Per-repo retention is exact; cross-repo sharing is not subtracted.
- Historical time series. Only the current snapshot is exposed.
- Per-branch breakdown. Branches share objects, and attribution would require walking.
- Installation-wide default quotas. Quotas are explicitly opt-in per user; unset means unlimited.

## Definition of "storage used"

For a user `U`:

```
bytes_used(U) = Σ over repos R owned by U:
                  bytes currently allocated in R's storage namespace
```

- **Owner**: the first path segment of the repository ID. The internal repo ID is `{owner}/{name}` (see `namespacedRepository` in `pkg/api/controller.go`).
- **"Bytes currently allocated in R's storage namespace"**: the sum, over every physical object present in the block-storage namespace backing repo R, of that object's stored byte length. This is what the block-storage backend reports as allocated, including Surogate Hub metadata under the namespace and pre-commit staging uploads that have not yet been garbage-collected.
- **Same physical address counted once per repo**: graveler reuses the same physical address when an entry is copied within a repo, so it naturally appears once in the per-repo allocated bytes.
- **Per-repo, not cross-repo, dedup**: if the same content is uploaded into two of the user's repos, it is counted twice (because it is physically stored twice — one copy in each namespace). Xet/CAS cross-repo deduplication is explicitly out of scope.
- **Staging is included**: an upload that has been written to the block store but not yet committed is part of `bytes_used`, because those bytes really are allocated. When staging is reset or expires, the orphan is removed by GC and the counter decrements.

## Architecture

Three runtime components, implemented under `pkg/stats` next to the existing `UsageReporter`:

### 1. `StorageAccountant` — in-process delta cache

- Maintains a small map `(owner, repo) → int64 delta` in memory.
- Exposes `Add(owner, repo, delta int64)` callable from block-store write sites that know the repository.
- A periodic flusher (default every 5 s, mirroring `UsageReporter.Start`) applies pending deltas to the KV store using `SetIf` + predicate retry (same pattern as `UsageReporter.updateRecord`).
- On flush, also updates the per-user denormalized total.
- If a flush fails after deltas are removed from memory, the flusher must requeue those deltas before returning the error.

### 2. KV partition `storage`

New partition alongside the existing `usage` partition:

| Key                              | Value         | Meaning                                      |
| -------------------------------- | ------------- | -------------------------------------------- |
| `storage/repo/{owner}/{repo}`    | int64 (ascii) | Bytes currently allocated in that repo's storage namespace |
| `storage/user/{owner}`           | int64 (ascii) | Sum of all `{owner}`'s repo counters         |
| `storage/meta/{owner}/last_reconciled_at` | RFC3339       | Timestamp of last completed reconciler pass for `{owner}` |
| `storage/quota/{owner}`          | int64 (ascii) | Maximum bytes allowed for `{owner}` (absent ⇒ unlimited) |

The per-user total is denormalized so the API hot path is a single point lookup. Drift between user-total and sum-of-repos is corrected on every reconciler pass.

### 3. Reconciler — periodic background walker

Started from `cmd/sghub` server lifecycle alongside `UsageReporter`. Default interval **1 hour**, configurable. For each repository:

1. Acquire a per-repo in-process reconciler lock. This only prevents duplicate passes within one server process; concurrent replicas are tolerated because reconciler writes are idempotent `SetIf` updates.
2. Ask the block-storage adapter for the total bytes currently allocated in the repo's storage namespace. The default implementation uses `block.Adapter.GetWalker` with the repository storage namespace and sums `block.ObjectStoreEntry.Size`; adapters that support cheap namespace sizing (S3 inventory, GCS bucket metrics) may override through a small optional interface.
3. Flush pending in-memory deltas for this repo via `StorageAccountant`, then `SetIf` the freshly observed value with predicate retry.
4. After all of an owner's repos have been reconciled, write `storage/user/{owner}` = sum of repo counters, and set `storage/meta/{owner}/last_reconciled_at` to the pass start time.

If a pass exceeds the next interval (large installation), the next pass starts as soon as the previous finishes. A single process must not overlap its own passes.

## Hook sites

The accountant is called at the layer that already knows both the repo (and therefore the owner) and the bytes physically written to or removed from the block store. In this codebase those sites are the API and gateway upload paths, repository copy/import paths, GC, and internal metadata writers that call `block.Adapter`.

| Event                                                                 | Action                                              |
| --------------------------------------------------------------------- | --------------------------------------------------- |
| `block.Adapter.Put`, `UploadPart`, or `CompleteMultiPartUpload` succeeds in repo R's namespace | `Add(owner, repo, +bytes_written)` |
| `block.Adapter.Copy` materializes a new physical object in repo R's namespace | `Add(owner, repo, +bytes_copied)` when the destination backend reports or can determine it |
| `block.Adapter.Remove` succeeds and the caller knows the removed object's stored size | `Add(owner, repo, -bytes_removed)` |
| GC sweep removes an orphaned/unreachable object                       | Same as remove above; if size is unknown, skip the delta and rely on the reconciler |
| Repository deleted                                                    | Read current `storage/repo/{owner}/{repo}`, decrement `storage/user/{owner}` by that value, then delete the repo key |
| Repository created                                                    | Initialize `storage/repo/{owner}/{repo}` = 0        |

The accountant takes the byte count the block-store adapter or wrapper reports as actually written. The current `block.Adapter.Put` return type does not include a byte count, so wrappers must use `upload.Blob.Size`, multipart response `ContentLength`, request part sizes, copied object metadata, or walker output where available. The current `block.Adapter.Remove` does not return deleted size; delete decrements are therefore best-effort unless the caller already knows the object size.

Branch-level operations do not call the accountant merely because a branch changed. They do call it indirectly if the operation writes block-store objects such as `_hub` ranges, metaranges, manifests, or copied payload objects. Whether copied entries are deduped is the block-store adapter's concern: if the adapter or wrapper determines that no new physical bytes were written, the accountant adds zero.

Exact insertion points (function names) are an implementation-plan concern; the spec only requires that every block-store write inside a repo namespace passes through the accountant with the actual byte count, and every delete does so when the removed byte count is available.

The accountant is best-effort. A missed call leaks drift that the reconciler fixes within one interval. The reconciler is the source of truth; hooks just make the hot read fresh.

## API

Three new OpenAPI operations in `api/swagger.yml`.

### `GET /auth/users/{userId}/storage`

```
Path:
  userId — Surogate Hub user id

200 OK:
  {
    "user": "alice",
    "bytes_used": 1234567890,
    "quota_bytes": 10737418240,     // omitted when unlimited
    "bytes_remaining": 9502850350,  // omitted when unlimited
    "repositories": [
      { "name": "training-data", "bytes_used": 900000000 },
      { "name": "evals",         "bytes_used": 334567890 }
    ],
    "last_reconciled_at": "2026-05-16T10:23:00Z",
    "is_estimate": false
  }

404 Not Found: user does not exist
403 Forbidden: caller is not {userId} and lacks auth:ReadUser
401 Unauthorized
500 Internal Server Error
```

- `bytes_used` at the top level is read from `storage/user/{userId}`.
- `repositories` is the result of a KV `Scan` on prefix `storage/repo/{userId}/`; this part is O(number of repos owned by the user).
- `last_reconciled_at` comes from `storage/meta/{userId}/last_reconciled_at`.
- `is_estimate` is `true` if `last_reconciled_at` is missing (reconciler has never run for this user).
- `quota_bytes` / `bytes_remaining` are present only when `storage/quota/{userId}` exists. `bytes_remaining = max(0, quota_bytes - bytes_used)`.

### `PUT /auth/users/{userId}/quota`

```
Body:
  { "quota_bytes": 10737418240 }

204 No Content: set or replaced
400 Bad Request: quota_bytes < 0
404 Not Found: user does not exist
403 Forbidden: caller lacks auth:WriteUser
```

Writes `storage/quota/{userId}`.

### `DELETE /auth/users/{userId}/quota`

```
204 No Content: removed (or already absent)
404 Not Found: user does not exist
403 Forbidden: caller lacks auth:WriteUser
```

Removes `storage/quota/{userId}`, reverting the user to unlimited.

### Authorization

`GET /auth/users/{userId}/storage`:

- If the authenticated principal's username equals `{userId}` → allow (self-serve).
- Otherwise → require `auth:ReadUser` (the permission already used for `GET /auth/users/{userId}`).

`PUT/DELETE /auth/users/{userId}/quota`:

- Require `auth:WriteUser` (the permission already used for mutating user records). Users cannot set their own quota.

Wired the same way as existing reads in `pkg/api/controller.go`:

```go
if !c.authorize(w, r, permissions.Node{
    Permission: permissions.Permission{
        Action:   permissions.ReadUserAction,
        Resource: permissions.UserArn(userID),
    },
}) { return }
```

with a short-circuit before the call when the authenticated user's username equals `userID`.

## Quota enforcement

When `storage/quota/{owner}` is set, write requests into any repo owned by `owner` are checked at the start of the upload:

```
allow_write(owner, content_length):
  if content_length < 0: return REJECT_UNKNOWN_SIZE
  quota = kv.get("storage/quota/" + owner)        // single point lookup
  if quota is absent: return ALLOW                // unlimited
  used  = kv.get("storage/user/" + owner)         // single point lookup
  if used + content_length > quota: return REJECT
  return ALLOW
```

- Reject response: **413 Payload Too Large** with body `{"error": "storage quota exceeded", "quota_bytes": Q, "bytes_used": U}`.
- If quota is set and the request size is unknown, reject before streaming with **411 Length Required**. A streaming quota-limited reader is only acceptable if the implementation can guarantee aborted writes do not leave retained bytes behind. Do not accept an unknown-size write blindly when quota is set.
- For known-size single-object writes, the check fires before any bytes are streamed to block storage, so a rejected upload consumes no quota and produces no garbage.
- Two KV point lookups per upload. Acceptable on the hot path; cacheable per request if benchmarks show it matters (out of scope for this spec).

### Soft-check semantics

- The check reads the **last-flushed** counter, not the in-memory accountant delta. Concurrent uploads in flight from the same user can therefore push the user up to `quota + sum(in_flight_content_length)` before the next flush propagates the new value.
- This is acceptable per the chosen "soft check at upload start" mode. Operators sizing quotas should leave some headroom.
- The reconciler corrects any drift between counter and reality within one interval (default 1 h), at which point new uploads see the corrected number.

### Write surfaces that must call the check

The check must be invoked from every code path that admits new bytes into a repo's storage namespace. From the existing controller, those include at least:

- Object upload via the REST API (`PUT /repositories/{owner}/{repo}/branches/{branch}/objects`)
- S3-gateway PUT (`pkg/gateway/operations`)
- S3 multipart part upload and upload-copy-part (`UploadPart`, `UploadCopyPart`, `UploadCopyPartRange`) before each part is written/copied
- Multipart completion only for final-size validation/accounting using `CompleteMultiPartUploadResponse.ContentLength`; S3 multipart initiation does not know the final size
- Copy paths (`Catalog.CopyEntry`, S3 copy object/copy part) that materialize objects into the destination repo
- Internal metadata writes such as repository create dummy objects, dump/restore manifests, GC prepared files, ranges, and metaranges when they write to the repo storage namespace
- Import paths that only register external full addresses do not consume bytes in the repo namespace unless they copy/materialize data there

The accountant interface stays the same; what's new is `Allow(owner, contentLength) (bool, QuotaInfo)` invoked next to it.

## Configuration

Use a new top-level configuration block. Do not place this under `stats`, which is already used for the telemetry collector, or under `usage_report`, which reports monthly installation usage:

```yaml
storage_usage:
  enabled: true
  storage_accountant:
    flush_interval: 5s    # default
  storage_reconciler:
    interval: 1h          # default
    concurrency: 4        # max repos reconciled in parallel
```

A zero `interval` disables the reconciler (test/dev only).

## Failure modes and recovery

| Scenario                                       | Outcome                                                                                 |
| ---------------------------------------------- | --------------------------------------------------------------------------------------- |
| Server crashes between accountant flushes      | In-memory deltas lost; reconciler corrects the value within one interval                |
| GC removes objects without firing a hook       | Counter overstates usage; reconciler corrects                                           |
| Race: two writers update the same repo counter concurrently | KV `SetIf` predicate retry, same as existing `UsageReporter.updateRecord` |
| Repo deletion races with active writes         | Delete removes the repo key and decrements the user total from the stored repo value; late writes may create drift until the next reconciler pass |
| Repo restored or recreated after delete        | Repository creation initializes the counter; the next reconciler pass sets it to the actual namespace size |
| Reconciler interrupted mid-pass                | No write-back happens for unreconciled repos; values stay at last-flushed accountant state |

The invariant `storage/user/{owner} == Σ storage/repo/{owner}/*` is only guaranteed immediately after a reconciler pass. Between passes, point-in-time reads may show small drift; that's why `last_reconciled_at` is exposed.

## Testing

Following the patterns already established by `pkg/stats/usage_counter_test.go` and `pkg/api/controller_test.go`:

- **Unit**: accountant `Add` math (positive, negative, multi-key), flush behavior with predicate retry, response shape, authz cases (self / admin / forbidden), quota check `Allow` logic (absent / under / at / over).
- **Integration**: commit N objects to a repo, fetch `/auth/users/{userId}/storage`, expect `Σ size_bytes` plus any counted repo metadata bytes. Delete some, run GC, fetch again, expect the reduced value after either the delete hook or a forced reconciler pass.
- **Drift recovery**: write an object directly via graveler (bypassing the accountant), fetch — expect stale. Force a reconciler pass, fetch — expect corrected.
- **Repo deletion**: delete a repo with non-zero counter, fetch — expect the user total reduced by exactly the repo's bytes, and the repo absent from the list.
- **Multi-repo**: create two repos for the same user, verify per-repo entries and that the top-level total equals the sum.
- **Quota set / read / clear**: `PUT` a quota, fetch storage and see `quota_bytes`. `DELETE` the quota, fetch — `quota_bytes` absent. `PUT` with negative value — 400.
- **Quota rejection**: set quota at `K`, fill repo to `K-1`, attempt upload of 100 bytes — expect 413 with quota body. Verify no bytes landed in storage. Clear quota, retry — expect success.
- **Quota soft-check overage window**: with quota near limit, race two concurrent uploads — verify both may pass the start check, the stored total may temporarily exceed quota by in-flight bytes, and later uploads are rejected after flush/reconciliation reflects the actual stored bytes.

## Out of scope (explicit)

- Notifications / webhooks when crossing thresholds.
- Per-branch attribution.
- Cross-repo (CAS/Xet) dedup-aware accounting.
- Historical samples or graphs.
- Cross-installation reporting.
- Installation-wide default quotas applied to every user.
- Soft-quota or grace-period semantics — over quota is rejected immediately.

Each of these is a clean follow-up once the counter and per-user quota exist.
