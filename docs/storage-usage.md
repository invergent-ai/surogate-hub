# Per-User Storage Reporting and Quotas

Surogate Hub tracks how many bytes each user's repositories occupy in block storage and exposes
that figure (with an optional admin-set quota) through three HTTP endpoints under `/auth/users`.

This page documents the surface for operators and API consumers. The full design lives in
[`docs/superpowers/specs/2026-05-16-user-storage-reporting-design.md`](superpowers/specs/2026-05-16-user-storage-reporting-design.md).

## What gets counted

For each user `U`, `bytes_used(U)` is the sum, over every repository whose ID is `U/<name>`, of
the bytes allocated to that repository's block-storage namespace.

- Counted on every successful object upload via the REST API
  (`PUT /repositories/{user}/{repo}/branches/{branch}/objects`), the S3 gateway PUT path, and
  presigned multipart completion. The delta is the actual byte count written to the block store.
- Counted on `CreateRepository` (counter initialized to 0) and decremented on `DeleteRepository`.
- Within a single repository, the same physical address is counted once — `CopyEntry` does not
  double-count.
- Across repositories, an object uploaded twice is counted twice (no cross-repo dedup).
- Object-level deletes are **not** hooked through the accountant (the block adapter's `Remove`
  does not report the deleted byte count). The periodic reconciler corrects the resulting drift
  within one interval, default `1h`.
- Repository IDs that are not in `{owner}/{name}` form are excluded from per-user accounting.

## API

### `GET /auth/users/{userId}/storage`

Returns the per-user total, per-repository breakdown, and quota (if set).

```json
{
  "user": "alice",
  "bytes_used": 1234567890,
  "quota_bytes": 10737418240,
  "bytes_remaining": 9502850350,
  "repositories": [
    { "name": "training-data", "bytes_used": 900000000 },
    { "name": "evals",         "bytes_used": 334567890 }
  ],
  "last_reconciled_at": "2026-05-16T10:23:00Z",
  "is_estimate": false
}
```

- `quota_bytes` and `bytes_remaining` are omitted (`null`) when no quota is set.
- `bytes_remaining` is clamped at zero when usage exceeds the quota (in-flight overage during the
  soft check, drift the reconciler hasn't corrected yet).
- `last_reconciled_at` is `null` until the reconciler has completed at least one pass for the
  user. `is_estimate` is `true` while it is `null`.

Authorization: callers may always read their own user (`{userId}` equals the authenticated user's
username); reading another user's record requires `auth:ReadUser`.

### `PUT /auth/users/{userId}/quota`

Admin-only (`auth:WriteUser`). Sets or replaces the storage quota for the user.

```json
{ "quota_bytes": 10737418240 }
```

- `204 No Content` on success.
- `400 Bad Request` when `quota_bytes < 0`.
- `404 Not Found` when the user does not exist.

### `DELETE /auth/users/{userId}/quota`

Admin-only. Removes any existing quota for the user, reverting them to unlimited. Idempotent: a
user with no quota still returns `204`.

## Quota enforcement

When a quota is set, write paths reject requests that would exceed it before the bytes are streamed
to the block store:

- `PUT /repositories/{user}/{repo}/branches/{branch}/objects` returns `413 Request Entity Too
  Large` with `{ "error": "storage quota exceeded", "quota_bytes": Q, "bytes_used": U }`.
- The S3 gateway PUT path returns an `EntityTooLarge` (`HTTP 400` / S3 error code) for the same
  condition.
- If a quota is set and `Content-Length` is unknown (`< 0`), the REST API returns `411 Length
  Required`. The S3 gateway returns `MissingContentLength`.

The check is **soft**: it reads the last-flushed counter, so concurrent uploads in flight from the
same user can momentarily push the total slightly above the quota. The next reconciler pass
corrects the counter, and subsequent uploads see the new value. Operators sizing quotas should
leave headroom for in-flight overage.

S3 multipart upload completion runs the same check at the completion step, when the assembled
size is known.

## Configuration

`storage_usage` is its own top-level block, separate from the existing `usage_report`
(installation-wide telemetry) and `stats` (collector) blocks:

```yaml
storage_usage:
  enabled: true                       # default false
  storage_accountant:
    flush_interval: 5s                # default 5s — how often in-memory deltas are flushed to KV
  storage_reconciler:
    interval: 1h                      # default 1h — how often the per-repo walker runs
    concurrency: 4                    # default 4  — repos reconciled in parallel
```

When `storage_usage.enabled = false`, the `StorageAccountant` and `QuotaChecker` are nil, every
upload path becomes a no-op for the counter, and the new endpoints return `503 Service
Unavailable` if called.

## Reconciler

The reconciler runs as a goroutine started from `cmd/sghub/cmd/run.go` alongside the existing
`UsageReporter`. Every interval it walks each repository's storage namespace via
`block.Adapter.GetWalker`, sums the returned `ObjectStoreEntry.Size`, and overwrites the per-repo
counter. After each owner's repositories are processed, the per-user denormalized total is
rewritten from the sum of repo counters and `storage/meta/{owner}/last_reconciled_at` is updated.

If a pass takes longer than the configured interval, the next pass starts as soon as the previous
one finishes. A single process never overlaps its own passes.

## KV layout

All counters and quotas live under the new `storage` partition:

| Key                                          | Value           | Meaning                                                |
| -------------------------------------------- | --------------- | ------------------------------------------------------ |
| `storage/repo/{owner}/{repo}`                | int64 (ASCII)   | Bytes currently allocated in that repo's namespace     |
| `storage/user/{owner}`                       | int64 (ASCII)   | Sum of `{owner}`'s repo counters (denormalized)        |
| `storage/quota/{owner}`                      | int64 (ASCII)   | Maximum bytes allowed for `{owner}` — absent ⇒ unlimited |
| `storage/meta/{owner}/last_reconciled_at`    | RFC3339         | Timestamp of last completed reconciler pass            |
