# Per-Owner Storage Reporting and Quotas

Surogate Hub tracks how many bytes each **owner namespace**'s repositories occupy in block
storage and exposes that figure (with an optional admin-set quota) through three HTTP endpoints
under `/storage/owners`.

> **What's an "owner"?** Repository ids in Surogate Hub have the form `{owner}/{name}` — the
> owner is the first path segment. It is **not** necessarily a hub auth user; it can be any
> synthetic project / workspace id (e.g. surogate-ops uses `p-39264d5a`-style project ids as the
> owner of every repo in a project). Storage usage and quotas are scoped to that owner namespace.

This page documents the surface for operators and API consumers. The full design lives in
[`docs/superpowers/specs/2026-05-16-user-storage-reporting-design.md`](superpowers/specs/2026-05-16-user-storage-reporting-design.md).

## What gets counted

For each owner `O`, `bytes_used(O)` is the sum, over every repository whose id is `O/<name>`, of
the bytes allocated to that repository's block-storage namespace.

- Counted on every successful object upload via the REST API
  (`PUT /repositories/{owner}/{repo}/branches/{branch}/objects`), the S3 gateway PUT path, and
  presigned multipart completion. The delta is the actual byte count written to the block store.
- Counted on `CreateRepository` (counter initialized to 0) and decremented on `DeleteRepository`.
- Within a single repository, the same physical address is counted once — `CopyEntry` does not
  double-count.
- Across repositories, an object uploaded twice is counted twice (no cross-repo dedup).
- Object-level deletes are **not** hooked through the accountant (the block adapter's `Remove`
  does not report the deleted byte count). The periodic reconciler corrects the resulting drift
  within one interval, default `1h`.
- Repository ids that are not in `{owner}/{name}` form are excluded from per-owner accounting.

## API

### `GET /storage/owners/{owner}`

Returns the per-owner total, per-repository breakdown, and quota (if set).

```json
{
  "owner": "p-39264d5a",
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
  owner. `is_estimate` is `true` while it is `null`.
- An owner namespace with no repositories returns `200 OK` with `bytes_used: 0` and
  `repositories: []`. The endpoint does **not** 404 for unknown owners — owners are not required
  to be registered hub auth users.

Authorization: callers may always read storage for an owner equal to their own authenticated
username (self-serve, when a human user owns repos under their own name); reading any other
owner's record requires `auth:ReadUser`. Authenticated callers without that permission receive
**401 Unauthorized** (Surogate Hub's `authorize` wrapper returns 401 for both unauthenticated and
authenticated-but-insufficient-permission cases — there is no 403 path).

### `PUT /storage/owners/{owner}/quota`

Admin-only (`auth:WriteUser`). Sets or replaces the storage quota for an owner namespace.

```json
{ "quota_bytes": 10737418240 }
```

- `204 No Content` on success.
- `400 Bad Request` when `quota_bytes < 0`.
- `503 Service Unavailable` when `storage_usage.enabled = false`.

> **Operator note:** `quota_bytes: 0` is accepted and hard-blocks every subsequent upload by the
> owner (since `used + content_length > 0` for any non-empty upload). Treat 0 as an intentional
> "lock the owner out" value, not as a sensible default. To remove a quota and revert to
> unlimited, use `DELETE /storage/owners/{owner}/quota` rather than `PUT { "quota_bytes": 0 }`.

### `DELETE /storage/owners/{owner}/quota`

Admin-only. Removes any existing quota for the owner, reverting it to unlimited. Idempotent: an
owner with no quota still returns `204`.

## Quota enforcement

When a quota is set, write paths reject requests that would exceed it before the bytes are streamed
to the block store:

- `PUT /repositories/{owner}/{repo}/branches/{branch}/objects` returns `413 Request Entity Too
  Large` with `{ "error": "storage quota exceeded", "quota_bytes": Q, "bytes_used": U }`.
- The S3 gateway PUT path returns an `EntityTooLarge` (`HTTP 400` / S3 error code) for the same
  condition.
- If a quota is set and `Content-Length` is unknown (`< 0`), the REST API returns `411 Length
  Required`. The S3 gateway returns `MissingContentLength`.

The check is **soft**: it reads the last-flushed counter, so concurrent uploads in flight from the
same owner can momentarily push the total slightly above the quota. The next reconciler pass
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
counter. After each owner's repositories are processed, the per-owner denormalized total is
rewritten from the sum of repo counters and `storage/meta/{owner}/last_reconciled_at` is updated.

If a pass takes longer than the configured interval, the next pass starts as soon as the previous
one finishes. A single process never overlaps its own passes.

## KV layout

All counters and quotas live under the `storage` partition:

| Key                                          | Value           | Meaning                                                |
| -------------------------------------------- | --------------- | ------------------------------------------------------ |
| `storage/repo/{owner}/{repo}`                | int64 (ASCII)   | Bytes currently allocated in that repo's namespace     |
| `storage/user/{owner}`                       | int64 (ASCII)   | Sum of `{owner}`'s repo counters (denormalized; key name kept for backwards-compat with on-disk data, semantically per-owner) |
| `storage/quota/{owner}`                      | int64 (ASCII)   | Maximum bytes allowed for `{owner}` — absent ⇒ unlimited |
| `storage/meta/{owner}/last_reconciled_at`    | RFC3339         | Timestamp of last completed reconciler pass            |
