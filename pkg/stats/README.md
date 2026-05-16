# pkg/stats

Statistics and storage-accounting primitives shared by the API server and the S3 gateway.

## Components

- **`Collector`** (`collector.go`, `sender.go`) — installation-wide telemetry buffer/sender.
- **`UsageCounter`** (`usage_counter.go`) — atomic counter for API/gateway call totals.
- **`UsageReporter`** (`usage_counter.go`) — monthly KV-persisted usage records for
  installation-wide reporting.
- **`StorageAccountant`** (`storage_accountant.go`) — in-memory delta cache + periodic KV flusher
  for per-repo allocated bytes. The accountant is `nil` and a no-op when
  `storage_usage.enabled=false`.
- **`StorageReconciler`** (`storage_reconciler.go`) — periodic walker that reads bytes-allocated
  from a `NamespaceSizer` and overwrites the per-repo counters. Drives drift back to zero.
- **`QuotaChecker`** (`storage_quota.go`) — per-user storage quota CRUD + the `Allow` check
  consulted by upload paths.
- **Production adapters** in [`storagewiring/`](./storagewiring) bridge the reconciler to the
  catalog (`CatalogRepoLister`) and to the block adapter (`BlockNamespaceSizer`).

## KV partitions

- `usage` — installation-wide monthly counters (existing).
- `storage` — per-user / per-repo allocated bytes and quotas (this work). See
  [`docs/storage-usage.md`](../../docs/storage-usage.md) for the layout.

## Object-level deletes

`StorageAccountant` deliberately does **not** hook object-level deletes: `block.Adapter.Remove`
does not return the size of the deleted object, and rewiring every call site to look it up first
would couple deletes to a metadata read. The `StorageReconciler` pass corrects the resulting
drift within one interval (default 1 h). Repository-level deletes **are** hooked via
`StorageAccountant.DeleteRepo`, which reads the per-repo counter once and decrements the per-user
total atomically.
