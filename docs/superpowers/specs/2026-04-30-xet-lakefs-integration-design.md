# XET Integration into Surogate-Hub — Design Spec

**Date:** 2026-04-30
**Status:** Approved (brainstorming complete; awaiting implementation plan)
**Owner:** flavius@statemesh.net

## 1. Goal

Integrate HuggingFace's XET content-addressed storage protocol into Surogate-Hub to deliver:

1. **Storage cost reduction** — chunk-level deduplication across all repos in an instance.
2. **Faster uploads of model checkpoints** — re-uploads of similar checkpoints transfer only the new chunks.

Non-goals (v1): HuggingFace Hub API compatibility for end users, migration of existing non-XET data, multi-region CAS, browser/WASM uploads.

## 2. Background

XET (`xet-core`, the protocol used by HuggingFace Hub since 2025) splits files into variable-length chunks via gear-hash content-defined chunking, packs unique chunks into compressed CAS blocks called *xorbs*, and reconstructs files from per-file *shards* (chunk → xorb mapping). The `xet-core` repository contains the client side; the CAS server side is HuggingFace-hosted SaaS.

Background reading: see the brief at the top of this design's brainstorming session and `study/xet-core/` in the repo for the upstream client implementation.

## 3. Decisions Made During Brainstorming

| # | Decision | Alternatives considered |
|---|---|---|
| 1 | Build a CAS server that speaks the XET wire protocol, hosted inside Surogate-Hub | (A) Use HF's hosted CAS — rejected, defeats cost-reduction goal; (C) Implement only the concepts in Go — rejected, loses HF client ecosystem |
| 2 | Smart client speaks XET; existing S3 PUT path is *not* required for XET-stored objects (uploads go through XET only). S3 GET *is* required (S3 reads must transparently work on XET-stored files). | Pure server-side chunking on S3 PUT — rejected, doesn't deliver upload speedup; bidirectional — rejected, write-side overlap unnecessary |
| 3 | HF wire-format compatibility (Option B) — reuse `hf_xet` as the client | Roll our own Go client — deferred to follow-up |
| 4 | Global instance-wide dedup (Option C) | Per-repo / per-namespace — rejected, smaller savings; cross-tenant chunk-existence info-leak is accepted |
| 5 | Single instance-wide CAS bucket | Multi-region — deferred |
| 6 | Eager xorb refcount table — *removed* in §6 in favour of mark-sweep GC; only the file-refs auth index is maintained eagerly | Eager refcount everywhere — rejected, unnecessary bookkeeping |
| 7 | Capability check on reconstruction reads (verify the file_hash is reachable via *some* lakeFS path the user can read) | Trust file_hash as a capability per HF's model — rejected, closes share-the-hash escalation |
| 8 | Reuse `auth.encrypt.secret_key` for JWT signing | New dedicated secret — rejected, more rotation burden |
| 9 | Mark-sweep GC reusing the existing lakeFS GC walker, operator-triggered via `cmd/lakefs gc xet` | Eager refcount + lazy cleanup — rejected, complexity not worth the latency win; always-on internal job — rejected, operators want explicit timing |
| 10 | MVP smart client = Python wrapper over `hf_xet`'s existing PyO3 bindings (~50 LOC), shipped at `clients/python/surogate-xet/` | Standalone Rust CLI — deferred; cgo from Go — deferred; pure-Go reimplementation — deferred |

## 4. Architecture Overview

Three new logical components inside Surogate-Hub:

### 4.1 `pkg/xet/cas` — CAS server endpoints

HTTP routes mounted on the existing lakeFS HTTP server (same process):

| Method | Path | Purpose | Scope |
|---|---|---|---|
| `POST` | `/xet/v1/xorbs/{prefix}/{hash}` | Upload a xorb | `write` |
| `POST` | `/xet/v1/shards` | Register a shard (file → xorb refs) | `write` |
| `GET`  | `/xet/v1/chunks/{prefix}/{hash}` | Global dedup probe | `read` |
| `GET`  | `/xet/v2/reconstructions/{file_hash}` | Return manifest + presigned xorb URLs | `read` + capability |
| `GET`  | `/xet/v1/xorbs/{prefix}/{hash}` | Server-side proxy for xorb bytes (presign fallback) | `read` + capability |
| `POST` | `/xet/v1/token` | Exchange lakeFS creds for a short-lived JWT | (any lakeFS auth) |
| `GET`  | `/xet/v1/token/refresh` | Refresh a still-valid JWT | (current JWT) |

Endpoints implement the HF XET wire format byte-for-byte so `hf_xet` and `huggingface_hub` clients work unmodified by configuration.

### 4.2 `pkg/xet/store` — storage layout

- **Xorbs** → existing `block.Adapter` at instance-wide path `<cas-namespace>/_xet/xorbs/<hash[0:2]>/<hash[2:4]>/<hash>`. Reuses S3/GCS/Azure adapters; durability inherited from the underlying bucket.
- **Shards (file manifests)** → `pkg/kv` under partition `xet`, key `xet/shard/<file_hash>`, value = raw HF binary shard bytes (verbatim, no re-encoding). A small companion `xet/shard_meta/<file_hash>` holds decoded summary fields.
- **Global dedup index** → KV: `xet/chunk/<chunk_hash>` → `<shard_hash>` (first-writer-wins; not overwritten).
- **File-refs index** (for the §5 capability check) → KV: `xet/file_refs/<file_hash>` → set of `(repo, branch, path)` tuples currently staged or committed. Updated on lakeFS object link/unlink. Tolerates stale entries; the read-time check additionally verifies path → file_hash.

### 4.3 `pkg/xet/reconstruct` — read-side reconstruction

A reusable Go module that, given a file_hash, loads its shard from KV and either (a) returns the manifest + presigned xorb URLs (the XET smart-client read path), or (b) streams reconstructed bytes (the S3-gateway read path). Both paths share the same range-mapping core.

### 4.4 Integration with lakeFS objects

A XET-uploaded file appears in graveler as a normal lakeFS object whose `physical_address` is `xet://<file_merklehash>`. Read paths recognise the scheme and dispatch to `pkg/xet/reconstruct` instead of doing a normal `block.Adapter.Get`. Commits, branches, merges, diff, ACLs, and metadata are unchanged. From graveler's perspective, an XET object is an object with an unusual physical address — nothing more.

## 5. Data Model

### 5.1 lakeFS object representation

| Field | Value |
|---|---|
| `physical_address` | `xet://<file_merklehash>` |
| `size_bytes` | total unpacked size from the shard |
| `etag` | `<file_merklehash>` (hex) — content-addressed, stable across commits |
| `metadata["x-xet-file-hash"]` | `<file_merklehash>` (redundant but explicit) |

Reader-side detection is a single string-prefix check on `physical_address`.

### 5.2 KV partition layout

```
xet/shard/<file_hash>           → raw HF shard bytes
xet/shard_meta/<file_hash>      → { created_at, size, num_xorbs, num_chunks }
xet/chunk/<chunk_hash>          → <shard_hash>
xet/file_refs/<file_hash>       → set<(repo, branch, path)>
```

### 5.3 Block storage layout

```
<cas-namespace>/_xet/xorbs/<hash[0:2]>/<hash[2:4]>/<hash>
```

Two-level hash prefixing avoids hot S3 partitions. Single instance-wide CAS namespace to enable global dedup.

## 6. Write Path

Two-phase upload, both phases driven by the smart client.

### 6.1 Phase 1 — XET upload (wire-format)

1. Client (Python wrapper around `hf_xet`) streams the file through gear-hash CDC on a worker pool.
2. Every ~1000 chunks, batches chunk hashes and probes `GET /xet/v1/chunks/{prefix}/{hash}` for global dedup hits — reuses returned xorb refs without re-uploading.
3. Packs new chunks into xorbs (up to 8K chunks / 64 MB per xorb), compresses with LZ4 or BG4-LZ4, and `POST`s xorbs **concurrently** to `/xet/v1/xorbs/{prefix}/{hash}`.
4. Once all xorbs are durable, `POST /xet/v1/shards` registers the shard. Server returns `{ file_hash, was_inserted }`.

### 6.2 Phase 2 — lakeFS staging

Client makes one extra call to the existing lakeFS API:

```
POST /api/v1/repositories/{repo}/branches/{branch}/objects?path=models/llama.safetensors
{
  "physical_address": "xet://<file_hash>",
  "size_bytes":       <unpacked_total>,
  "checksum":         "<file_hash>"
}
```

This reuses lakeFS's existing physical-address linking. The only change required is allowing the `xet://` URI scheme in [pkg/block/namespace.go](../../../pkg/block/namespace.go). Subsequent commits, branches, merges, diffs all work without further XET awareness.

### 6.3 Server-side endpoint behaviour

**`POST /xet/v1/xorbs/{prefix}/{hash}`**
- Recompute MerkleHash of decompressed content; reject mismatch (prevents CAS poisoning).
- Idempotent: if `Exists` returns true, respond `{"was_inserted": false}` without re-uploading.
- Stream body straight into the block adapter; no buffering of full xorb in RAM.
- CPU-bounded concurrency via semaphore `xet.verify.max_concurrent` (default `runtime.NumCPU()`).

**`POST /xet/v1/shards`**
- Parse shard binary format → extract referenced xorb hashes.
- Verify every referenced xorb exists in the CAS via `block.Adapter.Exists`. Reject if any is missing — prevents dangling shards.
- KV transaction: write `xet/shard/<hash>`, `xet/shard_meta/<hash>`, write `xet/chunk/<chunk_hash> → <shard_hash>` for chunks not yet indexed (first-writer-wins).

**`GET /xet/v1/chunks/{prefix}/{hash}`**
- KV lookup `xet/chunk/<hash>` → `<shard_hash>` → load `xet/shard/<shard_hash>` → return raw shard bytes (HF binary format). 404 if unknown.

### 6.4 Concurrency model

| Layer | Parallelism |
|---|---|
| Chunking (client) | `hf_xet` compute pool |
| Xorb upload (client) | `hf_xet` JoinSet — concurrent POSTs |
| Server xorb ingest | per-request goroutines (Go HTTP server default) |
| Server xorb verify | bounded semaphore (NumCPU) |
| Shard register KV writes | single transactional batch |

## 7. Read Paths

### 7.1 XET smart-client read (the fast path)

`GET /xet/v2/reconstructions/{file_hash}`

1. Auth: bearer JWT, `read` scope, capability check (§8.4).
2. Load shard from KV. Optional `Range:` header narrows returned terms.
3. For each referenced xorb, generate presigned GET URLs with byte-ranges via `block.Adapter.GetPreSignedURL` ([pkg/block/adapter.go:194](../../../pkg/block/adapter.go#L194)).
4. Return JSON manifest in the exact V2 wire format: `{ offset_into_first_range, terms[], xorbs{hash → [{url, ranges}]} }`.

Bytes flow object-storage → client directly. lakeFS handles only metadata. This is the path that delivers the upload-speed and download-speed wins.

**Presigning fallback.** When the underlying adapter can't presign (`mem`, `transient`, restricted deployments, or browser CORS contexts), the manifest contains URLs pointing at the server-side proxy: `GET /xet/v1/xorbs/{prefix}/{hash}` with `Range:`.

### 7.2 S3-gateway / lakeFS API read (server-side reconstruction)

When a `GET /repo/branch/path` arrives at the existing S3 gateway:

1. Graveler resolves the path → entry with `physical_address` starting with `xet://`.
2. Gateway dispatches to `reconstruct.Stream(ctx, fileHash, rangeHdr, w)`.
3. `reconstruct.Stream`:
   - Loads shard from KV; locates terms overlapping the requested range.
   - Opens N parallel xorb-fetch goroutines (default `xet.read.parallel_xorbs = 4`).
   - An ordered queue delivers results in term-order; results stream to the response writer.
   - Per-chunk LZ4 / BG4-LZ4 decompression as bytes arrive.
   - Trims leading/trailing bytes when the range starts/ends mid-chunk.

Streaming + bounded buffer keeps RAM flat regardless of file size. Range GETs from parquet/Spark only fetch the xorbs that overlap.

### 7.3 Server-side xorb cache

Bounded local-disk LRU shared by both read paths:

- Default-on, `xet.cache.path = /var/lib/lakefs/xet-cache`, `xet.cache.size_bytes = 10 GB`.
- Eviction: random victim (matches `xet-core`'s client-side cache choice; cheaper than LRU bookkeeping).
- Disable with `xet.cache.enabled: false` for stateless deployments.

### 7.4 ETag and conditional reads

XET file hashes are content-addressed and stable → ideal `ETag` values. The S3 gateway already serves `ETag` from `entry.checksum`; we populate it with the file_hash. `If-None-Match` works for free.

## 8. Auth (JWT)

### 8.1 Token issuer

`POST /xet/v1/token` accepts any existing lakeFS auth method (SigV4, bearer session, Basic). Resolves identity via `pkg/auth`, computes effective scopes from lakeFS RBAC, issues an HS256 JWT signed with the existing `auth.encrypt.secret_key`.

Claims:

```json
{
  "sub":   "<lakefs-user-id>",
  "scope": ["read", "write"],
  "iat":   <epoch>,
  "exp":   <epoch + 15m>,
  "iss":   "<lakefs-instance-id>"
}
```

Response:

```json
{
  "token":              "<JWT>",
  "expires_at_seconds": 1714500000,
  "refresh_url":        "https://lakefs.example.com/xet/v1/token/refresh"
}
```

### 8.2 Refresh

`GET /xet/v1/token/refresh` — authenticated with the still-valid JWT, returns a fresh one. `hf_xet`'s `DirectRefreshRouteTokenRefresher` expects exactly this shape and refreshes 30s before expiry.

### 8.3 Per-request auth middleware

`xetAuthMiddleware` mounted on `/xet/*` extracts the bearer token, verifies HMAC signature, checks scope vs. operation, rejects expired tokens.

### 8.4 Capability check on reconstruction (cross-tenant mitigation)

Before serving a reconstruction or proxy-xorb-read, authorize the request via a two-step check:

1. **Candidate lookup.** Read `xet/file_refs/<file_hash>` to get the set of `(repo, branch, path)` tuples that have ever been linked to this file_hash. The set may include stale tuples (paths that have since been deleted, overwritten, or GC'd) — that's fine.
2. **Verify a current, accessible reference exists.** For each candidate tuple, check that (a) the requester has `fs:ReadObject` on `(repo, branch, path)` AND (b) graveler currently resolves that path to `physical_address = xet://<file_hash>`. Authorize if any tuple passes both.

If no tuple passes, return 404 (not 403; don't leak existence).

The two-step structure is what lets us avoid eager unlink bookkeeping: file_refs is append-only, so it may include stale entries, but step (b) re-verifies through graveler so stale entries can never grant access.

This closes the share-the-hash escalation. It does *not* mitigate the chunk-dedup probe leak (`/v1/chunks/...` reveals chunk existence by design); that's an inherent CAS dedup property and an accepted risk.

## 9. Garbage Collection

### 9.1 Strategy

Mark-sweep, riding on lakeFS's existing GC walker. No eager xorb refcount table.

**Walk:**
1. Existing lakeFS GC commit walker enumerates every live `(repo, ref, path)` entry across branches, tags, retained commits.
2. For each entry whose `physical_address` starts with `xet://`, extract the `file_hash`.
3. For each unique `file_hash`, load the shard and collect referenced `xorb_hash` and `chunk_hash` values into live sets.

**Sweep:**
| Target | Walk | Delete |
|---|---|---|
| Xorbs | `block.Adapter.GetWalker` over `_xet/xorbs/` | objects not in live xorbs set |
| Shards | KV scan over `xet/shard/*` | keys not in live shards set |
| Chunk dedup index | KV scan over `xet/chunk/*` | entries pointing at dead shards |
| File refs (auth index) | rewrite from live set | stale tuples |

### 9.2 In-flight upload safety

Ignore xorbs younger than `xet.gc.min_age` (default 24h). Since xorbs are write-once-immutable, the modification time is the upload time. This protects in-flight uploads and provides a recovery window.

### 9.3 Trigger

`cmd/lakefs gc xet` — operator-triggered CLI subcommand with `--dry-run`. Same pattern as the existing `lakefs gc`. No always-on background goroutine.

### 9.4 Output

Per run, written to logs (and optionally `stats-worker`):

```json
{
  "duration_sec":        ...,
  "live_files":          N,
  "live_shards":         N,
  "live_xorbs":          N,
  "deleted_shards":      N,
  "deleted_xorbs":       N,
  "bytes_reclaimed":     N,
  "skipped_in_flight":   N,
  "errors":              N
}
```

## 10. Configuration

```yaml
xet:
  enabled: true
  cas_namespace: "s3://my-cas-bucket/"   # global xorb storage
  jwt:
    ttl: 15m
    refresh_window: 30s
  cache:
    enabled: true
    path: /var/lib/lakefs/xet-cache
    size_bytes: 10737418240               # 10 GB
  read:
    parallel_xorbs: 4
  verify:
    max_concurrent: 0                     # 0 = runtime.NumCPU()
  gc:
    min_age: 24h
```

JWT signing reuses `auth.encrypt.secret_key` — no new secret material.

## 11. Phased Delivery (MVP)

### Phase 1 — CAS server foundation
- `pkg/xet/cas/` — four wire-compatible endpoints + JWT middleware + token issuer + refresh.
- `pkg/xet/store/` — KV layout, block-adapter access for xorbs.
- `pkg/config/` — new `xet:` section.
- HTTP route registration for `/xet/*`.
- End-to-end smoke test against `hf_xet` (curl-driven upload + dedup probe + manifest fetch).

### Phase 2 — lakeFS object integration
- Allow `xet://` scheme in [pkg/block/namespace.go](../../../pkg/block/namespace.go) validator.
- Existing `linkPhysicalAddress` API accepts XET addresses.
- File-refs index appended on link (the only write hook needed); pruning of stale tuples is handled by GC (Phase 5), not by eager unlink hooks. The §8.4 read-time path-resolution check makes stale entries safe.
- Tests covering stage → commit → diff → branch operations on XET objects.

### Phase 3 — Read paths
- `pkg/xet/reconstruct/` — range mapping, parallel xorb fetch, streaming decompression.
- S3 gateway dispatch on `xet://` physical address.
- lakeFS API GET dispatch.
- Server-side disk LRU cache (default-on, 10 GB).
- Range-GET correctness tests (parquet-style access patterns).

### Phase 4 — Smart client
- `clients/python/surogate-xet/` — Python package wrapping `hf_xet`'s PyO3 bindings + lakeFS staging call.
- Token-exchange helper (lakectl creds → JWT).
- Integration test: upload checkpoint → commit → read via S3 → re-upload similar checkpoint → verify dedup hit-rate on the second upload.

### Phase 5 — GC
- `cmd/lakefs gc xet` CLI subcommand with `--dry-run`.
- Reuse existing GC walker; expand `xet://` addresses; sweep unreferenced xorbs/shards/index entries.
- `min_age` guard; stats output.

## 12. Out of Scope (v1)

| Item | Why deferred |
|---|---|
| Native Go XET client (cgo / WASM / pure-Go) | Python wrapper covers the model-checkpoint workflow |
| Standalone Rust CLI binary | Same — Python wrapper ships faster |
| Migrating existing non-XET objects to XET | Distinct project — one-shot historical dedup tool |
| Xorb pinning / TTL policies | No user demand yet |
| Multi-region CAS replication | Single-bucket assumption |
| WASM browser uploads | webui change, separate project |
| `git_xet` (Git-LFS-style workflow) | Different access pattern, separate project |
| Rate-limiting the chunk-existence probe | Accepted info-leak |
| HuggingFace Hub API compatibility (so `huggingface_hub.upload_file` "just works") | Requires a Hub-API-shaped service, large scope |

## 13. Risks

| Risk | Severity | Plan |
|---|---|---|
| HF wire-format drift (future protocol version breaks compat) | Medium | Pin to v2 reconstruction / v1 xorbs; track HF releases; v3 is a future spec |
| Server CPU on S3-gateway reads of XET objects (cold-read decompression) | Medium | Disk cache absorbs hot reads; measure in load-test. CDN in front if sustained pressure |
| JWT expiry during a multi-hour upload of a 50 GB+ file | Low | Smart client refreshes 30s before expiry via `/xet/v1/token/refresh`. Add a CI test with a low TTL |
| In-flight upload race with GC | Low | `min_age` guard (24h default); xorbs are write-once-immutable |
| `block.Adapter` listing required for GC sweep — `mem`/`transient` may not support it | Low | Production adapters (S3/GCS/Azure) all support `GetWalker`. Doc the constraint |
| Cross-tenant chunk-existence probe (`/v1/chunks/{prefix}/{hash}`) | Accepted | Per global-dedup decision. Future: per-IP / per-token rate-limiting |

## 14. File Map

**New:**

```
pkg/xet/
├── cas/          # endpoint handlers, middleware, JWT
├── store/        # KV layout, shard parse, indexes
├── reconstruct/  # range mapping, streaming, cache
└── gc/           # mark-sweep
cmd/lakefs/cmd/gc_xet.go
clients/python/surogate-xet/
```

**Modified:**

- [pkg/block/namespace.go](../../../pkg/block/namespace.go) — allow `xet://` scheme
- `pkg/api/controller.go` (or wherever `linkPhysicalAddress` lives) — accept `xet://`
- `pkg/gateway/operations/getobject.go` — dispatch on `xet://`
- [pkg/config/config.go](../../../pkg/config/config.go) — new `xet:` section
- HTTP route registration — mount `/xet/*` routes

## 15. References

- Upstream client: [study/xet-core/](../../../study/xet-core/)
- HF wire format: [study/xet-core/openapi/cas.openapi.yaml](../../../study/xet-core/openapi/cas.openapi.yaml)
- Existing block adapter interface: [pkg/block/adapter.go:184](../../../pkg/block/adapter.go#L184)
- KV layer: [pkg/kv/](../../../pkg/kv/)
