# XET Integration into Surogate-Hub — Design Spec

**Date:** 2026-04-30
**Status:** Approved; implementation in progress
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
| 4 | Global instance-wide dedup (Option C) | Per-repo / per-namespace — rejected, smaller savings; cross-tenant dedup metadata leak is accepted (`/v1/chunks` reveals shard bytes for matching chunks) |
| 5 | Single instance-wide CAS bucket | Multi-region — deferred |
| 6 | Eager xorb refcount table — *removed* in §6 in favour of mark-sweep GC; only the file-refs auth index is maintained eagerly | Eager refcount everywhere — rejected, unnecessary bookkeeping |
| 7 | Capability check on reconstruction reads (verify the file_hash is reachable via *some* lakeFS path the user can read) | Trust file_hash as a capability per HF's model — rejected, closes share-the-hash escalation |
| 8 | Reuse `auth.encrypt.secret_key` for JWT signing | New dedicated secret — rejected, more rotation burden |
| 9 | Mark-sweep GC reusing the existing lakeFS GC walker, operator-triggered via `cmd/lakefs gc xet` | Eager refcount + lazy cleanup — rejected, complexity not worth the latency win; always-on internal job — rejected, operators want explicit timing |
| 10 | MVP smart client = Python wrapper over `hf_xet`'s existing PyO3 bindings (~50 LOC), shipped at `clients/python/surogate-xet/` | Standalone Rust CLI — deferred; cgo from Go — deferred; pure-Go reimplementation — deferred |
| 11 | Shard registration uses an explicit crash-safe state machine over single-key `SetIf` (§6.5) — not a multi-key transaction. Per-tuple `file_refs` keys (§5.2) avoid read-modify-write contention. | Adding a transaction abstraction to `pkg/kv` — rejected, large surface change for one feature; storing `file_refs` as a single-key set value — rejected, racy |

## 4. Architecture Overview

Three new logical components inside Surogate-Hub:

### 4.1 `pkg/xet/cas` — CAS server endpoints

HTTP routes mounted on the existing lakeFS HTTP server (same process):

| Method | Path | Purpose | Scope |
|---|---|---|---|
| `POST` | `/xet/v1/xorbs/{prefix}/{hash}` | Upload a xorb | `write` |
| `POST` | `/xet/v1/shards` | Register a shard (file → xorb refs) | `write` |
| `GET`  | `/xet/v1/chunks/{prefix}/{hash}` | Global dedup probe; returns dedup shard bytes | `read` |
| `GET`  | `/xet/v2/reconstructions/{file_hash}` | Return manifest + presigned xorb URLs | `read` + capability |
| `GET`  | `/xet/v1/xorbs/{prefix}/{hash}?grant=...` | Server-side proxy for xorb bytes (presign fallback) | signed reconstruction grant |
| `POST` | `/xet/v1/token` | Exchange lakeFS creds for a short-lived JWT | (any lakeFS auth) |
| `GET`  | `/xet/v1/token/refresh` | Refresh a still-valid JWT | (current JWT) |

Endpoints implement the HF XET wire format byte-for-byte so `hf_xet` and `huggingface_hub` clients work unmodified by configuration.

### 4.2 `pkg/xet/store` — storage layout

- **Xorbs** → existing `block.Adapter` at instance-wide path `<cas-namespace>/_xet/xorbs/<hash[0:2]>/<hash[2:4]>/<hash>`. Reuses S3/GCS/Azure adapters; durability inherited from the underlying bucket.
- **Shards (file manifests)** → `pkg/kv` under partition `xet`, key `xet/shard/<file_hash>`, value = raw HF binary shard bytes (verbatim, no re-encoding). A small companion `xet/shard_meta/<file_hash>` holds decoded summary fields.
- **Global dedup index** → KV: `xet/chunk/<chunk_hash>` → `<file_hash>` (first-writer-wins; not overwritten). The value names the canonical `xet/shard/<file_hash>` key that contains a valid dedup shard for this chunk.
- **File-refs index** (for the §8.4 capability check) → KV: one key per tuple, `xet/file_refs/<file_hash>/<repo>/<ref>/<path>` with empty value (presence = membership). Enumerated via `Scan` with prefix `xet/file_refs/<file_hash>/`. One key per tuple avoids read-modify-write contention on a shared "set" value, since the KV layer in [pkg/kv/store.go](../../../pkg/kv/store.go) only exposes single-key `Get`/`Set`/`SetIf`/`Delete`/`Scan`. Append-only on link/direct-context backfill; pruned by GC. Tolerates stale entries; the read-time check (§8.4) additionally verifies path → file_hash.

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
xet/shard/<file_hash>                                  → raw HF shard bytes
xet/shard_meta/<file_hash>                             → { created_at, size, num_xorbs, num_chunks }
xet/chunk/<chunk_hash>                                 → <file_hash>      # first-writer-wins; points at xet/shard/<file_hash>
xet/file_refs/<file_hash>/<repo>/<ref>/<path>          → ""               # presence = membership; one key per tuple
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
- Parse shard binary format → extract referenced xorb hashes and chunk hashes; compute `file_hash` from shard contents and verify it matches the client-asserted hash.
- Verify every referenced xorb exists in the CAS via `block.Adapter.Exists`. Reject if any is missing — prevents dangling shards.
- Then run the crash-safe registration state machine described in §6.5 (no multi-key KV transaction is available; we sequence single-key `SetIf` calls and define explicit recovery semantics).
- Return `{ "file_hash": "...", "was_inserted": <bool> }` once the canonical shard write (step 1 of §6.5) has succeeded. The chunk-index writes are best-effort and may complete after the response, which is safe because dedup index misses only cost a re-upload, never correctness.

**`GET /xet/v1/chunks/{prefix}/{hash}`**
- KV lookup `xet/chunk/<hash>` → `<file_hash>` → load `xet/shard/<file_hash>` → return raw shard bytes (HF binary format). 404 if unknown.

### 6.4 Concurrency model

| Layer | Parallelism |
|---|---|
| Chunking (client) | `hf_xet` compute pool |
| Xorb upload (client) | `hf_xet` JoinSet — concurrent POSTs |
| Server xorb ingest | per-request goroutines (Go HTTP server default) |
| Server xorb verify | bounded semaphore (NumCPU) |
| Shard register KV writes | per-key (no multi-key transaction; see §6.5) |

### 6.5 Crash-safe shard registration (state machine)

The KV layer ([pkg/kv/store.go](../../../pkg/kv/store.go)) exposes only single-key `Get`/`Set`/`SetIf`/`Delete`/`Scan` — no multi-key transactions. Shard registration touches three kinds of keys (canonical shard, decoded meta, chunk-index entries) and must remain crash-safe and idempotent under retries and concurrent uploads of the same `file_hash`.

We sequence the writes as a state machine, ordered so that any partial state observed by a reader or a crashed writer is either correct or self-healing on retry.

**Steps (in order):**

1. **Canonical shard write — the commit point.**
   `SetIf(xet/shard/<file_hash>, raw_shard_bytes, predicate=absent)`
   - Single-key `Set` is atomic in every backend. Either the full bytes land or none do.
   - Success → this server "won" the registration; continue.
   - `ErrPredicateFailed` → another writer (concurrent or earlier) won. Their bytes are now durable. Treat as a dedup hit: skip step 2 (already written or will be), continue to step 3 (it's safe to add chunk-index entries that point at the existing shard).
2. **Decoded meta write.**
   `Set(xet/shard_meta/<file_hash>, summary_json)`
   - Plain `Set`; idempotent (same `file_hash` always produces the same summary).
   - Crash between (1) and (2) leaves shard present, meta absent. Readers fall back to parsing the shard bytes (§6.5 reader rule R2 below).
3. **Chunk-index publication — best-effort, idempotent.**
   For each `chunk_hash` in the shard, in any order, possibly concurrent:
   `SetIf(xet/chunk/<chunk_hash>, file_hash, predicate=absent)`
   - `ErrPredicateFailed` → fine, another shard already indexed this chunk; leave the existing pointer untouched (first-writer-wins).
   - Crash mid-step leaves a partial chunk index. Future uploads of the same data will re-call `SetIf` and complete the missing entries (or GC will sweep dead pointers — see §9). Missing chunk entries cost a missed dedup, never correctness.
4. **Respond.** Return `{ file_hash, was_inserted }` to the client. `was_inserted = true` only if step 1 succeeded for this writer.

**Reader tolerance rules:**

- **R1.** `GET /xet/v1/chunks/{prefix}/{hash}` reads `xet/chunk/<hash>` → `<file_hash>` → `xet/shard/<file_hash>`. If the shard is absent (race with deletion or partial write before step 1 commits), return 404; the client falls through to upload the chunk. Always correct.
- **R2.** Anywhere we read `xet/shard_meta/<file_hash>`: tolerate absence by parsing summary fields out of `xet/shard/<file_hash>` instead. Meta is a cache, not a source of truth.
- **R3.** Reconstruction paths read `xet/shard/<file_hash>` only. If absent → 404. Step 1 is the visibility gate.

**Concurrency invariants:**

- Two clients uploading the same `file_hash` simultaneously: only one wins step 1 (`SetIf` absent). The loser observes the predicate failure and treats it as a dedup hit. Both proceed to step 3 with the same chunk → file_hash mapping; per-chunk `SetIf` ensures no clobbering.
- Two clients uploading *different* shards that share chunks: each independently runs step 3 for its own chunks; first-writer-wins on each `xet/chunk/<chunk_hash>` key. Either pointer is a valid dedup hit.

**Recovery / re-drive:**

A crashed registration where step 1 completed but step 3 didn't fully run leaves the system safe but with reduced dedup. We do not need a foreground recovery loop; the system self-heals over time as new uploads encounter the same chunks and complete the missing index entries via step 3. (Optional future enhancement: a `cmd/lakefs xet repair` operator command that re-walks all shards and re-runs step 3 for any missing chunk entries. Out of scope for v1.)

## 7. Read Paths

### 7.1 XET smart-client read (the fast path)

`GET /xet/v2/reconstructions/{file_hash}` with optional query parameters `repo`, `ref`, and `path` when the caller knows the lakeFS logical object being reconstructed.

1. Auth: bearer JWT, `read` scope, capability check (§8.4). The Surogate Python wrapper supplies `repo/ref/path` after upload/link so the direct logical-context check is the normal path; unmodified HF clients can still rely on the `file_refs` scan fallback.
2. Load shard from KV. Optional `Range:` header narrows returned terms.
3. For each referenced xorb, generate presigned GET URLs with byte-ranges via `block.Adapter.GetPreSignedURL` ([pkg/block/adapter.go:194](../../../pkg/block/adapter.go#L194)).
4. Return JSON manifest in the exact V2 wire format: `{ offset_into_first_range, terms[], xorbs{hash → [{url, ranges}]} }`.

Bytes flow object-storage → client directly. lakeFS handles only metadata. This is the path that delivers the upload-speed and download-speed wins.

**Presigning fallback.** When the underlying adapter can't presign (`mem`, `transient`, restricted deployments, or browser CORS contexts), the manifest contains URLs pointing at the server-side proxy: `GET /xet/v1/xorbs/{prefix}/{hash}?grant=<signed-grant>` with `Range:`.

The signed grant is minted only after the reconstruction capability check succeeds. It is an HMAC-signed compact token using the same XET JWT signing key, with claims:

```json
{
  "sub": "<lakefs-user-id>",
  "file_hash": "<file_hash>",
  "xorb_hash": "<xorb_hash>",
  "ranges": [{"start": 0, "end": 131071}],
  "iat": <epoch>,
  "exp": <epoch + 15m>
}
```

`GET /xet/v1/xorbs/{prefix}/{hash}` verifies the grant signature, expiry, `xorb_hash`, and requested `Range:` against the granted byte ranges. It does **not** try to re-run the `file_refs` capability scan, because the proxy route does not carry a logical lakeFS path and cannot safely infer one from an xorb hash alone.

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

Before serving a reconstruction (and before minting any proxy-xorb grant), authorize the request via one of two paths:

1. **Direct logical-context check (preferred).** If the request carries `(repo, ref, path)` context, check that (a) the requester has `fs:ReadObject` on `(repo, ref, path)` AND (b) graveler currently resolves that path to `physical_address = xet://<file_hash>`. Authorize if both pass. If this succeeds and the corresponding `file_refs` key is missing, backfill `xet/file_refs/<file_hash>/<repo>/<ref>/<path>` best-effort.
2. **Candidate enumeration fallback.** If no logical context is supplied, `Scan` with prefix `xet/file_refs/<file_hash>/` to enumerate candidate `(repo, ref, path)` tuples that have ever been linked to this file_hash. The enumerated set may include stale tuples (paths that have since been deleted, overwritten, or GC'd) — that's fine.
3. **Verify a current, accessible reference exists.** For each candidate tuple, check that (a) the requester has `fs:ReadObject` on `(repo, ref, path)` AND (b) graveler currently resolves that path to `physical_address = xet://<file_hash>`. Authorize if any tuple passes both. Short-circuit on first match.

If no tuple passes, return 404 (not 403; don't leak existence).

**Why this is safe under the §6.5 model:**

- `file_refs` is append-only on successful link (§5.1, §11 Phase 2). It can be stale (point at deleted paths). It may under-count only after a crash between the graveler write and the `file_refs` write; the direct logical-context check repairs that state by verifying graveler directly and backfilling the missing key.
- Step (b) re-verifies through graveler — the source of truth for "what is this path's physical_address right now?" — so stale `file_refs` entries can never grant access.
- The link ordering rule (graveler entry written before `file_refs` entry — §11 Phase 2) ensures that any `file_refs` entry observed has at some point been backed by a real graveler entry. The reverse state (graveler entry exists but `file_refs` does not) is safe: S3/lakeFS path reads still authorize through the normal path, and XET reconstruction can authorize via direct logical context or converge after a retried link/repair.

**Performance:**

A direct logical-context check is one auth decision plus one graveler lookup. The fallback `Scan` plus per-tuple graveler lookup is a few KV ops for typical files referenced by 1-3 tuples. For pathological cases (the same file linked at thousands of paths), `xet.read.capability_scan_batch_size` (default 32) controls scan page size and per-page verification work; it is **not** a correctness cap. Continue scanning until an accessible live tuple is found, the iterator is exhausted, or the request context/deadline is cancelled. If the context is cancelled before exhaustion, return 503/timeout rather than a false 404.

This closes the share-the-hash escalation for reconstruction and xorb bytes. It does *not* mitigate the global dedup probe leak: `/v1/chunks/...` returns raw HF shard bytes for matching chunks, which reveals chunk existence and shard/xorb metadata by design. That disclosure is required for HF-compatible global dedup and is an accepted risk; the proxy grant model in §7.1 ensures those leaked xorb hashes are not sufficient to fetch xorb bytes through Surogate-Hub.

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
| Xorbs | `block.Adapter.GetWalker` over `_xet/xorbs/` | objects not in live xorbs set, older than `min_age` |
| Shards | KV `Scan` over `xet/shard/*` | keys not in live shards set |
| Chunk dedup index | KV `Scan` over `xet/chunk/*` | entries whose pointed shard is gone |
| File refs (auth index) | KV `Scan` over `xet/file_refs/*` | entries whose `(repo, ref, path)` no longer resolves to the indexed `<file_hash>` in graveler |

The file-refs sweep is a per-entry path-resolution check, not a "rewrite the set". Concurrency-safe because:
- A new link writes the graveler entry first, then the `file_refs` key. So at any time GC observes a `file_refs` key, the graveler entry was at some point in the past.
- If GC reads `file_refs` and then graveler resolves correctly → keep. If graveler doesn't resolve → delete (the link no longer represents a live reference). A subsequent re-link recreates the entry; correct.
- The narrow race (link in flight: graveler write done, `file_refs` write pending, GC happens to scan in between) cannot occur because GC reads `file_refs` keys, not graveler — and a `file_refs` key not yet written doesn't appear in the scan, so GC has nothing to delete.

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
    capability_scan_batch_size: 32
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
- **Link ordering invariant.** Inside the link handler, the sequence is: (1) validate `xet://` scheme and canonical shard existence, (2) write the graveler entry, (3) `Set(xet/file_refs/<file_hash>/<repo>/<branch>/<path>, "")`. Link-time refs are branch refs because staging writes happen on branches; direct-context reconstruction may later backfill tag/commit refs using the same `<ref>` key slot. This ordering ensures that whenever a `file_refs` key exists, the graveler entry it refers to either currently exists or has at some point existed — required for the §9.1 GC race-freedom argument. If step (3) fails after step (2) succeeds, return a retryable 5xx; retries and the §8.4 direct logical-context backfill converge the index.
- Tests covering stage → commit → diff → branch operations on XET objects, including a crash-injection test that fails between steps (2) and (3), verifies S3/lakeFS path reads still work, and asserts that either a re-attempted link or a direct-context reconstruction backfills `file_refs`.

### Phase 3 — Read paths
- `pkg/xet/reconstruct/` — range mapping, parallel xorb fetch, streaming decompression.
- S3 gateway dispatch on `xet://` physical address.
- lakeFS API GET dispatch.
- Presign fallback grants: reconstruction responses that use server-side proxy URLs include signed xorb grants; proxy reads reject missing, expired, wrong-xorb, and out-of-range grants.
- Server-side disk LRU cache (default-on, 10 GB).
- Range-GET correctness tests (parquet-style access patterns).

### Phase 4 — Smart client
- `clients/python/surogate-xet/` — Python package wrapping `hf_xet`'s PyO3 bindings + lakeFS staging call.
- Token-exchange helper (lakectl creds → JWT).
- Reconstruction helper supplies `repo/ref/path` context when invoking the Surogate reconstruction endpoint so the direct logical-context capability check is used after upload/link.
- Integration test: upload checkpoint → commit → read via S3 → re-upload similar checkpoint → verify dedup hit-rate on the second upload.

### Phase 5 — GC
- `cmd/lakefs gc xet` CLI subcommand with `--dry-run`.
- Reuse existing GC walker; expand `xet://` addresses; sweep unreferenced xorbs/shards/index entries.
- `min_age` guard; stats output.

### 11.1 Implementation Progress and TODOs

Last updated: 2026-04-30.

**Working rule:** baby steps, focused tests first, commit after each green slice.

**Completed and committed:**

- [x] Tightened the design spec around crash-safe shard registration, per-tuple `file_refs`, capability-check scanning, GC file-ref sweeping, and the explicit no-`pkg/kv`-transactions decision.
- [x] Added `pkg/xet/store.Registry` with canonical shard registration, `SetIf` commit point, best-effort shard meta, chunk-index publication, chunk dedup lookup, and `file_refs` helpers.
- [x] Added minimal `pkg/xet/cas` routes for shard registration and chunk dedup probes.
- [x] Mounted `/xet/*` on the API server behind explicit lakeFS authentication.
- [x] Added `xet://<file_hash>` validation to the link-physical-address path and record `file_refs` only after the graveler entry is written.
- [x] Added focused unit and ESTI coverage for shard registration, dedup probe, authenticated route mount, XET physical-address linking, and missing-shard rejection.
- [x] Added a block-adapter-backed xorb CAS store and idempotent `POST /xet/v1/xorbs/{prefix}/{hash}` route.
- [x] Wired the xorb store into the API server under an instance-wide XET storage namespace.
- [x] Added current JSON-shim shard registration validation for declared `xorb_ids` and ESTI coverage for xorb-backed shard registration.
- [x] Added current JSON-shim `file_hash` verification before shard registration, with focused handler and ESTI coverage.

**In progress:**

- [ ] Replace the current JSON shard-registration shim with real HF/XET binary shard parsing:
  - [ ] Extract referenced xorb hashes, chunk hashes, file size, and summary fields from the HF binary shard.
  - [ ] Compute the real XET file MerkleHash and verify it matches the asserted `file_hash`.
  - [ ] Store raw binary shard bytes verbatim in `xet/shard/<file_hash>`.
  - [ ] Update dedup probe tests to assert returned bytes are the original binary shard.
  - [ ] Run focused parser and CAS handler tests.
  - [ ] Commit as `feat(xet): parse binary shards`.

**Remaining TODOs:**

- [ ] Verify xorb upload content:
  - [ ] Parse/decompress xorb payloads enough to recompute and validate the uploaded xorb hash.
  - [ ] Add `xet.verify.max_concurrent` CPU-bound verification control.
  - [ ] Keep idempotent duplicate-upload behavior unchanged.
- [ ] Implement reconstruction reads:
  - [ ] Add `pkg/xet/reconstruct` range mapping over shard terms.
  - [ ] Add manifest generation for `GET /xet/v2/reconstructions/{file_hash}`.
  - [ ] Add block-adapter presigned URL support and server-side proxy fallback grants.
  - [ ] Add S3 gateway and lakeFS API GET dispatch for `xet://` physical addresses.
  - [ ] Add range-read correctness tests.
- [ ] Implement XET token auth:
  - [ ] Add `POST /xet/v1/token` and `GET /xet/v1/token/refresh`.
  - [ ] Issue short-lived JWTs signed with `auth.encrypt.secret_key`.
  - [ ] Enforce read/write scopes per XET route.
- [ ] Implement reconstruction capability checks:
  - [ ] Direct `(repo, ref, path)` authorization and graveler verification path.
  - [ ] `file_refs` `Scan` fallback with `xet.read.capability_scan_batch_size`.
  - [ ] Best-effort direct-context backfill for missing `file_refs`.
  - [ ] Return 404, not 403, when no accessible live tuple exists.
- [ ] Add crash-injection coverage for link ordering:
  - [ ] Fail after graveler write and before `file_refs` write.
  - [ ] Verify normal lakeFS/S3 path reads still work.
  - [ ] Verify retry or direct-context reconstruction backfills the missing `file_refs` key.
- [ ] Add XET GC:
  - [ ] Add `cmd/lakefs gc xet --dry-run`.
  - [ ] Reuse the lakeFS GC walker to mark live XET shards and xorbs.
  - [ ] Sweep stale shards, chunk-index entries, xorbs older than `xet.gc.min_age`, and stale per-tuple `file_refs`.
- [ ] Add smart-client smoke:
  - [ ] Add a curl or `hf_xet` smoke test that uploads xorbs, registers a shard, links the `xet://` object, reads it back, and verifies a second similar upload gets dedup hits.

**Known test-suite status:**

- Focused XET unit/API/ESTI tests passed at the last green checkpoint.
- The full local ESTI suite was run and failed on pre-existing local-suite issues unrelated to the focused XET path: stale runner flag usage, read-only repository setup, local import configuration, `lakectl` golden output drift, help-branding drift, and a multipart upload panic.

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
| Rate-limiting the global dedup metadata probe | Accepted info-leak |
| HuggingFace Hub API compatibility (so `huggingface_hub.upload_file` "just works") | Requires a Hub-API-shaped service, large scope |

## 13. Risks

| Risk | Severity | Plan |
|---|---|---|
| HF wire-format drift (future protocol version breaks compat) | Medium | Pin to v2 reconstruction / v1 xorbs; track HF releases; v3 is a future spec |
| Server CPU on S3-gateway reads of XET objects (cold-read decompression) | Medium | Disk cache absorbs hot reads; measure in load-test. CDN in front if sustained pressure |
| JWT expiry during a multi-hour upload of a 50 GB+ file | Low | Smart client refreshes 30s before expiry via `/xet/v1/token/refresh`. Add a CI test with a low TTL |
| In-flight upload race with GC | Low | `min_age` guard (24h default); xorbs are write-once-immutable |
| `block.Adapter` listing required for GC sweep — `mem`/`transient` may not support it | Low | Production adapters (S3/GCS/Azure) all support `GetWalker`. Doc the constraint |
| Cross-tenant dedup probe (`/v1/chunks/{prefix}/{hash}` reveals chunk existence plus HF shard/xorb metadata) | Accepted | Per global-dedup decision. Xorb bytes still require reconstruction-presigned object URLs or a signed proxy grant. Future: per-IP / per-token rate-limiting |
| Proxy-xorb URL used outside the authorized reconstruction | High | Proxy URLs carry short-lived signed grants bound to user, file hash, xorb hash, byte ranges, and expiry; proxy rejects requests outside the grant |
| `file_refs` under-count after crash between graveler write and index write | Medium | Link returns retryable 5xx on index-write failure; direct logical-context reconstruction verifies graveler directly and backfills the missing key |

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
