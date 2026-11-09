# Xet garbage collection

Date: 2026-10-26. Status: approved scope, design for planning (revised after the design review).

## Why

Xet file content (xorbs) lives under the hub's `_hub_xet` storage namespace, outside every
repository and shared between them. Deleting a repository removes its file refs, so its quota
usage drops, but the xorb bytes stay in object storage forever. Uploads whose link never
happened (failed, aborted, or retried under another name) leave file records and xorbs that
nothing will ever use. Nothing removes either today: `sghub gc xet` only reports, and its
report is inaccurate (see "Existing defects").

Once xet is switched on in an environment, a sweep is the only way to get that space back.

A measurement of PROD on 2026-10-26 (798 of 799 repositories, regular objects only, xet not
yet enabled) found 6.15 GiB in repository namespaces: 92.8% used by branch heads and
uncommitted changes, 0.3% used only by history, 6.5% (0.40 GiB, mostly one repository) used
by nothing. A regular-object GC is therefore not worth building now; only the xet sweep is in
scope.

## Goals

- `sghub gc xet` deletes xet data that no existing repository holds, safely enough to run
  unattended on a schedule.
- It runs as a Kubernetes CronJob shipped by the hub's Helm chart (`helm/` in this repo).
- `--dry-run` reports exactly what a real run would do and changes nothing.

## Non-goals

- Garbage collection of regular (non-xet) objects. No Spark, now or later.
- Walking commit history. Every commit keeps its files; liveness is decided by file refs.
- Freeing a xet file that was linked and then deleted before any commit. It keeps counting
  against its repository until the repository is deleted, as regular objects do today.
- Making hf_xet clients drop cached xorbs the server has deleted.

## Anti-goals (a run that does any of these is a failure, even if it "works")

- Deleting a file record or xorb that an existing repository's entry still points at.
- Deleting a xorb younger than its grace period.
- Losing data because a run crashes or is killed partway.

## Definitions

- **File ref**: `xet/file_refs/<escaped repo>/<file hash>`, value = file size. Written when a
  repository links a xet file (before the entry), removed when the repository is deleted or
  created. A repository is charged for exactly the refs it holds (see
  `pkg/api/controller.go` `LinkPhysicalAddress`, `pkg/xet/store/register.go`).
- **File record**: `xet/file/<file hash>`, the verified segment list of a file.
- **Xorb footer**: `xet/xorb/<xorb hash>`, the stored footer (chunk hashes, boundaries).
- **Xorb object**: `<xet namespace>/xet/xorbs/default/<xorb hash>` in the blockstore.
- A file is **held** when at least one file ref names it. A xorb is **used** when at least one
  live file record has a segment in it.

## Decided (by the product owner)

- Both xet files and xorbs are collected; regular objects are not.
- History is kept: nothing a commit references may be deleted.
- It is a `sghub` subcommand run periodically by a Kubernetes CronJob.

## Design calls (mine, accepted in conversation or after the design review)

- Liveness is ref-based, not commit-walk based (see Non-goals for the consequence).
- The CronJob lives in the hub's own chart, disabled by default, enabled per environment when
  xet is.
- Deletion is two-phase (park, then delete in a later run), and parked data is **restored on
  demand**: any upload, shard registration or link that needs a parked footer or file record
  brings it back. Restoring and deleting are mutually exclusive through a compare-and-set on
  the parked key, so a run can never delete something a request is bringing back.
- Grace periods count from when data was parked (stopped being needed), not from when it was
  written: file records are deleted 24h after parking, xorbs 7 days after parking. hf_xet
  clients keep a local cache and dedupe new uploads against xorbs they uploaded earlier; a
  client whose cache still references a xorb deleted after 7 days unused fails that upload
  ("xorb ... not found") until its cache is cleared. That residual risk is accepted.
- Nothing in the request paths ever deletes a file ref or answers 404 because of the GC; the
  only failure a client can see is a retryable 503 while a xorb is being deleted, or the cache
  case above.

## Keys and states

- Live keys, unchanged: file record `xet/file/<file hash>`, footer `xet/xorb/<xorb hash>`.
- Parked keys: `xet/gc/file/<file hash>` and `xet/gc/xorb/<xorb hash>`. Value (JSON):
  `{"state": "parked" | "restoring" | "deleting", "parked_at": <unix seconds>, "data": <the
  file record JSON or the footer bytes, base64>}`.
- **Park** (sweep only): create the parked key with state `parked` (`SetIf`, nil predicate:
  only if absent), then delete the live key.
- **Restore** (request paths and sweep): read the parked key; if `parked`, `SetIf` it to
  `restoring` with the read value as predicate; on success write the live key (create only
  if absent — a newer live value wins) and delete the parked key. If the parked key is
  `deleting`, the data is going away: an upload answers 503 (retryable), a registration or a
  link answers as if the data never existed.
- **Delete** (sweep only): `SetIf` the parked key from `parked` to `deleting` with the read
  value as predicate; on success delete the object (xorbs), then the parked key. If the
  `SetIf` fails, a request is restoring it: skip.
- A parked xorb key without `data` (an orphan object, step 9) is restored by rebuilding the
  footer from the stored object, as `postXorb` already does today.
- After the object is removed, the parked key moves to `removed` before it is deleted, so a
  delete resumed after a crash never removes an object again; and a resumed delete of a xorb
  that has a live footer (uploaded again after its object was removed) keeps the object.
- A crash leaves a parked key in `restoring`, `deleting` or `removed`. The next run finishes it: for
  `restoring`, write the live key from `data` if absent and delete the parked key; for
  `deleting`, delete the object (idempotent) and the parked key.

## The sweep

One run, in this order. Each step that needs a complete set (held, used) aborts the run if
the scan that builds it fails, rather than acting on a partial set.

1. **Refs of missing repositories.** Delete file refs whose repository does not exist. A ref
   whose repository exists is never touched.
2. **Held set** = file hashes named by any remaining ref.
3. **Unfinished parked keys.** Finish `restoring` and `deleting` keys as described above.
4. **Parked files.** Held → restore. Not held and parked at least `xet.gc.min_age` ago
   (default 24h) → delete (parked key only; file records have no object).
5. **Park files.** Live file records not held, whose `registered_at` is older than
   `xet.gc.min_age` → park.
6. **Used set** = xorb hashes with a segment in any live file record, after steps 4–5.
7. **Parked xorbs.** Used → restore. Not used and parked at least `xet.gc.xorb_min_age`
   (default 168h) ago → delete.
8. **Park xorbs.** Live footers of xorbs not used whose object is older than
   `xet.gc.min_age` → park. (Object age protects a fresh upload whose shard is not
   registered yet; the 7-day clock starts here.)
9. **Orphan objects.** Xorb objects with no live or parked footer, not used, older than
   `xet.gc.min_age` → park them with no `data` (state `parked`), so step 7 deletes them a
   grace period later. (Left by a footer write that failed after the object write.)

Ages of xorb objects come from the blockstore listing's last-modified time; ages of file
records from a new `registered_at` field.

## Restore on demand in the request paths

- **Xorb upload** (`postXorb`, `pkg/xet/cas/handler.go`). When the object already exists and
  there is no live footer, the current code rebuilds the footer from the object
  (`recordXorbInfo`). Change: first restore from the parked key if there is one. If the
  parked key is `deleting`, answer 503. If there is no parked key either (orphan), take the
  parked key as `restoring` with a create-only `SetIf` before rebuilding the footer from the
  object, so it cannot interleave with step 9 or a later delete; delete the parked key after
  writing the live footer. When the object does not exist, upload as today.
- **Shard registration** (`postShard`, via `xorbInfos`). A missing live footer with a
  `parked` key → restore, then continue. `deleting` or absent → reject as today
  ("xorb ... not found").
- **Link** (`LinkPhysicalAddress`). A missing file record with a `parked` key → restore, then
  continue. Never delete a ref in this path.
- **Reads** (reconstruction, `ReconstructFileRange`). A held file is never parked by a
  correct run, so reads need no change; if a read finds a parked record or footer anyway, it
  restores it like the paths above.

## Changes

- `pkg/xet/store`: `ShardFileInfo` gains `RegisteredAt` (JSON `registered_at`, Unix seconds),
  set by `RegisterFile` on the stored record (the shard parser leaves it zero). Records
  without it count as old. Registry methods to list file records and footers, delete single
  refs, and park, restore and delete parked keys, shared by the sweep and the request paths.
  Keep the existing style of `register.go`.
- `pkg/xet/cas` (`postXorb`, `postShard`/`xorbInfos`, reads) and `pkg/api/controller.go`
  (`LinkPhysicalAddress`): restore on demand, as described above.
- `pkg/xet/gc`: replace `DryRun` with one sweep that runs the steps above and returns a
  report; dry-run computes the same report without writing. Report counts and bytes for every
  step, plus the hashes acted on. Take a KV lease so two runs (a manual run and the CronJob)
  never overlap.
- `cmd/sghub/cmd/gc_xet.go`: `sghub gc xet` sweeps; `--dry-run` reports only. Output stays
  JSON. Exit non-zero if any step failed (it continues past single-item errors and reports
  them, except where a step aborts the run as described above).
- Existing defects to fix along the way:
  - `ListXorbsFromWalker` never sets `XorbRef.Mtime`, so every xorb counts as old. Use the
    walker entry's last-modified time.
  - The scan batch size is read from `xet.read.capability_scan_batch_size`, a key whose only
    other use was removed. Move it to `xet.gc.scan_batch_size` (default 1000).
- Config: `xet.gc.min_age` (exists, 24h), new `xet.gc.xorb_min_age` (168h), new
  `xet.gc.scan_batch_size` (1000). Update `pkg/config` defaults and the example config in
  `helm/values.yaml` if it lists xet keys.
- `helm/`: a CronJob template, values `xetGC.enabled` (false), `xetGC.schedule`
  (`"30 3 * * *"`), `xetGC.dryRun` (false), resources. Same image, config and environment
  (database connection, blockstore credentials) as the hub deployment — reuse the chart's env
  helpers. `concurrencyPolicy: Forbid`, a deadline, small history limits.

## Testing

- Unit tests per step, including: a parked file that gained a ref is restored; a parked xorb
  that became used is restored; age guards count from `parked_at`; refs of existing
  repositories are never deleted; dry-run writes nothing (assert the KV and blockstore are
  unchanged); keys left in `restoring` or `deleting` are finished by the next run.
- Mutual exclusion: a restore and a delete racing on the same parked key, in both orders —
  exactly one wins, and a won delete is never followed by a live key for a deleted object.
- Request paths: re-uploading a xorb whose footer is parked restores it; uploading one whose
  parked key is `deleting` gets 503; registering a shard over a parked footer restores it;
  linking a parked file restores it and keeps the ref.
- End to end on a local quickstart hub with hf_xet (as done for the xet review): upload two
  files through the SDK, delete the repository holding one, run `sghub gc xet` twice with
  the age settings at zero, check the deleted file's record and xorbs are gone and the other
  file still downloads.
- Helm: `helm template` with `xetGC.enabled=true` renders a valid CronJob.

## Left to the builder

- Exact Registry method names and report field names.
- Whether steps share one scan of file records or scan per step (correctness first; PROD will
  hold at most thousands of records).
- CronJob resource defaults and deadline.
