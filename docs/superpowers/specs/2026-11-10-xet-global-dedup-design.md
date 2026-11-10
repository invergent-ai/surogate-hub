# Xet chunk-level deduplication (the CAS chunk lookup)

Date: 2026-11-10. Status: approved by the product owner ("fix it"), built on branch `feat/xet-global-dedup`.

## Why

hf_xet dedups an upload against chunks the server already holds by calling
`GET /v1/chunks/{prefix}/{chunk hash}`. The hub does not implement it, so a client only
dedups against its own local cache. Measured on DEV (R2, hf_xet 1.4.3, fresh client cache)
on a 256 MiB `model.safetensors`:

- re-uploading identical content sent the whole file and stored 239 MiB of new xorbs, because
  the original import had packed its chunks into xorbs shared with other files;
- changing 1 KiB in the middle sent the whole file and stored one new 59 MiB xorb.

With the lookup, the client skips every chunk the hub already has: an identical re-upload sends
almost nothing, a small edit sends the changed chunks.

## Contract (hf_xet 1.4.3, `xet-core` v1.4.3)

- The client queries the first chunk of each file and chunks whose hash satisfies
  `word[3] % 1024 == 0` (word 3 = bytes 24..32 of the raw hash, little endian), at most one
  query per 256 chunks. `prefix` is `default-merkledb`; the token is the upload (write) token.
- 200 with a shard body, or 404. The body is loaded with `import_shard_from_bytes`, i.e. it must
  be a complete MDB shard file (header, file section, xorb section, lookup tables, footer).
- The client then dedups against the shard's xorb sections through its chunk lookup table, keying
  its own chunk hashes with the footer's `chunk_hash_hmac_key` when one is set, and ignores the
  shard after `shard_key_expiry`.

## Design

- **Index.** When `postShard` registers verified files, for each file it records
  `xet/chunk/<escaped scope>/<chunk hash>` → `<file hash>|<xorb hash>`, for the file's first chunk
  and for every dedup-eligible chunk (the rule above) of the distinct xorbs it references. An
  entry whose file record is still live is kept, so an upload that is never linked cannot take
  over a live file's entries; an entry whose file is gone is replaced. Indexing errors are logged
  and do not fail the upload.
- **Scope.** `xet.dedup.scope`: `owner` (default) keys the index by the owner of the repository
  named in the upload token, so a lookup only finds chunks uploaded to the same owner's
  repositories; `global` uses one index for all owners, as Hugging Face does. Owner scope keeps one
  project from confirming that another project holds a given file.
- **Lookup.** `GET /v1/chunks/{prefix}/{hash}` (write scope): read the index entry, load the file
  record (live only, never restored), skip its segments up to the indexed xorb (no footer reads),
  check that xorb's live footer holds the chunk, and return it plus the following live xorbs, up
  to 32 (~2 GiB of data; ~1.5 MiB of shard, at most ~17 MiB when every chunk is minimum size).
  404 when there is no entry, the file record or the indexed xorb's footer is gone, or it does not
  hold the chunk.
- **Response shard.** Header, empty file section (bookend), the selected xorbs sorted by hash with
  their chunk entries (keyed chunk hash, unpacked start offset, unpacked size), bookend, an empty
  file lookup table, the xorb lookup table, the chunk lookup table sorted by truncated keyed hash,
  and the footer: the HMAC key, creation time, expiry, stored byte totals.
- **Key.** `HMAC-SHA256(server secret, scope | UTC date)`. The key is public anyway (it is in the
  footer); keyed hashes only keep a caller from learning hashes of chunks it lacks. A random key
  per answer would make the client keep one shard collection per answer and hash every chunk once
  per collection, slowing large uploads down.
- **Validity.** `min(xet.gc.xorb_min_age / 4, 24h)`; lookups are off (404) below one hour.

## Safety

- **A xorb hash is a read capability unless shards are verified and links are claimed.** A file
  hash is `hmac0(merkle root of its chunks)` and a xorb hash is the merkle root of its chunks, so
  the hash of a file that is exactly one whole xorb follows from the xorb hash, which reads (and
  now chunk lookups) hand out. Two checks close this, and both are required:
  - `postShard` requires each segment's verification entry (`range_hash = blake3_keyed(
    VERIFICATION_KEY, concat of the range's raw chunk hashes)`, always sent by hf_xet) and checks it
    against the stored footer, so registering a file requires the real chunk hashes. Errors never
    echo hashes the hub computed.
  - `postShard` records an upload claim for the repository named in the upload token; linking a
    `xet://` file into a repository requires that claim or an existing ref in the same repository,
    and answers "not found" otherwise. Claims are dropped with the repository's refs.
- **No chunk hashes leak.** Chunk hashes are only returned keyed, so a client learns which of the
  chunks it already has the hub holds, never the hashes of chunks it lacks. File records are never
  included. A one-chunk xorb's hash equals its chunk's hash, so after the first xorb (which holds
  the queried chunk, which the caller has) one-chunk xorbs are never offered.
- **GC.** The lookup only returns xorbs whose footer is live and that a live file record uses, so
  GC does not consider them unused. If GC parks one before the client's `postShard`, `postShard`
  restores it as today. A xorb is deleted only after `xorb_min_age` parked; an answer expires after
  at most a quarter of that. A long-running client process keeps loaded shards past their expiry
  (hf_xet only checks expiry when it scans its cache directory), so it can still dedup against a
  deleted xorb; `postShard` then fails with 400 until the process restarts. hf_xet's own local
  cache already has that exposure (3-week expiry).
- Index keys of files GC deletes stay until the next index write for the chunk; the lookup treats
  them as misses. They are one key per ~64 MiB of data.

## Testing

- Unit: dedup eligibility matches the client rule; the shard writer's offsets, lookup tables,
  keyed hashes and footer are consistent and parse back; `postShard` indexes the first chunk and
  eligible chunks per scope; the lookup returns 404 for unknown, other-owner, parked or deleted
  data, and a keyed shard holding the file's xorbs otherwise.
- End to end with hf_xet on DEV (`~/xet-dev-test/branch_dedup.py`): an identical re-upload stores
  no new xorbs and a 1 KiB change stores at most one small xorb, from a fresh client cache.
