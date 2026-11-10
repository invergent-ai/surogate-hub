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
  `xet/chunk/<escaped scope>/<chunk hash>` → file hash, for the file's first chunk and for every
  chunk of the xorbs it references that is dedup-eligible (the rule above). Last write wins, so
  the index follows the newest file. Indexing errors are logged and do not fail the upload.
- **Scope.** `xet.dedup.scope`: `owner` (default) keys the index by the owner of the repository
  named in the upload token, so a lookup only finds chunks uploaded to the same owner's
  repositories; `global` uses one index for all owners, as Hugging Face does. Owner scope keeps one
  project from confirming that another project holds a given file.
- **Lookup.** `GET /v1/chunks/{prefix}/{hash}` (write scope): read the index entry, load the file
  record (live only, never restored), walk its segments' xorbs in file order, keep those with a
  live footer, and start at the first one holding the queried chunk. Return up to 32 of them
  (~2 GiB of data, ~1.5 MiB of shard). 404 when there is no entry, the file record or footers are
  gone, or no live xorb holds the chunk.
- **Response shard.** Header, empty file section (bookend), the selected xorbs sorted by hash with
  their chunk entries (keyed chunk hash, unpacked start offset, unpacked size), bookend, an empty
  file lookup table, the xorb lookup table, the chunk lookup table sorted by truncated keyed hash,
  and the footer: a fresh random HMAC key per response, creation time, expiry 24h later, stored
  byte totals.

## Safety

- **No chunk hashes leak.** Chunk hashes are only returned keyed with a random per-response key, so
  a client learns which of the chunks it already has the hub holds, never the hashes of chunks it
  lacks. Registering a file over chunks still requires their real hashes (`postShard` verifies the
  file hash against the stored footers), so the response gives no way to read data the caller does
  not already have. File records are never included. A one-chunk xorb's hash equals its chunk's
  hash, so after the first xorb (which holds the queried chunk, which the caller has) one-chunk
  xorbs are never offered.
- **GC.** The lookup only returns xorbs whose footer is live and that a live file record uses, so
  GC does not consider them unused. If GC parks one before the client's `postShard`, `postShard`
  restores it as today. A xorb is deleted only after 7 days parked; the response expires after 24h,
  so a client never dedups against a xorb that can already be deleted.
- Index keys of files GC deletes stay until the next index write for the chunk; the lookup treats
  them as misses. They are one key per ~64 MiB of data.

## Testing

- Unit: dedup eligibility matches the client rule; the shard writer's offsets, lookup tables,
  keyed hashes and footer are consistent and parse back; `postShard` indexes the first chunk and
  eligible chunks per scope; the lookup returns 404 for unknown, other-owner, parked or deleted
  data, and a keyed shard holding the file's xorbs otherwise.
- End to end with hf_xet on DEV (`~/xet-dev-test/branch_dedup.py`): an identical re-upload stores
  no new xorbs and a 1 KiB change stores at most one small xorb, from a fresh client cache.
