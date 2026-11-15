# Upstream and adaptation

Source: [cloudflare/workers-sdk](https://github.com/cloudflare/workers-sdk/tree/264fb4408b40f6847b6f0eaaac4c190bea46ecbd/packages/miniflare/src/workers/r2),
commit `264fb4408b40f6847b6f0eaaac4c190bea46ecbd`, tag
`miniflare@5.20260921.0-alpha`. MIT, Copyright (c) 2020 Cloudflare, Inc.
The full permission/copyright notice is in [LICENSE-MIT](LICENSE-MIT); copied
source files also carry an SPDX identifier and pinned origin comment.

- `validator.worker.ts`: original validation and condition semantics; shared import redirected locally.
- `errors.worker.ts`: original wire errors; shared import redirected locally.
- `r2Object.worker.ts`: original object/wire encoding; local import and explicit propagation of body-pipe failure.
- `schemas.worker.ts`: original protocol schemas/types and the three metadata tables; local imports, chunk layouts and upload creation/expiry timestamps added. The part-to-object FK remains deferred; the upload FK is omitted so completed parts can outlive expired upload records.
- `range.ts`, `zod.worker.ts`: original shared helpers, with provenance headers.
- `sql.worker.ts`: original named-parameter/transaction helpers; execution uses native `sql.exec()` instead of Miniflare's `sql.prepare()` shim.
- `constants.ts`: supported limits/headers, 8 MiB chunks, 128 MiB parts, 1 GiB objects and a seven-day upload lifetime.
- `shared.ts`: small framework replacement; base64 and metadata-prefix helpers retain Miniflare behavior, with exact prefix length validation.
- `bucket.worker.ts`: retains Miniflare regular-object SQL, delimiter grouping, list pagination and conditional response encoding. Removes framework decorators and the process-local read locks; uses native DO state, HTTP BLOBS, durable retirement queue, pre-upload admission and post-upload transactional condition recheck. Cursor/startAfter maximum uses UTF-8 bytes, matching SQLite key ordering rather than JavaScript UTF-16 ordering.
- `blobs.ts`, `index.ts`, `build.mjs`: Coworkerd HTTP/entry/build adapters (Apache-2.0).

The schema dependency Zod is pinned to `4.4.3` (MIT) in `package-lock.json`;
esbuild `0.28.1` is a build dependency. Dependencies are installed rather than
vendored. Their npm packages include their respective license notices. No
Miniflare runtime framework or local filesystem blob implementation is required.

## Chunked-object extension

The chunking/stream adapter in `chunks.ts` is Coworkerd code (Apache-2.0).
The object table adds upload chunk size/count while migrating old single-blob
rows. The final claiming transaction and Miniflare condition/checksum semantics
remain intact. Retirement now runs only in alarms, with a persistent retry state;
request handlers enqueue names and schedule an alarm. Both list queries use prefix lower/upper bounds without changing filtering or
ordering. Bounded and unbounded statements are compiled separately so SQLite
uses both index bounds instead of a nullable-parameter OR filter.

`r2Object.worker.ts` now pumps metadata and body through one final length-checked
stream using a reusable 64 KiB BYOB buffer. The chunk reader is a byte stream
that fills consumer-provided buffers. The first blob response is checked before
metadata is exposed, including empty objects. Wire fields remain unchanged.

## Multipart extension

`multipart.ts` restores the pinned Miniflare multipart SQL and the original
register/complete/abort transaction checks and ordering (upstream lines 250–354,
425–612). Random part/upload identifiers, separate MD5 storage, MD5-N object
ETags, sorting, minimum/equal-size rules, unknown/final-state errors and
idempotent terminal abort behavior follow that source. Adaptations are:

- Part rows contain the same chunk layout as a regular PUT and share its streaming
  hash/upload helper. The common BYOB reader plans ranges across ordered parts.
- Transactional retirement queues replace framework background deletion/read
  locks. Late or failed claims queue every attempted chunk; no process-local
  reader locks remain. BLOBS retention protects captured versions.
- Known part length and a 128 MiB limit are checked before writing; selected total
  size is checked against 1 GiB before any completion mutations.
- Registration/completion recheck upload expiry. Alarm-driven expiry and terminal
  record cleanup are new. The trusted TTL test binding follows Miniflare's pattern
  of an environment-only test override; tenant request metadata cannot select it.
- Each GC row owns its retry deadline/delay. Fresh names and upload expiry remain
  independent of failed-name backoff; no bucket-wide retry gate is used.

The original framework, filesystem blobs and read-lock machinery are not runtime
dependencies. Metadata/wire encoders and errors retain their original license.
