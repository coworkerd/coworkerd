# Upstream and adaptation

Source: [cloudflare/workers-sdk](https://github.com/cloudflare/workers-sdk/tree/264fb4408b40f6847b6f0eaaac4c190bea46ecbd/packages/miniflare/src/workers/r2),
commit `264fb4408b40f6847b6f0eaaac4c190bea46ecbd`, tag
`miniflare@5.20260921.0-alpha`. MIT, Copyright (c) 2020 Cloudflare, Inc.
The full permission/copyright notice is in [LICENSE-MIT](LICENSE-MIT); copied
source files also carry an SPDX identifier and pinned origin comment.

- `validator.worker.ts`: original validation and condition semantics; shared import redirected locally.
- `errors.worker.ts`: original wire errors; shared import redirected locally.
- `r2Object.worker.ts`: original object/wire encoding; local import and explicit propagation of body-pipe failure.
- `schemas.worker.ts`: original protocol schemas/types and object schema; local imports and unused multipart SQL tables removed. Multipart request schemas remain for provenance but are rejected before dispatch.
- `range.ts`, `zod.worker.ts`: original shared helpers, with provenance headers.
- `sql.worker.ts`: original named-parameter/transaction helpers; execution uses native `sql.exec()` instead of Miniflare's `sql.prepare()` shim.
- `constants.ts`: supported limits and headers only; single-blob size is 25 MiB.
- `shared.ts`: small framework replacement; base64 and metadata-prefix helpers retain Miniflare behavior, with exact prefix length validation.
- `bucket.worker.ts`: retains Miniflare regular-object SQL, delimiter grouping, list pagination and conditional response encoding. Removes multipart storage/assembly and framework decorators; uses native DO state, HTTP BLOBS, durable retirement queue, pre-upload admission and post-upload transactional condition recheck. Cursor/startAfter maximum uses UTF-8 bytes, matching SQLite key ordering rather than JavaScript UTF-16 ordering.
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
request handlers enqueue names and schedule an alarm. Both list queries add a
`key >= prefix` index lower bound without changing filtering or ordering.
