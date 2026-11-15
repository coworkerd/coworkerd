# R2 internal Worker

This is an isolated module; existing Coworkerd builds and runtime do not load it.
Each service/DO namespace is one bucket, serving the native workerd R2 HTTP
protocol. Tenant code uses the native `R2Bucket` API.

Build here with `npm ci` then `npm run build` (Node 22+). The distribution includes
`worker.mjs`, a source map and the Miniflare/Zod license notices. Enable
`nodejs_compat`, configure a SQLite-backed `R2BucketObject` namespace, bind it as
`BUCKET_OBJECT`, and bind `BLOBS` to an already-scoped trusted blob service. The
tenant's `r2Bucket` service designator targets this service. Tenants do not choose
bucket identities, blob targets, credentials or retirement positions.

Objects are at most 1 GiB. Bodies use sequential, immutable blobs of at most
8 MiB, named `<random-upload-id>/<zero-based-chunk-index>`. Empty objects have one
empty chunk. The index stores `blob_id` (upload id), `chunk_size`, `chunk_count`
and the original Miniflare metadata. Old r32 rows migrate with `chunk_size=0`,
retaining their exact single-blob name until overwritten/deleted.

PUT streams one chunk at a time, with known-length FixedLengthStreams and running
MD5/requested checksums. It retains a source packet and stream buffers rather
than the entire object. After all bytes and checksums are verified, one SQLite
transaction rechecks conditions, switches the index and queues every old chunk.
Failed/ambiguous chunk writes, input errors and failed claims queue all attempted
names. `ctx.waitUntil()` keeps that cleanup alive if the caller's producer aborts.
There is no pre-upload reservation. A process crash before a claim/cleanup can
still leave unknown orphan chunks; namespace-level orphan reclamation belongs
to platform integration.

Multipart uploads use the native create/uploadPart/complete/abort methods. Each
part is streamed by the same chunk uploader, at most 128 MiB per part. Parts store
random opaque ETags and separate MD5 digests. Completion validates Miniflare
part rules and a selected total of at most 1 GiB before atomically linking the
selected parts. The final ETag is MD5 of the concatenated binary part MD5s,
followed by the part count. Replaced, unused, aborted and failed-claim parts
queue every chunk for retirement in the same transaction. Completion has no
conditional option.

GET captures one indexed version and lazily reads only overlapping chunks, in
order, with per-chunk ranges. Missing, short or failed chunks error the stream;
a short object is never returned as a successful complete value. BLOBS retirement
must preserve old bytes long enough for readers of a replaced version.

Requests only enqueue retirement and persist an alarm; they never wait for blob
DELETE. Alarms process up to 64 eligible names per pass, at most 8 concurrently.
Successful acknowledgements remove rows transactionally. Each failed name keeps
its own earliest retry time and delay (1 second, doubling to 60 seconds). A name
in backoff does not hold up fresh work; when all names fail their individual
attempt intervals grow without an idle alarm loop. Retry state survives restarts.
Old r33 queue rows migrate in place; its old `_r2_gc_state` table, if present,
is no longer consulted.

Uploads expire seven days after creation. An alarm aborts expired in-progress
uploads, queues all their parts and removes terminal upload records when their
creation reaches the same seven-day limit. Completed object parts outlive their
upload record. The next alarm is the earliest upload expiry/terminal cleanup or
eligible retirement, so expiry neither waits for nor resets a name's GC backoff.
Opening a bucket schedules persisted work without a tenant request. The trusted
lab-only text binding `MINIFLARE_TEST_MULTIPART_UPLOAD_TTL_MS` can shorten the
lifetime; production wiring must omit it.

Both list queries seek between a prefix and its Unicode code-point successor.
Empty prefixes and prefixes with no successor omit the upper bound; filtering,
delimiter grouping and UTF-8 pagination ordering are unchanged.

`BLOBS.fetch()` supports known-length create-only PUT, whole/single-range GET and
idempotent retirement DELETE. The binding owns target prefix, authentication,
quotas, confirmed positions and physical-retention policy. Objects above 1 GiB,
parts above 128 MiB, non-Standard storage classes and SSE-C are explicitly rejected. Keys remain limited to 1024 UTF-8 bytes, PUT custom metadata to the
Miniflare 2048-byte rule, and batch delete to 1000 keys.

See [UPSTREAM.md](UPSTREAM.md) for source pin, license and adaptation details.
