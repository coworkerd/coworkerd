# Chunked R2 internal Worker

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

GET captures one indexed version and lazily reads only overlapping chunks, in
order, with per-chunk ranges. Missing, short or failed chunks error the stream;
a short object is never returned as a successful complete value. BLOBS retirement
must preserve old bytes long enough for readers of a replaced version.

Requests only enqueue retirement and persist an alarm; they never wait for blob
DELETE. Alarms process up to 64 names per pass, at most 8 concurrently. Successful
DELETE acknowledgements remove rows transactionally. Failures stop the batch and
back off from 1 second, doubling to 60 seconds; retry state survives restarts.
Remaining successful batches schedule another immediate alarm. Opening a bucket
schedules pending work without performing retirement in that request.

`BLOBS.fetch()` supports known-length create-only PUT, whole/single-range GET and
idempotent retirement DELETE. The binding owns target prefix, authentication,
quotas, confirmed positions and physical-retention policy. Multipart methods,
objects above 1 GiB, non-Standard storage classes and SSE-C are explicitly
unsupported. Keys remain limited to 1024 UTF-8 bytes, custom metadata to the
Miniflare 2048-byte rule, and batch delete to 1000 keys.

See [UPSTREAM.md](UPSTREAM.md) for source pin, license and adaptation details.
