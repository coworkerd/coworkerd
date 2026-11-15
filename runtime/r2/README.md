# Single-blob R2 internal Worker

This is an isolated module; existing Coworkerd builds and runtime do not load it.
It serves the workerd native R2 HTTP protocol. Each service/DO namespace represents
one bucket. Tenant code uses the native `R2Bucket`, not a JavaScript emulation.

Build here with `npm ci` then `npm run build` (Node 22+). The generated
`dist/worker.mjs` is an ES module with `R2BucketObject` and a default fetch entry.
Enable `nodejs_compat`, configure a SQLite-backed `R2BucketObject` namespace, bind
it as `BUCKET_OBJECT`, and bind `BLOBS` to an already-scoped blob service.
The tenant's `r2Bucket` service designator points at this module's service.
Do not let a tenant supply a namespace identity or arbitrary blob target.

`BLOBS.fetch()` uses relative random body names in HTTP paths. It must support
known-length, create-only PUT, GET with one byte range, and idempotent DELETE.
The binding owns the target prefix, authentication, quotas and delayed physical
retirement. DELETE retires a body; readers of an older indexed version must still
be able to open/read it during the blob service's retention period.

An object is at most 25 MiB. Multipart methods, larger values, non-Standard
storage classes and SSE-C are explicitly unsupported. Custom metadata is limited
to Miniflare's 2048-byte rule; keys to 1024 UTF-8 bytes; batch deletion to 1000.
The index is Miniflare's `_mf_objects` table. `_r2_pending_gc` holds immutable body
names waiting for DELETE acknowledgement; no body bytes are stored in SQLite.

PUT admits known length/key/metadata before uploading a fresh name. A synchronous
SQLite transaction rechecks the condition, publishes the new index row and queues
the previous body. Failed upload/digest/claim paths queue and delete the new name.
GC runs after commit, on subsequent requests, on open and via alarm. Only a
successful blob DELETE removes a queue row. At most 1024 entries are attempted
per pass; remaining/failed entries retain an alarm. No reservation is published
before uploading. A crash between successful body upload and the claim may leave
an unreferenced body without a queue record; namespace-level orphan reclamation
belongs to later platform integration, not an in-process reservation protocol.

GET captures one row before fetching its immutable body. Returned metadata and
range describe that row, even if a concurrent PUT replaces the key. A failed
upstream response fails GET; a failed body stream propagates to the native body
consumer instead of yielding a successful truncated value.

See [UPSTREAM.md](UPSTREAM.md) for source pin, license and adaptation details.
