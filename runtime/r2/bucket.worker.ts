// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
import { Buffer } from "node:buffer";
import { all, get, base64Decode, base64Encode, maybeApply, readPrefix, discard, createTypedSql } from "./shared";
import type { TypedSql } from "./sql.worker";
import { R2Headers, R2Limits, MULTIPART_UPLOAD_TTL_MS } from "./constants";
import { R2Error, InternalError, InvalidMetadata, NoSuchKey, PreconditionFailed, EntityTooLarge } from "./errors.worker";
import { InternalR2Object, InternalR2ObjectBody } from "./r2Object.worker";
import { R2BindingRequestSchema, SQL_SCHEMA, MultipartUploadState } from "./schemas.worker";
import type { ObjectRow, R2Conditional, InternalR2PutOptions, InternalR2GetOptions, InternalR2ListOptions, R2Range, InternalR2CreateMultipartUploadOptions, R2PublishedPart } from "./schemas.worker";
import type { InternalR2Objects } from "./r2Object.worker";
import { Validator } from "./validator.worker";
import { BlobStore } from "./blobs";
import { uploadChunks, readChunks } from "./chunks";
import { multipartStmts, queueBodies, retireObject, generateId } from "./multipart";

const validate = new Validator();
const unsupported = (message: string) => new R2Error(400, `Not supported: ${message}`, 10029);
const version = () => crypto.randomUUID().replaceAll("-", "");
function parseMetadata(raw: any) {
 if (typeof raw.method !== "string") throw new InvalidMetadata();
 if (!["head","get","put","delete","list","createMultipartUpload","uploadPart","completeMultipartUpload","abortMultipartUpload"].includes(raw.method)) throw unsupported(`R2 method ${raw.method}`);
 if (raw.storageClass !== undefined && raw.storageClass !== "Standard") throw unsupported("storageClass other than Standard");
 if (raw.ssecKey !== undefined || raw.ssec !== undefined || raw.encryption !== undefined) throw unsupported("ssecKey");
 return R2BindingRequestSchema.parse(raw);
}
async function decodeMetadata(req: Request) {
 const size = Number(req.headers.get(R2Headers.METADATA_SIZE));
 if (!Number.isSafeInteger(size) || size <= 0 || size > 1024 * 1024 || !req.body) throw new InvalidMetadata();
 const [prefix, value] = await readPrefix(req.body, size);
 try {
  return {metadata: parseMetadata(JSON.parse(new TextDecoder().decode(prefix))), metadataSize: size, value};
 } catch(error) {await discard(value).catch(() => {});throw error;}
}
function encodeResult(result: InternalR2Object | InternalR2Objects) {
 const encoded = result instanceof InternalR2Object ? result.encode() : InternalR2Object.encodeMultiple(result);
 return new Response(encoded.value, {headers:{[R2Headers.METADATA_SIZE]: String(encoded.metadataSize), "Content-Type":"application/json"}});
}
export function prefixSuccessor(prefix: string): string | null {
 const points = Array.from(prefix, ch => ch.codePointAt(0)!);
 while (points.length) {
  const last = points.pop()!;
  if (last === 0x10ffff) continue;
  points.push(last === 0xd7ff ? 0xe000 : last + 1);
  return String.fromCodePoint(...points);
 }
 return null;
}
function encodeJSONResult(result: unknown) {
 const encoded = JSON.stringify(result);
 return new Response(encoded, {headers:{[R2Headers.METADATA_SIZE]:String(Buffer.byteLength(encoded)), "Content-Type":"application/json"}});
}
function sqlStmts(db: TypedSql) {
	const stmtGetPreviousByKey = db.stmt<
		Pick<ObjectRow, "key">,
		Pick<ObjectRow, "blob_id" | "etag" | "uploaded" | "chunk_size" | "chunk_count">
	>("SELECT blob_id, etag, uploaded, chunk_size, chunk_count FROM _mf_objects WHERE key = :key");
	// Regular statements
	const stmtGetByKey = db.stmt<Pick<ObjectRow, "key">, ObjectRow>(`
    SELECT key, blob_id, chunk_size, chunk_count, version, size, etag, uploaded, checksums, http_metadata, custom_metadata
    FROM _mf_objects WHERE key = :key
  `);
	const stmtPut = db.stmt<ObjectRow>(`
    INSERT OR REPLACE INTO _mf_objects (key, blob_id, chunk_size, chunk_count, version, size, etag, uploaded, checksums, http_metadata, custom_metadata)
    VALUES (:key, :blob_id, :chunk_size, :chunk_count, :version, :size, :etag, :uploaded, :checksums, :http_metadata, :custom_metadata)
  `);
	const stmtDelete = db.stmt<
		Pick<ObjectRow, "key">,
		Pick<ObjectRow, "blob_id" | "chunk_size" | "chunk_count">
	>("DELETE FROM _mf_objects WHERE key = :key RETURNING blob_id, chunk_size, chunk_count");

	function stmtListWithoutDelimiter<ExtraColumns extends (keyof ObjectRow)[]>(
		bounded: boolean, ...extraColumns: ExtraColumns
	) {
		const columns: (keyof ObjectRow)[] = [
			"key",
			"version",
			"size",
			"etag",
			"uploaded",
			"checksums",
			...extraColumns,
		];
		// TODO: consider applying same `:start_after IS NULL` trick to KeyValueStore
		return db.stmt<
			{ limit: number; prefix: string; start_after: string | null; prefix_end?: string },
			Omit<ObjectRow, "blob_id"> & Pick<ObjectRow, ExtraColumns[number]>
		>(`
      SELECT ${columns.join(", ")}
      FROM _mf_objects
      WHERE key >= :prefix ${bounded ? "AND key < :prefix_end" : ""} AND substr(key, 1, length(:prefix)) = :prefix
      AND (:start_after IS NULL OR key > :start_after)
      ORDER BY key LIMIT :limit
    `);
	}

 return {
  getByKey: stmtGetByKey,
  put: db.txn((row: ObjectRow, onlyIf?: R2Conditional) => {
   const previous = get(stmtGetPreviousByKey({key: row.key}));
   validate.condition(previous, onlyIf);
   stmtPut(row);
   retireObject(db, row.key, previous);
  }),
  deleteByKeys: db.txn((keys: string[]) => {
   for (const key of keys) {
    const old = get(stmtDelete({key}));
    retireObject(db, key, old);
   }
  }),

		listWithoutDelimiter: [stmtListWithoutDelimiter(false), stmtListWithoutDelimiter(true)],
		listHttpMetadataWithoutDelimiter: [stmtListWithoutDelimiter(false,"http_metadata"), stmtListWithoutDelimiter(true,"http_metadata")],
		listCustomMetadataWithoutDelimiter:
			[stmtListWithoutDelimiter(false,"custom_metadata"), stmtListWithoutDelimiter(true,"custom_metadata")],
		listHttpCustomMetadataWithoutDelimiter: [stmtListWithoutDelimiter(false,"http_metadata","custom_metadata"), stmtListWithoutDelimiter(true,"http_metadata","custom_metadata")],
		listMetadata: [false, true].map(bounded => db.stmt<
			{
				limit: number;
				start_after: string | null;
				prefix: string;
				prefix_end?: string;
				delimiter: string;
			},
			Omit<ObjectRow, "key" | "blob_id"> & {
				last_key: string;
				delimited_prefix_or_key: `dlp:${string}` | `key:${string}`;
			}
		>(`
      SELECT
        -- When grouping by a delimited prefix, this will give us the last key with that prefix.
        --   NOTE: we'll use this for the next cursor. If we didn't return the last key, the next page may return the
        --   same delimited prefix. Essentially, we're skipping over all keys with this group's delimited prefix.
        -- When grouping by a key, this will just give us the key.
        max(key) AS last_key,
        iif(
            -- Try get 1-indexed position \`i\` of :delimiter in rest of key after :prefix...
                                                       instr(substr(key, length(:prefix) + 1), :delimiter),
            -- ...if found, we have a delimited prefix of the :prefix followed by the rest of key up to and including the :delimiter
            'dlp:' || substr(key, 1, length(:prefix) + instr(substr(key, length(:prefix) + 1), :delimiter) + length(:delimiter) - 1),
            -- ...otherwise, we just have a regular key
            'key:' || key
        ) AS delimited_prefix_or_key,
        -- NOTE: we'll ignore metadata for delimited prefix rows, so it doesn't matter which keys' we return
        version, size, etag, uploaded, checksums, http_metadata, custom_metadata
      FROM _mf_objects
      WHERE key >= :prefix ${bounded ? "AND key < :prefix_end" : ""} AND substr(key, 1, length(:prefix)) = :prefix
      AND (:start_after IS NULL OR key > :start_after)
      GROUP BY delimited_prefix_or_key -- Group keys with same delimited prefix into a row, leaving others in their own rows
      ORDER BY last_key LIMIT :limit;
    `)),

 };
}

/** A single namespace instance is a single bucket. Bodies are immutable BLOBS names. */
export class R2BucketObject {
 readonly #stmts: ReturnType<typeof sqlStmts>;
 readonly db: TypedSql;
 readonly blob: BlobStore;
 readonly #multipart: ReturnType<typeof multipartStmts>;
 readonly #uploadTtl: number;
 #gcRunning = false;
 constructor(readonly ctx: DurableObjectState, readonly env: {BLOBS: Fetcher; MINIFLARE_TEST_MULTIPART_UPLOAD_TTL_MS?: string}) {
  this.db = createTypedSql(ctx.storage);
  this.db.exec(SQL_SCHEMA);
  const columns = new Set(all(this.db.exec("PRAGMA table_info(_mf_objects)")).map(row => row.name));
  if (!columns.has("chunk_size")) this.db.exec("ALTER TABLE _mf_objects ADD COLUMN chunk_size INTEGER NOT NULL DEFAULT 0");
  if (!columns.has("chunk_count")) this.db.exec("ALTER TABLE _mf_objects ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 1");
  this.db.exec("CREATE TABLE IF NOT EXISTS _r2_pending_gc (blob_id TEXT PRIMARY KEY NOT NULL, next_retry_at INTEGER NOT NULL DEFAULT 0, retry_ms INTEGER NOT NULL DEFAULT 1000)");
  const gcColumns = new Set(all(this.db.exec("PRAGMA table_info(_r2_pending_gc)")).map(row => row.name));
  if (!gcColumns.has("next_retry_at")) this.db.exec("ALTER TABLE _r2_pending_gc ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0");
  if (!gcColumns.has("retry_ms")) this.db.exec("ALTER TABLE _r2_pending_gc ADD COLUMN retry_ms INTEGER NOT NULL DEFAULT 1000");
  this.db.exec("CREATE INDEX IF NOT EXISTS _r2_gc_due ON _r2_pending_gc(next_retry_at)");
  this.#stmts = sqlStmts(this.db);
  this.#multipart = multipartStmts(this.db);
  const ttl = Number(env.MINIFLARE_TEST_MULTIPART_UPLOAD_TTL_MS ?? MULTIPART_UPLOAD_TTL_MS);
  this.#uploadTtl = Number.isSafeInteger(ttl) && ttl > 0 ? ttl : MULTIPART_UPLOAD_TTL_MS;
  this.blob = new BlobStore(env.BLOBS);
  ctx.blockConcurrencyWhile(() => this.#scheduleGc());
 }
 async #scheduleGc() {
  if (this.#gcRunning) return;
  const gc = get(this.db.exec("SELECT min(next_retry_at) AS due FROM _r2_pending_gc"))!.due;
  const expiry = get(this.db.exec("SELECT min(expires_at) AS due FROM _mf_multipart_uploads"))!.due;
  const due = Math.min(gc === null ? Infinity : Number(gc),expiry === null ? Infinity : Number(expiry));
  if (Number.isFinite(due)) {
   const when = Math.max(Date.now()+1,due);
   const alarm = await this.ctx.storage.getAlarm();
   if (alarm === null || alarm > when || alarm < Date.now()) await this.ctx.storage.setAlarm(when);
  }
 }
 async alarm() {
  this.#gcRunning = true;
  try {
   const now = Date.now();
   // Terminal upload records may disappear while completed object parts remain.
   // Expire after cleanup so an expired upload first transitions to ABORTED.
   this.ctx.storage.transactionSync(() => {
    this.db.exec("DELETE FROM _mf_multipart_uploads WHERE expires_at <= ? AND state != ?",now,MultipartUploadState.IN_PROGRESS);
    const expired = all(this.db.exec<{upload_id:string}>("SELECT upload_id FROM _mf_multipart_uploads WHERE expires_at <= ? AND state = ?",now,MultipartUploadState.IN_PROGRESS));
    for (const {upload_id} of expired) {
     queueBodies(this.db, all(this.db.exec("DELETE FROM _mf_multipart_parts WHERE upload_id = ? RETURNING blob_id,chunk_size,chunk_count",upload_id)));
     this.db.exec("UPDATE _mf_multipart_uploads SET state = ? WHERE upload_id = ?",MultipartUploadState.ABORTED,upload_id);
    }
   });
   const rows = all(this.db.exec<{blob_id:string;retry_ms:number}>("SELECT blob_id,retry_ms FROM _r2_pending_gc WHERE next_retry_at <= ? ORDER BY next_retry_at,rowid LIMIT 64",now));
   if (!rows.length) return;
   await this.ctx.storage.sync(); // Retirement follows durable dereferencing.
   for (let i = 0; i < rows.length; i += 8) {
    const outcomes = await Promise.all(rows.slice(i,i+8).map(async row => {
     try {await this.blob.delete(row.blob_id);return {...row,ok:true};}
     catch {return {...row,ok:false};}
    }));
    this.ctx.storage.transactionSync(() => {
     for (const result of outcomes) {
      if (result.ok) {this.db.exec("DELETE FROM _r2_pending_gc WHERE blob_id = ?",result.blob_id);}
      else this.db.exec("UPDATE _r2_pending_gc SET next_retry_at = ?, retry_ms = ? WHERE blob_id = ?",Date.now()+result.retry_ms,Math.min(result.retry_ms*2,60000),result.blob_id);
     }
    });
   }
  } finally {this.#gcRunning = false;await this.#scheduleGc();}
 }
 async #head(key: string) {
  validate.key(key);
  const row = get(this.#stmts.getByKey({key}));
  if (!row) throw new NoSuchKey();
  return new InternalR2Object(row, {offset:0,length:row.size});
 }
 async #get(key: string, opts: InternalR2GetOptions) {
  validate.key(key);
  const row = get(this.#stmts.getByKey({key}));
  if (!row) throw new NoSuchKey();
  const full = {offset:0,length:row.size};
  try {validate.condition(row, opts.onlyIf);} catch (e) {
   if (e instanceof PreconditionFailed) e.attach(new InternalR2Object(row, full));
   throw e;
  }
  const range = validate.range(opts, row.size);
  const r2Range = range ? {offset:range.start,length:range.end-range.start+1} : full;
  const parts = row.blob_id === null ? this.#multipart.getPartsByKey(key) : [row];
  const body = await readChunks(this.blob,parts,r2Range);
  return new InternalR2ObjectBody(row, body, r2Range);
 }
 async #put(key: string, value: ReadableStream<Uint8Array>, size: number, opts: InternalR2PutOptions) {
  try {
   if (!Number.isSafeInteger(size) || size < 0) throw new InvalidMetadata();
   if (size > R2Limits.MAX_VALUE_SIZE) throw unsupported("PUT above 1 GiB");
   validate.key(key).size(size).metadataSize(opts.customMetadata);
  } catch(error) {await discard(value).catch(() => {});throw error;}
  const blobId = version();
  const attempted: string[] = [];
  let row: ObjectRow;
  try {
   const digests = await uploadChunks(this.blob,blobId,value,size,opts,attempted);
   const checksums = validate.hash(digests, opts);
   row = {key,blob_id:blobId,chunk_size:R2Limits.CHUNK_SIZE,chunk_count:Math.max(1,Math.ceil(size/R2Limits.CHUNK_SIZE)),version:version(),size,etag:digests.get("MD5")!.toString("hex"),uploaded:Date.now(),checksums:JSON.stringify(checksums),http_metadata:JSON.stringify(opts.httpMetadata ?? {}),custom_metadata:JSON.stringify(opts.customMetadata ?? {})};
   // The condition is deliberately checked after the body, inside the claiming transaction.
   this.#stmts.put(row, opts.onlyIf);
  } catch (error) {
   this.#queueAttempted(attempted);
   await this.#scheduleGc();
   throw error;
  }
  await this.#scheduleGc();
  return new InternalR2Object(row);
 }
 #queueAttempted(names: string[]) {
  this.ctx.storage.transactionSync(() => {for (const name of names) this.db.exec("INSERT OR IGNORE INTO _r2_pending_gc (blob_id) VALUES (?)",name);});
 }
 async #createMultipartUpload(key: string, opts: InternalR2CreateMultipartUploadOptions) {
  validate.key(key);
  const uploadId = generateId();
  const now = Date.now();
  this.#multipart.createMultipartUpload({key,upload_id:uploadId,http_metadata:JSON.stringify(opts.httpMetadata ?? {}),custom_metadata:JSON.stringify(opts.customMetadata ?? {}),created_at:now,expires_at:now+this.#uploadTtl});
  await this.#scheduleGc();
  return {uploadId};
 }
 async #uploadPart(key: string, uploadId: string, partNumber: number, value: ReadableStream<Uint8Array>, size: number) {
  try {
   validate.key(key);
   if (!Number.isSafeInteger(size) || size < 0) throw new InvalidMetadata();
   if (size > R2Limits.MAX_PART_SIZE) throw new EntityTooLarge();
  } catch(error) {await discard(value).catch(() => {});throw error;}
  const blobId = version();
  const attempted: string[] = [];
  try {
   const digests = await uploadChunks(this.blob,blobId,value,size,{},attempted);
   const etag = generateId();
   // Check state and expiry only after streaming, in the claiming transaction.
   this.#multipart.putPart(key,{upload_id:uploadId,part_number:partNumber,blob_id:blobId,chunk_size:R2Limits.CHUNK_SIZE,chunk_count:Math.max(1,Math.ceil(size/R2Limits.CHUNK_SIZE)),size,etag,checksum_md5:digests.get("MD5")!.toString("hex")});
   await this.#scheduleGc();
   return {etag};
  } catch(error) {this.#queueAttempted(attempted);await this.#scheduleGc();throw error;}
 }
 async #completeMultipartUpload(key: string, uploadId: string, parts: R2PublishedPart[]) {
  validate.key(key);
  const row = this.#multipart.completeMultipartUpload(key,uploadId,parts,R2Limits.MIN_MULTIPART_PART_SIZE);
  await this.#scheduleGc();
  return new InternalR2Object(row);
 }
 async #abortMultipartUpload(key: string, uploadId: string) {
  validate.key(key);
  this.#multipart.abortMultipartUpload(key,uploadId);
  await this.#scheduleGc();
 }
 async #delete(keys: string | string[]) {
  if (!Array.isArray(keys)) keys = [keys];
  if (keys.length > 1000) throw unsupported("delete above 1000 keys");
  for (const key of keys) validate.key(key);
  this.#stmts.deleteByKeys(keys);
  await this.#scheduleGc();
 }

	#listWithoutDelimiterQuery(excludeHttp: boolean, excludeCustom: boolean) {
		if (excludeHttp && excludeCustom) {
			return this.#stmts.listWithoutDelimiter;
		}
		if (excludeHttp) {
			return this.#stmts.listCustomMetadataWithoutDelimiter;
		}
		if (excludeCustom) {
			return this.#stmts.listHttpMetadataWithoutDelimiter;
		}
		return this.#stmts.listHttpCustomMetadataWithoutDelimiter;
	}

	async #list(opts: InternalR2ListOptions): Promise<InternalR2Objects> {
		const prefix = opts.prefix ?? "";

		let limit = opts.limit ?? R2Limits.MAX_LIST_KEYS;
		validate.limit(limit);

		// If metadata is requested, R2 may return fewer than `limit` results to
		// accommodate it. Simulate this by limiting the limit to 100.
		// See https://developers.cloudflare.com/r2/api/workers/workers-api-reference/#r2listoptions.
		const include = opts.include ?? [];
		if (include.length > 0) {
			limit = Math.min(limit, 100);
		}
		const excludeHttp = !include.includes("httpMetadata");
		const excludeCustom = !include.includes("customMetadata");
		const rowObject = (
			row: Omit<ObjectRow, "blob_id" | "http_metadata" | "custom_metadata"> & {
				http_metadata?: string;
				custom_metadata?: string;
			}
		) => {
			if (row.http_metadata === undefined || excludeHttp) {
				row.http_metadata = "{}";
			}
			if (row.custom_metadata === undefined || excludeCustom) {
				row.custom_metadata = "{}";
			}
			return new InternalR2Object(row as Omit<ObjectRow, "blob_id">);
		};

		// If cursor set, and lexicographically after `startAfter`, use that for
		// `startAfter` instead
		let startAfter = opts.startAfter;
		if (opts.cursor !== undefined) {
			const cursorStartAfter = base64Decode(opts.cursor);
			if (startAfter === undefined || Buffer.compare(Buffer.from(cursorStartAfter), Buffer.from(startAfter)) > 0) {
				startAfter = cursorStartAfter;
			}
		}

		let delimiter = opts.delimiter;
		if (delimiter === "") {
			delimiter = undefined;
		}

		// Run appropriate query depending on options
		const prefixEnd = prefixSuccessor(prefix);
		const params = {
			prefix,
			...(prefixEnd === null ? {} : {prefix_end:prefixEnd}),
			start_after: startAfter ?? null,
			// Increase the queried limit by 1, if we return this many results, we
			// know there are more rows. We'll truncate to the original limit before
			// returning results.
			limit: limit + 1,
		};

		let objects: InternalR2Object[];
		const delimitedPrefixes: string[] = [];
		let nextCursorStartAfter: string | undefined;

		if (delimiter !== undefined) {
			const rows = all(this.#stmts.listMetadata[params.prefix_end === undefined ? 0 : 1]({ ...params, delimiter }));

			// If there are more results, we'll be returning a cursor
			const hasMoreRows = rows.length === limit + 1;
			rows.splice(limit, 1);

			objects = [];
			for (const row of rows) {
				if (row.delimited_prefix_or_key.startsWith("dlp:")) {
					delimitedPrefixes.push(row.delimited_prefix_or_key.substring(4));
				} else {
					objects.push(rowObject({ ...row, key: row.last_key }));
				}
			}

			if (hasMoreRows) {
				nextCursorStartAfter = rows[limit - 1].last_key;
			}
		} else {
			// If we don't have a delimiter, we can use a more efficient query
			const query = this.#listWithoutDelimiterQuery(excludeHttp, excludeCustom);
			const rows = all(query[params.prefix_end === undefined ? 0 : 1](params));

			// If there are more results, we'll be returning a cursor
			const hasMoreRows = rows.length === limit + 1;
			rows.splice(limit, 1);

			objects = rows.map(rowObject);

			if (hasMoreRows) {
				nextCursorStartAfter = rows[limit - 1].key;
			}
		}

		// The cursor encodes a key to start after rather than the key to start at
		// to ensure keys added between `list()` calls are returned.
		const nextCursor = maybeApply(base64Encode, nextCursorStartAfter);

		return {
			objects,
			truncated: nextCursor !== undefined,
			cursor: nextCursor,
			delimitedPrefixes,
		};
	}

 async fetch(req: Request) {
  try {
   if (req.method === "GET") {
    const header = req.headers.get(R2Headers.REQUEST);
    if (!header) throw new InvalidMetadata();
    const metadata = parseMetadata(JSON.parse(header));
    if (metadata.method === "head") return encodeResult(await this.#head(metadata.object));
    if (metadata.method === "get") return encodeResult(await this.#get(metadata.object, metadata));
    if (metadata.method === "list") return encodeResult(await this.#list(metadata));
   } else if (req.method === "PUT") {
    const {metadata,metadataSize,value} = await decodeMetadata(req);
    if (metadata.method === "put" || metadata.method === "uploadPart") {
     const length = req.headers.get("Content-Length");
     if (length === null) {await discard(value).catch(() => {});throw new InvalidMetadata();}
     const size = Number(length)-metadataSize;
     const operation = metadata.method === "put" ? this.#put(metadata.object,value,size,metadata) : this.#uploadPart(metadata.object,metadata.uploadId,metadata.partNumber,value,size);
     this.ctx.waitUntil(operation.then(() => {},() => {}));
     const result = await operation;
     return metadata.method === "put" ? encodeResult(result as InternalR2Object) : encodeJSONResult(result);
    }
    if (metadata.method === "createMultipartUpload") return encodeJSONResult(await this.#createMultipartUpload(metadata.object,metadata));
    if (metadata.method === "completeMultipartUpload") return encodeResult(await this.#completeMultipartUpload(metadata.object,metadata.uploadId,metadata.parts));
    if (metadata.method === "abortMultipartUpload") {await this.#abortMultipartUpload(metadata.object,metadata.uploadId);return new Response(null);}
    if (metadata.method === "delete") {
     await this.#delete("object" in metadata ? metadata.object : metadata.objects);
     return new Response(null);
    }
   }
   throw unsupported(`HTTP ${req.method}`);
  } catch (error) {
   if (error instanceof R2Error) return error.toResponse();
   console.error("R2 internal operation failed", error instanceof Error ? error.message : String(error));
   return new InternalError().toResponse();
  }
 }
}
