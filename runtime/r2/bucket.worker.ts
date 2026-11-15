// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
import assert from "node:assert";
import { Buffer } from "node:buffer";
import { all, get, base64Decode, base64Encode, maybeApply, readPrefix, discard, createTypedSql } from "./shared";
import type { TypedSql } from "./sql.worker";
import { R2Headers, R2Limits } from "./constants";
import { R2Error, InternalError, InvalidMetadata, NoSuchKey, PreconditionFailed } from "./errors.worker";
import { InternalR2Object, InternalR2ObjectBody } from "./r2Object.worker";
import { R2BindingRequestSchema, SQL_SCHEMA } from "./schemas.worker";
import type { ObjectRow, R2Conditional, InternalR2PutOptions, InternalR2GetOptions, InternalR2ListOptions, R2Range } from "./schemas.worker";
import type { InternalR2Objects } from "./r2Object.worker";
import { Validator, R2_HASH_ALGORITHMS } from "./validator.worker";
import { BlobStore } from "./blobs";

const validate = new Validator();
const unsupported = (message: string) => new R2Error(400, `Not supported: ${message}`, 10029);
const version = () => crypto.randomUUID().replaceAll("-", "");
function parseMetadata(raw: any) {
 if (typeof raw.method !== "string") throw new InvalidMetadata();
 if (!["head","get","put","delete","list"].includes(raw.method)) throw unsupported(`R2 method ${raw.method}`);
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
function sqlStmts(db: TypedSql) {
	const stmtGetPreviousByKey = db.stmt<
		Pick<ObjectRow, "key">,
		Pick<ObjectRow, "blob_id" | "etag" | "uploaded">
	>("SELECT blob_id, etag, uploaded FROM _mf_objects WHERE key = :key");
	// Regular statements
	const stmtGetByKey = db.stmt<Pick<ObjectRow, "key">, ObjectRow>(`
    SELECT key, blob_id, version, size, etag, uploaded, checksums, http_metadata, custom_metadata
    FROM _mf_objects WHERE key = :key
  `);
	const stmtPut = db.stmt<ObjectRow>(`
    INSERT OR REPLACE INTO _mf_objects (key, blob_id, version, size, etag, uploaded, checksums, http_metadata, custom_metadata)
    VALUES (:key, :blob_id, :version, :size, :etag, :uploaded, :checksums, :http_metadata, :custom_metadata)
  `);
	const stmtDelete = db.stmt<
		Pick<ObjectRow, "key">,
		Pick<ObjectRow, "blob_id">
	>("DELETE FROM _mf_objects WHERE key = :key RETURNING blob_id");

	function stmtListWithoutDelimiter<ExtraColumns extends (keyof ObjectRow)[]>(
		...extraColumns: ExtraColumns
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
			{ limit: number; prefix: string; start_after: string | null },
			Omit<ObjectRow, "blob_id"> & Pick<ObjectRow, ExtraColumns[number]>
		>(`
      SELECT ${columns.join(", ")}
      FROM _mf_objects
      WHERE substr(key, 1, length(:prefix)) = :prefix
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
   if (previous?.blob_id) db.exec("INSERT OR IGNORE INTO _r2_pending_gc (blob_id) VALUES (?)", previous.blob_id);
  }),
  deleteByKeys: db.txn((keys: string[]) => {
   for (const key of keys) {
    const old = get(stmtDelete({key}));
    if (old?.blob_id) db.exec("INSERT OR IGNORE INTO _r2_pending_gc (blob_id) VALUES (?)", old.blob_id);
   }
  }),

		listWithoutDelimiter: stmtListWithoutDelimiter(),
		listHttpMetadataWithoutDelimiter: stmtListWithoutDelimiter("http_metadata"),
		listCustomMetadataWithoutDelimiter:
			stmtListWithoutDelimiter("custom_metadata"),
		listHttpCustomMetadataWithoutDelimiter: stmtListWithoutDelimiter(
			"http_metadata",
			"custom_metadata"
		),
		listMetadata: db.stmt<
			{
				limit: number;
				start_after: string | null;
				prefix: string;
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
      WHERE substr(key, 1, length(:prefix)) = :prefix
      AND (:start_after IS NULL OR key > :start_after)
      GROUP BY delimited_prefix_or_key -- Group keys with same delimited prefix into a row, leaving others in their own rows
      ORDER BY last_key LIMIT :limit;
    `),

 };
}

/** A single namespace instance is a single bucket. Bodies are immutable BLOBS names. */
export class R2BucketObject {
 readonly #stmts: ReturnType<typeof sqlStmts>;
 readonly db: TypedSql;
 readonly blob: BlobStore;
 #gc?: Promise<void>;
 constructor(readonly ctx: DurableObjectState, readonly env: {BLOBS: Fetcher}) {
  this.db = createTypedSql(ctx.storage);
  this.db.exec(SQL_SCHEMA);
  this.db.exec("CREATE TABLE IF NOT EXISTS _r2_pending_gc (blob_id TEXT PRIMARY KEY NOT NULL)");
  this.#stmts = sqlStmts(this.db);
  ctx.blockConcurrencyWhile(async () => {
   if (get(this.db.exec("SELECT blob_id FROM _r2_pending_gc LIMIT 1"))) await ctx.storage.setAlarm(Date.now());
  });
  this.blob = new BlobStore(env.BLOBS);
 }
 async alarm() { await this.#drainGc(); }
 async #drainGc() {
  if (this.#gc) return this.#gc;
  this.#gc = this.#runGc();
  try { await this.#gc; } finally { this.#gc = undefined; }
 }
 async #runGc() {
  // Persist retry intent before the first external DELETE. SQL/output durability uses the DO gate.
  const rows = all(this.db.exec<{blob_id: string}>("SELECT blob_id FROM _r2_pending_gc LIMIT 1024"));
  if (!rows.length) return;
  await this.ctx.storage.setAlarm(Date.now() + 1000);
  await this.ctx.storage.sync();
  for (const {blob_id} of rows) {
   try {
    await this.blob.delete(blob_id);
    this.db.exec("DELETE FROM _r2_pending_gc WHERE blob_id = ?", blob_id);
   } catch { /* Keep the durable row; alarm/open retries it. */ }
  }
  if (get(this.db.exec("SELECT blob_id FROM _r2_pending_gc LIMIT 1"))) await this.ctx.storage.setAlarm(Date.now() + 1000);
  else await this.ctx.storage.deleteAlarm();
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
  assert(row.blob_id !== null);
  const body = await this.blob.get(row.blob_id, range, r2Range.length);
  return new InternalR2ObjectBody(row, body, r2Range);
 }
 async #put(key: string, value: ReadableStream<Uint8Array>, size: number, opts: InternalR2PutOptions) {
  try {
   if (!Number.isSafeInteger(size) || size < 0) throw new InvalidMetadata();
   if (size > R2Limits.MAX_VALUE_SIZE) throw unsupported("PUT above 25 MiB (chunking is not implemented)");
   validate.key(key).size(size).metadataSize(opts.customMetadata);
  } catch(error) {await discard(value).catch(() => {});throw error;}
  const blobId = version();
  const algorithms = R2_HASH_ALGORITHMS.filter(({field}) => field === "md5" || opts[field] !== undefined);
  const digests = new Map();
  const hashes = algorithms.map(({name}) => new crypto.DigestStream(name));
  const writers = hashes.map(hash => hash.getWriter());
  const digesting = new TransformStream<Uint8Array,Uint8Array>({
   async transform(chunk, controller) {for (const writer of writers) await writer.write(chunk);controller.enqueue(chunk);},
   async flush() {for(let i=0;i<hashes.length;i++){await writers[i].close();digests.set(algorithms[i].name, Buffer.from(await hashes[i].digest));}}
  });
  let row: ObjectRow;
  try {
   await this.blob.put(blobId, value.pipeThrough(digesting), size);
   const checksums = validate.hash(digests, opts);
   row = {key,blob_id:blobId,version:version(),size,etag:digests.get("MD5").toString("hex"),uploaded:Date.now(),checksums:JSON.stringify(checksums),http_metadata:JSON.stringify(opts.httpMetadata ?? {}),custom_metadata:JSON.stringify(opts.customMetadata ?? {})};
   // The condition is deliberately checked after the body, inside the claiming transaction.
   this.#stmts.put(row, opts.onlyIf);
  } catch (error) {
   this.db.exec("INSERT OR IGNORE INTO _r2_pending_gc (blob_id) VALUES (?)", blobId);
   await this.#drainGc();
   throw error;
  }
  // Cleanup errors after a successful claim must never retire the current body.
  await this.#drainGc();
  return new InternalR2Object(row);
 }
 async #delete(keys: string | string[]) {
  if (!Array.isArray(keys)) keys = [keys];
  if (keys.length > 1000) throw unsupported("delete above 1000 keys");
  for (const key of keys) validate.key(key);
  this.#stmts.deleteByKeys(keys);
  await this.#drainGc();
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
		const params = {
			prefix,
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
			const rows = all(this.#stmts.listMetadata({ ...params, delimiter }));

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
			const rows = all(query(params));

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
   // Opening the bucket also retries a queue left by an interrupted prior instance.
   await this.#drainGc();
   if (req.method === "GET") {
    const header = req.headers.get(R2Headers.REQUEST);
    if (!header) throw new InvalidMetadata();
    const metadata = parseMetadata(JSON.parse(header));
    if (metadata.method === "head") return encodeResult(await this.#head(metadata.object));
    if (metadata.method === "get") return encodeResult(await this.#get(metadata.object, metadata));
    if (metadata.method === "list") return encodeResult(await this.#list(metadata));
   } else if (req.method === "PUT") {
    const {metadata,metadataSize,value} = await decodeMetadata(req);
    if (metadata.method === "put") {
     const length = req.headers.get("Content-Length");
     if (length === null) throw new InvalidMetadata();
     return encodeResult(await this.#put(metadata.object,value,Number(length)-metadataSize,metadata));
    }
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
