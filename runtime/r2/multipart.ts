// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Multipart transactions adapted from workers-sdk 264fb440; see UPSTREAM.md.
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { all, get } from "./shared";
import type { TypedSql } from "./sql.worker";
import { BadUpload, EntityTooLarge, EntityTooSmall, InternalError, InvalidPart, NoSuchUpload } from "./errors.worker";
import { MultipartUploadState } from "./schemas.worker";
import type { MultipartPartRow, MultipartUploadRow, ObjectRow, R2PublishedPart } from "./schemas.worker";
import { R2Limits } from "./constants";
import { bodyNames, type BodyLayout } from "./chunks";

export function generateId() {
 return Buffer.from(crypto.getRandomValues(new Uint8Array(128))).toString("base64url");
}
function generateVersion() {return crypto.randomUUID().replaceAll("-", "");}
function generateMultipartEtag(md5Hexes: string[]) {
 const hash = createHash("md5");
 for (const md5Hex of md5Hexes) hash.update(md5Hex, "hex");
 return `${hash.digest("hex")}-${md5Hexes.length}`;
}
export function queueBodies(db: TypedSql, rows: BodyLayout[]) {
 for (const row of rows) for (const name of bodyNames(row))
  db.exec("INSERT OR IGNORE INTO _r2_pending_gc (blob_id) VALUES (?)", name);
}
export function retireObject(db: TypedSql, key: string, row?: BodyLayout) {
 if (!row) return;
 if (row.blob_id !== null) queueBodies(db, [row]);
 else queueBodies(db, all(db.exec<MultipartPartRow>("DELETE FROM _mf_multipart_parts WHERE object_key = ? RETURNING *", key)));
}
export function multipartStmts(db: TypedSql) {
 const stmtGetPreviousByKey = db.stmt<{key:string},ObjectRow>("SELECT * FROM _mf_objects WHERE key = :key");
 const stmtPut = db.stmt<ObjectRow>(`INSERT OR REPLACE INTO _mf_objects
 (key,blob_id,chunk_size,chunk_count,version,size,etag,uploaded,checksums,http_metadata,custom_metadata)
 VALUES (:key,:blob_id,:chunk_size,:chunk_count,:version,:size,:etag,:uploaded,:checksums,:http_metadata,:custom_metadata)`);
	const stmtGetUploadState = db.stmt<
		Pick<MultipartUploadRow, "upload_id" | "key">,
		Pick<MultipartUploadRow, "state" | "expires_at">
	>(
		// For checking current upload state
		"SELECT state, expires_at FROM _mf_multipart_uploads WHERE upload_id = :upload_id AND key = :key"
	);
	const stmtGetUploadMetadata = db.stmt<
		Pick<MultipartUploadRow, "upload_id" | "key">,
		Pick<MultipartUploadRow, "http_metadata" | "custom_metadata" | "state" | "expires_at">
	>(
		// For checking current upload state, and getting metadata for completion
		"SELECT http_metadata, custom_metadata, state, expires_at FROM _mf_multipart_uploads WHERE upload_id = :upload_id AND key = :key"
	);
	const stmtUpdateUploadState = db.stmt<
		Pick<MultipartUploadRow, "upload_id" | "state">
	>(
		// For completing/aborting uploads
		"UPDATE _mf_multipart_uploads SET state = :state WHERE upload_id = :upload_id"
	);
	// Multipart part statements
	const stmtGetPreviousPartByNumber = db.stmt<
		Pick<MultipartPartRow, "upload_id" | "part_number">,
		Pick<MultipartPartRow, "blob_id" | "chunk_size" | "chunk_count">
	>(
		// For getting part number's previous blob ID to garbage collect
		"SELECT blob_id, chunk_size, chunk_count FROM _mf_multipart_parts WHERE upload_id = :upload_id AND part_number = :part_number"
	);
	const stmtPutPart = db.stmt<Omit<MultipartPartRow, "object_key">>(
		// For recording metadata when uploading parts
		`INSERT OR REPLACE INTO _mf_multipart_parts (upload_id, part_number, blob_id, chunk_size, chunk_count, size, etag, checksum_md5)
    VALUES (:upload_id, :part_number, :blob_id, :chunk_size, :chunk_count, :size, :etag, :checksum_md5)`
	);
	const stmtLinkPart = db.stmt<
		Pick<MultipartPartRow, "upload_id" | "part_number" | "object_key">
	>(
		// For linking parts with an object when completing uploads
		`UPDATE _mf_multipart_parts SET object_key = :object_key
    WHERE upload_id = :upload_id AND part_number = :part_number`
	);
	const stmtDeletePartsByUploadId = db.stmt<
		Pick<MultipartPartRow, "upload_id">,
		Pick<MultipartPartRow, "blob_id" | "chunk_size" | "chunk_count">
	>(
		// For deleting parts when aborting uploads
		"DELETE FROM _mf_multipart_parts WHERE upload_id = :upload_id RETURNING blob_id, chunk_size, chunk_count"
	);
	const stmtDeleteUnlinkedPartsByUploadId = db.stmt<
		Pick<MultipartPartRow, "upload_id">,
		Pick<MultipartPartRow, "blob_id" | "chunk_size" | "chunk_count">
	>(
		// For deleting unused parts when completing uploads
		"DELETE FROM _mf_multipart_parts WHERE upload_id = :upload_id AND object_key IS NULL RETURNING blob_id, chunk_size, chunk_count"
	);
	const stmtDeletePartsByKey = db.stmt<
		Pick<MultipartPartRow, "object_key">,
		Pick<MultipartPartRow, "blob_id" | "chunk_size" | "chunk_count">
	>(
		// For deleting dangling parts when overwriting an existing key
		"DELETE FROM _mf_multipart_parts WHERE object_key = :object_key RETURNING blob_id, chunk_size, chunk_count"
	);
	const stmtListPartsByUploadId = db.stmt<
		Pick<MultipartPartRow, "upload_id">,
		MultipartPartRow
	>(
		// For getting part metadata when completing uploads
		`SELECT upload_id, part_number, blob_id, chunk_size, chunk_count, size, etag, checksum_md5, object_key
    FROM _mf_multipart_parts WHERE upload_id = :upload_id`
	);
	const stmtListPartsByKey = db.stmt<
		Pick<MultipartPartRow, "object_key">,
		Pick<MultipartPartRow, "blob_id" | "size" | "chunk_size" | "chunk_count">
	>(
		// For getting part metadata when getting values, size included for range
		// requests, so we only need to read blobs containing the required data
		"SELECT blob_id, size, chunk_size, chunk_count FROM _mf_multipart_parts WHERE object_key = :object_key ORDER BY part_number"
	);

 return {
  getPartsByKey: (key:string) => all(stmtListPartsByKey({object_key:key})),
		createMultipartUpload: db.stmt<Omit<MultipartUploadRow, "state">>(`
      INSERT INTO _mf_multipart_uploads (upload_id, key, http_metadata, custom_metadata, created_at, expires_at)
      VALUES (:upload_id, :key, :http_metadata, :custom_metadata, :created_at, :expires_at)
    `),
		putPart: db.txn(
			(key: string, newRow: Omit<MultipartPartRow, "object_key">) => {
				// 1. Check the upload exists and is in-progress
				const uploadRow = get(
					stmtGetUploadState({
						key,
						upload_id: newRow.upload_id,
					})
				);
				if (uploadRow?.state !== MultipartUploadState.IN_PROGRESS || uploadRow.expires_at <= Date.now()) {
					throw new NoSuchUpload();
				}

				// 2. Check if we have an existing part with this number, then upsert
				const partRow = get(
					stmtGetPreviousPartByNumber({
						upload_id: newRow.upload_id,
						part_number: newRow.part_number,
					})
				);
				stmtPutPart(newRow);
				if (partRow) queueBodies(db, [partRow]);
			}
		),
		completeMultipartUpload: db.txn(
			(
				key: string,
				upload_id: string,
				selectedParts: R2PublishedPart[],
				minPartSize: number
			) => {
				// 1. Check the upload exists and is in-progress
				const uploadRow = get(stmtGetUploadMetadata({ key, upload_id }));
				if (uploadRow === undefined) {
					throw new InternalError();
				} else if (uploadRow.state > MultipartUploadState.IN_PROGRESS || uploadRow.expires_at <= Date.now()) {
					throw new NoSuchUpload();
				}

				// 2. Check all selected part numbers are unique
				const partNumberSet = new Set<number>();
				for (const { part } of selectedParts) {
					if (partNumberSet.has(part)) {
						throw new InternalError();
					}
					partNumberSet.add(part);
				}

				// 3. Get metadata for all uploaded parts, checking all selected parts
				//    exist
				const uploadedPartRows = stmtListPartsByUploadId({ upload_id });
				const uploadedParts = new Map<
					/* part number */ number,
					MultipartPartRow
				>();
				for (const row of uploadedPartRows) {
					uploadedParts.set(row.part_number, row);
				}
				const parts = selectedParts.map((selectedPart) => {
					// Try find matching uploaded part. If part couldn't be found, or
					// ETags don't match, throw.
					const uploadedPart = uploadedParts.get(selectedPart.part);
					// (if an uploaded part couldn't be found with the selected part
					// number, `uploadedPart?.etag` will be `undefined`, which will never
					// match `selectedPart.etag`, as we've validated it's a string)
					if (uploadedPart?.etag !== selectedPart.etag) {
						throw new InvalidPart();
					}
					return uploadedPart;
				});
				// `parts` now contains a list of selected parts' metadata.

				// 4. Check all but last part meets minimum size requirements. First
				//    check this in argument order, throwing a friendly error...
				for (const part of parts.slice(0, -1)) {
					if (part.size < minPartSize) {
						throw new EntityTooSmall();
					}
				}
				//    ...then check again in ascending part number order, throwing an
				//    internal error. We won't know where the current last element ends
				//    up in the sort, so we just check all parts again.
				//
				//    Also check that all but the last parts are the same size...
				parts.sort((a, b) => a.part_number - b.part_number);
				let partSize: number | undefined;
				for (const part of parts.slice(0, -1)) {
					// noinspection JSUnusedAssignment
					partSize ??= part.size;
					if (part.size < minPartSize || part.size !== partSize) {
						throw new BadUpload();
					}
				}
				//    ...and the last part is not greater than all others
				//    (if part size is defined, we must have at least one part)
				if (partSize !== undefined && parts[parts.length - 1].size > partSize) {
					throw new BadUpload();
				}

				const totalSize = parts.reduce((acc, { size }) => acc + size, 0);
				if (totalSize > R2Limits.MAX_VALUE_SIZE) throw new EntityTooLarge();

				// 5. Get existing upload if any, and delete previous multipart parts
				const oldBodies: BodyLayout[] = [];
				const existingRow = get(stmtGetPreviousByKey({ key }));
				const maybeOldBlobId = existingRow?.blob_id;
				if (maybeOldBlobId === null) {
					// If blob_id is null, this was a multipart object, so delete all
					// multipart parts
					const partRows = stmtDeletePartsByKey({ object_key: key });
					for (const partRow of partRows) {
						oldBodies.push(partRow);
					}
				} else if (maybeOldBlobId !== undefined) {
					oldBodies.push(existingRow!);
				}

				// 6. Write object to the database, and link parts with object
				const etag = generateMultipartEtag(
					parts.map(({ checksum_md5 }) => checksum_md5)
				);
				const newRow: ObjectRow = {
					key,
					blob_id: null,
					chunk_size: 0,
					chunk_count: 0,
					version: generateVersion(),
					size: totalSize,
					etag,
					uploaded: Date.now(),
					checksums: "{}",
					http_metadata: uploadRow.http_metadata,
					custom_metadata: uploadRow.custom_metadata,
				};
				stmtPut(newRow);
				for (const part of parts) {
					stmtLinkPart({
						upload_id,
						part_number: part.part_number,
						object_key: key,
					});
				}

				// 7. Delete unlinked, unused parts
				const partRows = stmtDeleteUnlinkedPartsByUploadId({ upload_id });
				for (const partRow of partRows) {
					oldBodies.push(partRow);
				}

				// 8. Mark the upload as completed
				stmtUpdateUploadState({
					upload_id,
					state: MultipartUploadState.COMPLETED,
				});

				queueBodies(db, oldBodies);
				return newRow;
			}
		),
		abortMultipartUpload: db.txn((key: string, upload_id: string) => {
			// 1. Make sure this multipart upload exists, ignoring finalised states
			const uploadRow = get(stmtGetUploadState({ key, upload_id }));
			if (uploadRow === undefined) {
				throw new InternalError();
			} else if (uploadRow.state > MultipartUploadState.IN_PROGRESS || uploadRow.expires_at <= Date.now()) {
				// If this upload has already been finalised, return here. `abort()` can
				// be called multiple times, and on already `complete()`ed uploads. In
				// the later case, we really don't want to delete pointed-to parts.
				return;
			}

			// 2. Delete all parts in the upload
			const partRows = all(stmtDeletePartsByUploadId({ upload_id }));
			queueBodies(db, partRows);

			// 3. Mark the uploaded as aborted
			stmtUpdateUploadState({
				upload_id,
				state: MultipartUploadState.ABORTED,
			});

			return;
		}),
 };
}
