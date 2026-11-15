// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
import assert from "node:assert";

// TODO(soon): we control these types, so we should fix them to make them nice

export type TypedValue = ArrayBuffer | string | number | null;
export type TypedResult = Record<string, TypedValue>;

export function isTypedValue(value: unknown): value is TypedValue {
	return (
		value === null ||
		typeof value === "string" ||
		typeof value === "number" ||
		value instanceof ArrayBuffer
	);
}

export interface TypedSqlStorage {
	exec<R extends TypedResult = TypedResult>(
		query: string,
		...bindings: TypedValue[]
	): TypedSqlStorageCursor<R>;
	prepare<
		P extends TypedValue[] = TypedValue[],
		R extends TypedResult = TypedResult,
	>(
		query: string
	): TypedSqlStorageStatement<P, R>;
}
export interface TypedSqlStorageCursor<R extends TypedResult = TypedResult> {
	raw(): IterableIterator<R[keyof R][]>;
	[Symbol.iterator](): IterableIterator<R>;
	readonly columnNames: string[];
	readonly rowsRead: number;
	readonly rowsWritten: number;
}
export interface TypedSqlStorageStatement<
	P extends TypedValue[] = TypedValue[],
	R extends TypedResult = TypedResult,
> {
	(...bindings: P): TypedSqlStorageCursor<R>;
}

export type StatementFactory = <
	P extends Record<string, TypedValue>,
	R extends TypedResult = TypedResult,
>(
	query: string
) => (argsObject: P) => TypedSqlStorageCursor<R>;
function createStatementFactory(sql: TypedSqlStorage): StatementFactory {
	return <
		P extends Record<string, TypedValue>,
		R extends TypedResult = TypedResult,
	>(
		query: string
	) => {
		// Replace named parameters (e.g. :key) with numerics (e.g. ?1)
		const keyIndices = new Map<string, number>();
		query = query.replace(/[:@$]([a-z0-9_]+)/gi, (_, name: string) => {
			let index = keyIndices.get(name);
			if (index === undefined) {
				index = keyIndices.size;
				keyIndices.set(name, index);
			}
			return `?${index + 1}`; // SQLite's parameters are 1-indexed
		});
		// Official workerd exposes exec(), not Miniflare's prepare shim.
		const stmt = (...bindings: TypedValue[]) => sql.exec<R>(query, ...bindings);

		// Return function taking arguments object
		return (argsObject: P) => {
			// Convert arguments object to array
			const entries = Object.entries(argsObject);
			assert.strictEqual(
				entries.length,
				keyIndices.size,
				"Expected same number of keys in bindings and query"
			);
			const argsArray = new Array<TypedValue>(entries.length);
			for (const [key, value] of entries) {
				const index = keyIndices.get(key);
				assert(index !== undefined, `Unexpected binding key: ${key}`);
				argsArray[index] = value;
			}

			return stmt(...argsArray);
		};
	};
}

export type TransactionFactory = <P extends unknown[], R>(
	closure: (...args: P) => R
) => (...args: P) => R;
function createTransactionFactory(
	storage: DurableObjectStorage
): TransactionFactory {
	return <P extends unknown[], R>(closure: (...args: P) => R) =>
		(...args: P) =>
			storage.transactionSync(() => closure(...args));
}

export type TypedSql = TypedSqlStorage & {
	stmt: StatementFactory;
	txn: TransactionFactory;
};
export function createTypedSql(storage: DurableObjectStorage): TypedSql {
	const sql = storage.sql as unknown as TypedSql;
	sql.stmt = createStatementFactory(sql);
	sql.txn = createTransactionFactory(storage);
	return sql;
}

export function get<R extends TypedResult>(
	cursor: TypedSqlStorageCursor<R>
): R | undefined {
	let result: R | undefined;
	for (const row of cursor) {
		result ??= row;
	}
	return result;
}

export function all<R extends TypedResult>(
	cursor: TypedSqlStorageCursor<R>
): R[] {
	return Array.from(cursor);
}

export function drain<R extends TypedResult>(cursor: TypedSqlStorageCursor<R>) {
	for (const _ of cursor) {
	}
}
