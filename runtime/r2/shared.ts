// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
import assert from "node:assert";
import { Buffer } from "node:buffer";
export * from "./sql.worker";
export * from "./range";
export type ValueOf<T> = T[keyof T];
export class HttpError extends Error {constructor(readonly code: number, message: string){super(message);}}
// Miniflare shared/data.ts (base64 helpers) and shared/blob.worker.ts (readPrefix).
export const base64Encode = (value: string) => Buffer.from(value,"utf8").toString("base64");
export const base64Decode = (value: string) => Buffer.from(value,"base64").toString("utf8");
export const maybeApply = <T,R>(fn:(value:T)=>R,value:T|undefined) => value === undefined ? undefined : fn(value);
export async function readPrefix(stream: ReadableStream<Uint8Array>, prefixLength: number) {
 const reader = stream.getReader({mode:"byob"});
 const result = await reader.readAtLeast(prefixLength,new Uint8Array(prefixLength));
 assert(result.value !== undefined && result.value.byteLength === prefixLength);
 reader.releaseLock();
 return [result.value,stream.pipeThrough(new IdentityTransformStream())] as const;
}

// Native R2 uploads may still be pumping while an admission error is detected.
// Drain without retaining bytes so the protocol error survives upload completion.
export async function discard(stream: ReadableStream<Uint8Array>) {
 await stream.pipeTo(new WritableStream({write() {}}));
}
