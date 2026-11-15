// SPDX-License-Identifier: Apache-2.0
import type { InclusiveRange } from "./range";
/** Only the node blob HTTP shape; target/auth/retention belong to the binding. */
export class BlobStore {
 constructor(readonly binding: Fetcher) {}
 #url(id: string) {return `http://blobs/${id}`;}
 async put(id: string, body: ReadableStream<Uint8Array>, size: number) {
  // uploadChunks supplies a FixedLengthStream. Do not couple two native fixed streams.
  const abort = new AbortController();
  try {
   const response = await this.binding.fetch(this.#url(id),{method:"PUT",body,signal:abort.signal,headers:{"Content-Length":String(size)}});
   await response.arrayBuffer();
   if (!response.ok) throw new Error(`BLOBS PUT failed (${response.status})`);
  } catch(error) {
   abort.abort(error);
   await body.cancel(error).catch(() => {});
   throw error;
  }
 }
 async get(id: string, range: InclusiveRange | undefined, expectedSize: number) {
  const response = await this.binding.fetch(this.#url(id),{headers:range ? {Range:`bytes=${range.start}-${range.end}`} : {}});
  if (!response.ok || !response.body) throw new Error(`BLOBS GET failed (${response.status})`);
  if (range && response.status !== 206 && !(range.start === 0 && Number(response.headers.get("Content-Length")) === expectedSize)) throw new Error("BLOBS ignored range");
  if (Number(response.headers.get("Content-Length")) !== expectedSize) throw new Error("BLOBS length mismatch");
  return response.body;
 }
 async delete(id: string) {
  const response = await this.binding.fetch(this.#url(id),{method:"DELETE"});
  await response.arrayBuffer();
  if (!response.ok) throw new Error(`BLOBS DELETE failed (${response.status})`);
 }
}
