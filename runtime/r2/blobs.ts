// SPDX-License-Identifier: Apache-2.0
import type { InclusiveRange } from "./range";
/** Only the node blob HTTP shape; target/auth/retention belong to the binding. */
export class BlobStore {
 constructor(readonly binding: Fetcher) {}
 #url(id: string) {return `http://blobs/${id}`;}
 async put(id: string, body: ReadableStream<Uint8Array>, size: number) {
  // Preserve known length across a TransformStream; BLOBS must never accept unknown length.
  const fixed = new FixedLengthStream(size);
  const abort = new AbortController();
  const pumping = body.pipeTo(fixed.writable, {signal:abort.signal});
  void pumping.catch(() => {});
  try {
   const response = await this.binding.fetch(this.#url(id),{method:"PUT",body:fixed.readable,headers:{"Content-Length":String(size)}});
   await response.arrayBuffer();
   if (!response.ok) throw new Error(`BLOBS PUT failed (${response.status})`);
   await pumping;
  } catch (error) {
   abort.abort(error);
   await pumping.catch(() => {});
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
