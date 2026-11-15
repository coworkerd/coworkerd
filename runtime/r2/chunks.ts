// SPDX-License-Identifier: Apache-2.0
import { Buffer } from "node:buffer";
import { BlobStore } from "./blobs";
import { R2Limits } from "./constants";
import { R2_HASH_ALGORITHMS } from "./validator.worker";
import type { DigestAlgorithm } from "./validator.worker";
import type { ObjectRow, InternalR2PutOptions, R2Range } from "./schemas.worker";

type Layout = Pick<ObjectRow,"blob_id"|"chunk_size"|"chunk_count">;
export function bodyNames(row: Layout): string[] {
 if (!row.blob_id) return [];
 if (!row.chunk_size) return [row.blob_id]; // Existing r32 index, before migration.
 return Array.from({length:row.chunk_count!},(_,i) => `${row.blob_id}/${i}`);
}

/** Stream sequential known-length chunks; only a source packet and stream buffers are held. */
export async function uploadChunks(blob: BlobStore,id: string,body: ReadableStream<Uint8Array>,size: number,options: InternalR2PutOptions,attempted: string[]) {
 const algorithms = R2_HASH_ALGORITHMS.filter(({field}) => field === "md5" || options[field] !== undefined);
 const hashes = algorithms.map(({name}) => new crypto.DigestStream(name));
 const writers = hashes.map(hash => hash.getWriter());
 for (const hash of hashes) void hash.digest.catch(() => {});
 const reader = body.getReader({mode:"byob"});
 let packet = new Uint8Array(0);
 let readBuffer = new ArrayBuffer(64 * 1024);
 let consumed = 0;
 try {
  const count = Math.max(1,Math.ceil(size/R2Limits.CHUNK_SIZE));
  for (let index = 0; index < count; index++) {
   const length = Math.min(R2Limits.CHUNK_SIZE,size-consumed);
   const stream = new FixedLengthStream(length);
   const writer = stream.writable.getWriter();
   const name = `${id}/${index}`;
   
   attempted.push(name); // Also retire an ambiguous failed PUT, which may have stored its bytes.
   const pumping = (async () => {
    try {
     let remaining = length;
     while (remaining > 0) {
      if (packet.byteLength === 0) {
       const next = await reader.read(new Uint8Array(readBuffer));
       if (next.value) readBuffer = next.value.buffer;
       if (next.done) throw new Error("R2 upload length mismatch: early EOF");
       packet = next.value;
       if (!packet.byteLength) continue;
      }
      const take = Math.min(remaining,packet.byteLength);
      const piece = packet.subarray(0,take);
      for (const digest of writers) await digest.write(piece);
      await writer.write(piece);
      packet = packet.subarray(take);
      remaining -= take;
      consumed += take;
     }
     await writer.close();
    } catch(error) {
     await writer.abort(error).catch(() => {});
     throw error;
    } finally {writer.releaseLock();}
   })();
   void pumping.catch(() => {});
   try {
    await blob.put(name,stream.readable,length);
    await pumping;
   } catch(error) {
    await stream.readable.cancel(error).catch(() => {});
    await pumping.catch(() => {});
    throw error;
   }
  }
  if (packet.byteLength) throw new Error("R2 upload length mismatch: excess bytes");
  for (;;) {const next=await reader.read(new Uint8Array(readBuffer));if(next.value)readBuffer=next.value.buffer;if(next.done)break;if(next.value.byteLength)throw new Error("R2 upload length mismatch: excess bytes");}
  const result = new Map<DigestAlgorithm,Buffer>();
  for (let i=0;i<hashes.length;i++) {
   await writers[i].close();
   result.set(algorithms[i].name,Buffer.from(await hashes[i].digest));
  }
  return result;
 } catch(error) {
  await reader.cancel(error).catch(() => {});
  for (const writer of writers) await writer.abort(error).catch(() => {});
  throw error;
 } finally {
  reader.releaseLock();
  for (const writer of writers) writer.releaseLock();
 }
}

/** Byte-stream output lets native consumers supply buffers instead of allocating per packet. */
export async function readChunks(blob: BlobStore,row: ObjectRow,range: R2Range) {
 const offset = range.offset ?? 0;
 const length = range.length ?? row.size;
 const chunkSize = row.chunk_size || row.size || 1;
 const names = bodyNames(row);
 let index = Math.floor(offset/chunkSize);
 const last = length ? Math.floor((offset+length-1)/chunkSize) : index;
 let reader: ReadableStreamBYOBReader | undefined;
 let expected = 0;
 let received = 0;
 const open = async () => {
  const chunkLength=row.chunk_size ? Math.min(chunkSize,row.size-index*chunkSize) : row.size;
  const start=Math.max(0,offset-index*chunkSize);
  const end=Math.min(chunkLength,offset+length-index*chunkSize);
  expected=end-start;received=0;
  const body=await blob.get(names[index],start!==0 || end!==chunkLength ? {start,end:end-1} : undefined,expected);
  reader=body.getReader({mode:"byob"});
 };
 // Validate the first response before exposing metadata, including empty objects.
 await open();
 return new ReadableStream<Uint8Array>({
  type:"bytes",
  autoAllocateChunkSize:64 * 1024,
  async pull(controller: ReadableByteStreamController) {
   try {
    const request=controller.byobRequest!;
    while (index<=last) {
     if (!reader) await open();
     if (expected===0) {
      await reader.cancel();reader.releaseLock();reader=undefined;index++;continue;
     }
     const view=request.view!;
     const next=await reader.read(new Uint8Array(view.buffer,view.byteOffset,Math.min(view.byteLength,64*1024)));
     if (!next.value?.byteLength) throw new Error("BLOBS chunk ended early");
     received+=next.value.byteLength;
     if (received>expected) throw new Error("BLOBS chunk exceeded its indexed size");
     if (received===expected) {
      await reader.cancel();reader.releaseLock();reader=undefined;index++;
     }
     request.respondWithNewView(next.value);return;
    }
    controller.close();request.respond(0);
   } catch(error) {
    if(reader){await reader.cancel(error).catch(()=>{});reader.releaseLock();reader=undefined;}
    controller.error(error);
   }
  },
  async cancel(reason) {
   if(reader){await reader.cancel(reason).catch(()=>{});reader.releaseLock();reader=undefined;}
  }
 } as UnderlyingByteSource);
}
