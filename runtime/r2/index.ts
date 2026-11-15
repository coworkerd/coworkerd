// SPDX-License-Identifier: Apache-2.0
export { R2BucketObject } from "./bucket.worker";
export default {fetch(request: Request,env: {BUCKET_OBJECT: DurableObjectNamespace}) {
 // Wire a separate namespace/service per bucket. No tenant chooses this identity.
 return env.BUCKET_OBJECT.get(env.BUCKET_OBJECT.idFromName("bucket")).fetch(request);
}};
