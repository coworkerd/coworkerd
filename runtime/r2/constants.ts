// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
export const R2Limits = { MAX_LIST_KEYS: 1000, MAX_KEY_SIZE: 1024, MAX_VALUE_SIZE: 1024 * 1024 * 1024, CHUNK_SIZE: 8 * 1024 * 1024, MAX_METADATA_SIZE: 2048, MAX_PART_SIZE: 128 * 1024 * 1024, MIN_MULTIPART_PART_SIZE: 5 * 1024 * 1024 } as const;
export const R2Headers = { ERROR: "cf-r2-error", REQUEST: "cf-r2-request", METADATA_SIZE: "cf-r2-metadata-size" } as const;

/** Upload lifetime, measured from creation, including terminal record retention. */
export const MULTIPART_UPLOAD_TTL_MS = 7 * 24 * 60 * 60 * 1000;
