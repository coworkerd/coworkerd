// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Cloudflare, Inc. <wrangler@cloudflare.com>
// Adapted from cloudflare/workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd
// packages/miniflare/src/workers/; see LICENSE-MIT and UPSTREAM.md.
export const R2Limits = { MAX_LIST_KEYS: 1000, MAX_KEY_SIZE: 1024, MAX_VALUE_SIZE: 25 * 1024 * 1024, MAX_METADATA_SIZE: 2048 } as const;
export const R2Headers = { ERROR: "cf-r2-error", REQUEST: "cf-r2-request", METADATA_SIZE: "cf-r2-metadata-size" } as const;
