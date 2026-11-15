import { build } from "esbuild";
import { copyFile } from "node:fs/promises";
await build({entryPoints:[new URL("index.ts",import.meta.url).pathname],outfile:new URL("dist/worker.mjs",import.meta.url).pathname,bundle:true,format:"esm",platform:"neutral",target:"es2022",external:["node:*","cloudflare:*"],conditions:["workerd","worker","browser"],sourcemap:true,banner:{js:"/*! Miniflare R2 adaptation: workers-sdk 264fb4408b40f6847b6f0eaaac4c190bea46ecbd. MIT; see LICENSE-MIT and LICENSE-ZOD. */"}});
await copyFile(new URL("LICENSE-MIT",import.meta.url),new URL("dist/LICENSE-MIT",import.meta.url));
await copyFile(new URL("node_modules/zod/LICENSE",import.meta.url),new URL("dist/LICENSE-ZOD",import.meta.url));
await copyFile(new URL("UPSTREAM.md",import.meta.url),new URL("dist/UPSTREAM.md",import.meta.url));
