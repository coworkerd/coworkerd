import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { constants, existsSync, readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { basename, delimiter, dirname, join, matchesGlob, relative, resolve, sep } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { unstable_readConfig, experimental_readRawConfig, unstable_defaultWranglerConfig } from "wrangler";
// The asset walk `wrangler dev` runs (the same .assetsignore rules and content-type table, ignore@5.3.1 and mime@4.0.7,
// as the upload of `wrangler deploy`).
import { buildAssetManifest } from "miniflare";

export const cli = fileURLToPath(new URL("./node_modules/wrangler/wrangler-dist/cli.js", import.meta.url));
const dependencies = fileURLToPath(new URL("./node_modules", import.meta.url));
const pinned = JSON.parse(readFileSync(new URL("./node_modules/wrangler/package.json", import.meta.url), "utf8"));
export const sha256 = value => createHash("sha256").update(value).digest("hex");
export function canonical(value) {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value !== null && typeof value === "object") return "{" + Object.keys(value).sort().map(k => JSON.stringify(k) + ":" + canonical(value[k])).join(",") + "}";
  return JSON.stringify(value);
}
const supported = new Set(["$schema", "name", "main", "compatibility_date", "compatibility_flags", "vars", "services",
  "build", "base_dir", "rules", "no_bundle", "find_additional_modules", "preserve_file_names", "minify", "tsconfig",
  "jsx_factory", "jsx_fragment", "alias", "define", "keep_names", "upload_source_maps", "send_metrics", "assets"]);
// Static assets: the directory, Cloudflare's not-found handling, the ASSETS binding (the Worker's own asset layer) and
// run_worker_first (true, or Cloudflare's path rules); html_handling only at its default.
const assetKeys = new Set(["directory", "html_handling", "not_found_handling", "binding", "run_worker_first"]);
const assetDefaults = { html_handling: "auto-trailing-slash" };
export const notFoundHandlings = ["none", "404-page", "single-page-application"];
// Cloudflare's own logging, placement and URL settings: accepted, ignored and reported; they do not change what the code does.
export const platform = {
  observability: "Cloudflare Workers Logs and Traces settings (Cloudflare's own logging)",
  placement: "Cloudflare Smart Placement (where Cloudflare runs the Worker)",
  workers_dev: "the workers.dev route on Cloudflare (Coworkerd routes come from the deployment manifest)",
  preview_urls: "Cloudflare preview URLs (Coworkerd routes come from the deployment manifest)"
};
// Set by Wrangler's own configuration normalization; build tools copy them into the configuration they redirect to.
const computed = new Set(["configPath", "userConfigPath", "topLevelName"]);
// Wrangler 4.136.1's DEFAULT_MODULE_RULES (not exported), applied after the configuration's own rules, as `wrangler
// deploy` classifies additional modules. Taken here rather than from unstable_getMiniflareWorkerOptions, which also
// loads the project's .dev.vars (or .env) values: the adapter never reads a value.
const DEFAULT_MODULE_RULES = [
  { type: "Text", globs: ["**/*.txt", "**/*.html", "**/*.sql"] },
  { type: "Data", globs: ["**/*.bin"] },
  { type: "CompiledWasm", globs: ["**/*.wasm", "**/*.wasm?module"] }
];
// Secrets: names only. `.dev.vars.example` (Cloudflare's convention for listing a project's secrets next to its
// configuration) is read with the line grammar of dotenv 16.3.1, the parser `wrangler dev` reads .dev.vars with; only
// the keys are kept. Never read: .dev.vars and .env files, which hold local values.
const DOTENV_LINE = /(?:^|^)\s*(?:export\s+)?([\w.-]+)(?:\s*=\s*?|:\s+?)(\s*'(?:\\'|[^'])*'|\s*"(?:\\"|[^"])*"|\s*`(?:\\`|[^`])*`|[^#\r\n]+)?\s*(?:#.*)?(?:$|$)/mg;
export const SECRETS_EXAMPLE = ".dev.vars.example";
export function exampleSecretNames(text) {
  const names = [];
  for (const match of text.replace(/\r\n?/mg, "\n").matchAll(DOTENV_LINE)) if (!names.includes(match[1])) names.push(match[1]);
  return names;
}
// Names as the deployment checks them: JavaScript identifiers of ASCII letters, digits, '_' and '$'; at most 64.
const environmentName = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
// Local value files: kept out of the staged copy of a project (the build does not need them).
const localValues = name => (name === ".dev.vars" || name.startsWith(".dev.vars.") || name === ".env" || name.startsWith(".env.")) &&
  !name.endsWith(".example");
const blank = value => value === null || value === undefined ||
  (Array.isArray(value) ? value.length === 0 : typeof value === "object" && Object.values(value).every(blank));
// Compared as JSON, as Wrangler compares a field with its default: fields left undefined are absent.
const json = value => value === undefined ? undefined : canonical(JSON.parse(JSON.stringify(value)));
// Every key outside the supported set, as listed before any of them is ignored.
export function unsupportedKeys(raw) {
  const found = Object.keys(raw).filter(key => !supported.has(key));
  for (const [index, binding] of (raw.services ?? []).entries()) {
    for (const key of Object.keys(binding)) if (!["binding", "service", "entrypoint"].includes(key)) found.push(`services[${index}].${key}`);
  }
  for (const key of Object.keys(raw.build ?? {})) if (!["command", "cwd", "watch_dir"].includes(key)) found.push(`build.${key}`);
  for (const [key, value] of Object.entries(raw.assets ?? {})) {
    if (!assetKeys.has(key) || (key === "html_handling" && value !== undefined && value !== assetDefaults.html_handling)) found.push(`assets.${key}`);
  }
  return found.sort();
}
// Unsupported keys are rejected unless they carry no configuration of the Worker: Cloudflare platform settings, empty
// values (such as the empty binding lists a framework build writes) and, in a configuration reached through Wrangler's
// deploy redirect, the fields Wrangler's normalization computes or leaves at its own default.
export function classify(raw, { redirected = false } = {}) {
  const rejected = [], ignored = [];
  for (const key of unsupportedKeys(raw)) {
    const service = /^services\[(\d+)\]\.(.+)$/.exec(key), build = /^build\.(.+)$/.exec(key), asset = /^assets\.(.+)$/.exec(key);
    const topLevel = !service && !build && !asset;
    const value = service ? raw.services[Number(service[1])][service[2]] : build ? raw.build[build[1]] : asset ? raw.assets[asset[1]] : raw[key];
    const reason = topLevel && Object.hasOwn(platform, key) ? ["platform", platform[key]]
      : blank(value) ? ["empty", "empty value (null, [] or {}, or only empty values): carries no configuration"]
      : asset && Object.hasOwn(assetDefaults, asset[1]) && value === assetDefaults[asset[1]]
        ? ["default", "equals Wrangler's own default for static assets: carries no configuration"]
      : redirected && topLevel && computed.has(key) ? ["computed", "set by Wrangler's configuration normalization and copied into the redirected configuration by the build tool"]
      : redirected && topLevel && json(value) === json(unstable_defaultWranglerConfig[key])
        ? ["default", "equals Wrangler's own default (unstable_defaultWranglerConfig), written by the build tool"] : null;
    if (reason) ignored.push({ key, reason: reason[0], detail: reason[1], value });
    else rejected.push(key);
  }
  return { rejected, ignored };
}
export const unsupported = (raw, options) => classify(raw, options).rejected;
// Wrangler's deploy redirect: a build tool (e.g. the Cloudflare Vite plugin) writes .wrangler/deploy/config.json next to
// the user's configuration, naming the configuration to deploy; `wrangler deploy` run in that directory follows it.
function redirect(projectDir) {
  const file = join(projectDir, ".wrangler/deploy/config.json");
  if (!existsSync(file)) return null;
  const { rawConfig: content } = experimental_readRawConfig({ config: file });
  if (typeof content.configPath !== "string" || !content.configPath) throw new Error(`Wrangler deploy redirect without configPath: ${file}`);
  const target = resolve(dirname(file), content.configPath);
  if (!existsSync(target)) throw new Error(`Wrangler deploy redirect points to a missing configuration: ${file} -> ${target}`);
  return { file, content, target };
}
// The Wrangler that builds the modules. Esbuild names every bundled file by its path relative to the project, including
// the polyfills and templates Wrangler injects from its own installation, so that installation must sit at a fixed place
// relative to the project: the project's own Wrangler when it has one (the node_modules lookup `npx wrangler` makes),
// otherwise this adapter's pinned Wrangler, run on a copy of the project staged next to a copy of the adapter's dependencies.
function projectWrangler(projectDir) {
  for (let directory = projectDir; ; directory = dirname(directory)) {
    const manifest = join(directory, "node_modules", "wrangler", "package.json");
    if (basename(directory) !== "node_modules" && existsSync(manifest)) {
      const pkg = JSON.parse(readFileSync(manifest, "utf8"));
      const bin = typeof pkg.bin === "string" ? pkg.bin : pkg.bin?.wrangler;
      if (pkg.name === "wrangler" && bin) return { source: "project", version: pkg.version, packageDirectory: dirname(manifest), cli: resolve(dirname(manifest), bin) };
    }
    if (dirname(directory) === directory) return null;
  }
}
async function stage(projectDir, configPath, temporary) {
  if (relative(projectDir, configPath).split(sep)[0] === "..") throw new Error(`Configuration outside the project directory: ${configPath}`);
  const project = join(temporary, "stage", "project"), packages = join(temporary, "stage", "adapter", "node_modules");
  const copy = { recursive: true, verbatimSymlinks: true, mode: constants.COPYFILE_FICLONE };
  await cp(projectDir, project, { ...copy, filter: source => basename(source) !== ".git" && !localValues(basename(source)) });
  await cp(dependencies, packages, copy);
  const bin = typeof pinned.bin === "string" ? pinned.bin : pinned.bin.wrangler;
  return { project, builder: { source: "adapter", version: pinned.version, packageDirectory: join(packages, "wrangler"), cli: resolve(packages, "wrangler", bin) } };
}
// Wrangler 4 loads .env files into its environment for every command (and treats them as local secret values); a
// builder that has --env-file is pointed at an empty file instead. Older ones load no .env file.
function supportsEnvFile(builder) {
  const dist = join(builder.packageDirectory, "wrangler-dist", "cli.js");
  return existsSync(dist) && readFileSync(dist, "utf8").includes('"env-file": {');
}
async function run(command, args, cwd) {
  const startedAtUnixMs = Date.now(), start = performance.now();
  // A custom build command finds the project's own binaries first, as under `npm run`.
  const path = [join(cwd, "node_modules", ".bin"), process.env.PATH].filter(Boolean).join(delimiter);
  const child = spawn(process.execPath, [command, ...args], { cwd, env: { ...process.env, PATH: path, WRANGLER_SEND_METRICS: "false", NO_COLOR: "1" }, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.on("data", c => { stdout += c; }); child.stderr.on("data", c => { stderr += c; });
  const exitCode = await new Promise((done, reject) => { child.on("error", reject); child.on("close", done); });
  return { command: [process.execPath, command, ...args], cwd, startedAtUnixMs, elapsedMs: performance.now() - start, exitCode, stdout, stderr };
}
async function files(directory, prefix = "") {
  const result = [];
  for (const entry of await readdir(join(directory, prefix), { withFileTypes: true })) {
    const path = prefix ? prefix + "/" + entry.name : entry.name;
    if (entry.isDirectory()) result.push(...await files(directory, path));
    else if (entry.isFile()) result.push(path);
    else throw new Error(`Unsupported build output entry: ${path}`);
  }
  return result.sort();
}
// Limits a package's assets must meet, as the deployment tool checks them (COWORKERD_ASSET_MAX_* override the defaults).
export function assetLimits(env = process.env) {
  const limit = (name, fallback) => {
    const raw = env[name];
    if (raw === undefined || raw === "") return fallback;
    if (!/^[1-9][0-9]*$/.test(raw)) throw new Error(`Invalid ${name}: ${raw}`);
    return Number(raw);
  };
  return { maxCount: limit("COWORKERD_ASSET_MAX_COUNT", 20000), maxFileBytes: limit("COWORKERD_ASSET_MAX_FILE_BYTES", 25 * 1024 * 1024),
    maxTotalBytes: limit("COWORKERD_ASSET_MAX_TOTAL_BYTES", 100 * 1024 * 1024) };
}
// Request path of an asset: '/' and the file's path below the directory, '/'-separated, without control characters or '\\'.
const unsafeAssetPath = path => /[\u0000-\u001f\u007f\\]/.test(path) || path.split("/").slice(1).some(s => s === "" || s === "." || s === "..");
// Cloudflare applies `_redirects` and `_headers` from the asset directory; Coworkerd has neither, so a file with any rule
// in it is unsupported configuration. Comments and blank lines carry none.
async function assetRuleFiles(directory) {
  const found = [];
  for (const name of ["_redirects", "_headers"]) {
    const text = await readFile(join(directory, name), "utf8").catch(error => { if (error.code === "ENOENT") return null; throw error; });
    if (text === null) continue;
    const rules = text.split(/\r?\n/).map(line => line.trim()).filter(line => line && !line.startsWith("#")).length;
    found.push({ name, bytes: Buffer.byteLength(text), rules });
  }
  return found;
}
// Every asset of the directory, as `wrangler dev` serves them: path, size, SHA-256 and content type (a type mime does
// not know is served as application/octet-stream), sorted by path as UTF-8 bytes.
async function collectAssets(directory, limits) {
  const { assetsReverseMap } = await buildAssetManifest(directory);
  const assets = [];
  let total = 0;
  for (const { filePath, contentType } of Object.values(assetsReverseMap)) {
    const path = "/" + filePath.split(sep).join("/");
    if (unsafeAssetPath(path)) throw new Error(`Unsafe static asset path: ${JSON.stringify(path)}`);
    const bytes = await readFile(join(directory, filePath));
    if (bytes.length > limits.maxFileBytes) throw new Error(`Static asset is larger than allowed: ${path} (${bytes.length} > ${limits.maxFileBytes} bytes)`);
    total += bytes.length;
    assets.push({ entry: { bytes: bytes.length, contentType: contentType ?? "application/octet-stream", path, sha256: sha256(bytes) }, content: bytes });
  }
  if (assets.length > limits.maxCount) throw new Error(`Package has more static assets than allowed: ${assets.length} > ${limits.maxCount}`);
  if (total > limits.maxTotalBytes) throw new Error(`Static assets together are larger than allowed: ${total} > ${limits.maxTotalBytes} bytes`);
  return assets.sort((a, b) => Buffer.compare(Buffer.from(a.entry.path), Buffer.from(b.entry.path)));
}
export async function adapt({ config: userConfigPath, output, workerId, versionId = "v1", pathPrefix, secrets: secretArguments = [],
  onIgnored = () => {} }) {
  const projectDir = dirname(resolve(userConfigPath));
  const redirected = redirect(projectDir);
  const configPath = redirected ? redirected.target : resolve(userConfigPath);
  const config = unstable_readConfig({ config: configPath });
  const { rawConfig } = experimental_readRawConfig({ config: configPath });
  const { rejected, ignored } = classify(rawConfig, { redirected: !!redirected });
  for (const item of ignored) onIgnored(item);
  if (rejected.length) throw new Error(`Unsupported Wrangler configuration: ${rejected.join(", ")}`);
  // A site of static assets alone has no main module.
  const assetsConfig = config.assets;
  if ((!config.main && !assetsConfig) || !config.compatibility_date) throw new Error("Worker main and compatibility_date are required");
  const notFoundHandling = assetsConfig?.not_found_handling ?? "none";
  if (assetsConfig && !notFoundHandlings.includes(notFoundHandling)) throw new Error(`Unsupported assets.not_found_handling: ${notFoundHandling}`);
  // The ASSETS binding names the version's own asset layer; run_worker_first is true or a list of path rules (Wrangler
  // checks the rules when it builds; the deployment tool checks them again).
  const assetsBinding = assetsConfig?.binding;
  if (assetsBinding !== undefined && !/^[A-Za-z0-9_.-]+$/.test(assetsBinding)) throw new Error(`Unsupported assets.binding name: ${assetsBinding}`);
  const runWorkerFirst = assetsConfig?.run_worker_first ?? false;
  if (typeof runWorkerFirst !== "boolean" && !(Array.isArray(runWorkerFirst) && runWorkerFirst.every(rule => typeof rule === "string")))
    throw new Error(`Unsupported assets.run_worker_first: ${JSON.stringify(runWorkerFirst)}`);
  const id = workerId ?? config.name;
  if (!id) throw new Error("Worker name is required");
  // Secret names: the keys of .dev.vars.example next to the user's configuration, and --secret NAME. A name given on
  // the command line is not echoed when refused (it could be a value typed by mistake).
  for (const name of secretArguments) {
    if (typeof name !== "string" || !environmentName.test(name))
      throw new Error("Unsupported --secret name: a JavaScript identifier of ASCII letters, digits, '_' and '$' (values are never taken)");
  }
  const examplePath = join(projectDir, SECRETS_EXAMPLE);
  const exampleNames = existsSync(examplePath) ? exampleSecretNames(await readFile(examplePath, "utf8")) : [];
  const badExample = exampleNames.filter(name => !environmentName.test(name));
  if (badExample.length) throw new Error(`Unsupported secret names in ${SECRETS_EXAMPLE}: ${badExample.join(", ")}`);
  const secretNames = [...new Set([...exampleNames, ...secretArguments])].sort();
  if (secretNames.length > 64) throw new Error(`More than 64 secrets: ${secretNames.length}`);
  const taken = new Set([...Object.keys(config.vars ?? {}), ...(config.services ?? []).map(s => s.binding),
    ...(assetsBinding !== undefined ? [assetsBinding] : [])]);
  const clashing = secretNames.filter(name => taken.has(name));
  if (clashing.length) throw new Error(`Secret names collide with vars or bindings: ${clashing.join(", ")}`);
  const temporary = await mkdtemp(join(tmpdir(), "coworkerd-wrangler-"));
  try {
    const outdir = join(temporary, "output");
    const own = projectWrangler(projectDir);
    const staged = own ? null : await stage(projectDir, configPath, temporary);
    const builder = own ?? staged.builder, buildRoot = staged ? staged.project : projectDir;
    const buildConfig = staged ? join(staged.project, relative(projectDir, configPath)) : configPath;
    const emptyEnv = supportsEnvFile(builder) ? join(temporary, "empty.env") : null;
    if (emptyEnv) await writeFile(emptyEnv, "");
    const build = await run(builder.cli, ["deploy", "--dry-run", "--config", buildConfig, "--outdir", outdir,
      ...(emptyEnv ? ["--env-file", emptyEnv] : [])], buildRoot);
    if (build.exitCode !== 0) throw new Error(`Wrangler build failed: ${build.stderr}\n${build.stdout}`);
    // The asset directory as Wrangler resolves it (relative to the configuration), read after the build has run.
    let assets = [], assetDirectory = null, configuredDirectory = null, ruleFiles = [];
    if (assetsConfig) {
      configuredDirectory = resolve(dirname(configPath), assetsConfig.directory);
      assetDirectory = staged ? join(staged.project, relative(projectDir, configuredDirectory)) : configuredDirectory;
      ruleFiles = await assetRuleFiles(assetDirectory);
      const ruled = ruleFiles.filter(f => f.rules > 0).map(f => `assets ${f.name} file`);
      if (ruled.length) throw new Error(`Unsupported Wrangler configuration: ${ruled.join(", ")}`);
      assets = await collectAssets(assetDirectory, assetLimits());
      if (!assets.length) throw new Error(`Static asset directory has no assets: ${assetDirectory}`);
    }
    const outputFiles = existsSync(outdir) ? await files(outdir) : [];
    const mainModule = !config.main ? "" : config.no_bundle ? basename(config.main) : basename(config.main).replace(/\.[cm]?[jt]sx?$/, ".js");
    if (mainModule && !outputFiles.includes(mainModule)) throw new Error(`Wrangler main output missing: ${mainModule}`);
    const rules = mainModule ? config.rules.concat(DEFAULT_MODULE_RULES).map(rule => ({ type: rule.type, include: rule.globs, fallthrough: rule.fallthrough })) : [];
    const kinds = { ESModule: "esm", CommonJS: "cjs", Text: "text", Data: "data", CompiledWasm: "wasm" };
    const modules = [];
    for (const name of mainModule ? outputFiles : []) {
      if (name === "README.md" || name.endsWith(".js.map") || name.endsWith(".mjs.map")) continue;
      let type = name === mainModule ? "esm" : undefined;
      if (!type) for (const rule of rules) if (rule.include.some(glob => matchesGlob(name, glob))) {
        type = kinds[rule.type]; break;
      }
      if (!type) throw new Error(`Unsupported or unclassified Wrangler module: ${name}`);
      const bytes = await readFile(join(outdir, name));
      modules.push({ bytes: bytes.length, name, sha256: sha256(bytes), type });
    }
    // Without assets the manifest is the code-only manifest, byte for byte.
    const manifestBytes = canonical(assets.length ? { assets: assets.map(a => a.entry), modules } : { modules });
    const variables = Object.entries(config.vars ?? {}).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([name, value]) =>
      ({ name, type: typeof value === "string" ? "text" : "json", value: typeof value === "string" ? value : canonical(value) }));
    const version = { versionId, format: "v2", compatibilityDate: config.compatibility_date,
      compatibilityFlags: config.compatibility_flags ?? [], mainModule, contentHash: sha256(manifestBytes), variables,
      ...(secretNames.length ? { secrets: secretNames } : {}),
      bindings: [...(config.services ?? []).map(s => ({ name: s.binding, workerId: s.service, entrypoint: s.entrypoint ?? "default" })),
        ...(assetsBinding !== undefined ? [{ name: assetsBinding, assets: true }] : [])],
      ...(assetsConfig ? { assets: { notFoundHandling, ...(runWorkerFirst === true ? { runWorkerFirst: true } : {}),
        ...(Array.isArray(runWorkerFirst) ? { runWorkerFirstRules: runWorkerFirst } : {}) } } : {}) };
    const deployment = { workers: [{ workerId: id, activeVersion: versionId, versions: [version] }],
      routes: [{ host: "*", pathPrefix: pathPrefix ?? `/${id}`, workerId: id }] };
    await mkdir(dirname(resolve(output)), { recursive: true });
    await mkdir(resolve(output));
    if (existsSync(outdir)) await cp(outdir, join(resolve(output), "wrangler-output"), { recursive: true });
    const directory = join(resolve(output), id, versionId);
    await mkdir(directory, { recursive: true });
    for (const module of modules) {
      await mkdir(dirname(join(directory, module.name)), { recursive: true });
      await cp(join(outdir, module.name), join(directory, module.name));
    }
    // Asset files under <package>/__coworkerd_assets/<path> (module names never start with the reserved __coworkerd_
    // prefix, so no module can collide with them), written from the bytes that were hashed.
    for (const { entry, content } of assets) {
      const target = join(directory, "__coworkerd_assets", ...entry.path.slice(1).split("/"));
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, content);
    }
    await writeFile(join(directory, "package.json"), manifestBytes);
    await writeFile(join(resolve(output), "manifest.json"), JSON.stringify(deployment, null, 2) + "\n");
    const record = { wrangler: builder.version, wranglerSource: builder.source, adapterWrangler: pinned.version,
      configurationApi: "unstable_readConfig", rawConfigurationApi: "experimental_readRawConfig",
      moduleRulesApi: "config.rules + Wrangler 4.136.1 DEFAULT_MODULE_RULES", envFile: emptyEnv ? "empty" : "not supported by the builder",
      defaultsApi: "unstable_defaultWranglerConfig", configPath: resolve(userConfigPath), usedConfigPath: configPath,
      redirect: redirected && { file: redirected.file, content: redirected.content, target: redirected.target }, ignored,
      builder: { ...builder, staged: !!staged, buildRoot, buildConfig }, build, mainModule, modules, packageManifest: manifestBytes, version, outputFiles,
      lockSha256: sha256(await readFile(new URL("./package-lock.json", import.meta.url))) };
    if (assetsConfig) {
      const contentTypes = {};
      for (const { entry } of assets) contentTypes[entry.contentType] = (contentTypes[entry.contentType] ?? 0) + 1;
      record.assets = { configured: rawConfig.assets, directory: configuredDirectory, readFrom: assetDirectory, notFoundHandling,
        htmlHandling: assetDefaults.html_handling, binding: assetsBinding ?? null, runWorkerFirst,
        count: assets.length, bytes: assets.reduce((sum, a) => sum + a.entry.bytes, 0), contentTypes, ruleFiles,
        assetWalkApi: "miniflare buildAssetManifest" };
    }
    // Names only, with where each came from.
    if (secretNames.length || exampleNames.length) record.secrets = { names: secretNames,
      example: exampleNames.length ? { file: SECRETS_EXAMPLE, names: exampleNames } : null, arguments: [...new Set(secretArguments)].sort() };
    await writeFile(join(resolve(output), "adapter.json"), JSON.stringify(record, null, 2) + "\n");
    return { deployment, record };
  } finally { await rm(temporary, { recursive: true, force: true }); }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Positional CONFIG OUTPUT [WORKER_ID] [VERSION_ID]; --secret NAME (repeatable) declares a secret by name.
  const positional = [], secrets = [];
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--secret") {
      if (index + 1 >= args.length) throw new Error("--secret requires a NAME");
      secrets.push(args[++index]);
    } else if (args[index].startsWith("--secret=")) secrets.push(args[index].slice("--secret=".length));
    else positional.push(args[index]);
  }
  const [config, output, workerId, versionId] = positional;
  if (!config || !output || positional.length > 4)
    throw new Error("Usage: node adapter.mjs CONFIG OUTPUT [WORKER_ID] [VERSION_ID] [--secret NAME]...");
  const onIgnored = item => console.error(`Ignored Wrangler configuration: ${item.key} (${item.reason}: ${item.detail})`);
  const result = await adapt({ config, output, workerId, versionId, secrets, onIgnored });
  const { record } = result;
  if (record.redirect) console.error(`Followed Wrangler deploy redirect: ${record.redirect.file} -> ${record.usedConfigPath}`);
  console.log(JSON.stringify({ output: resolve(output), contentHash: record.version.contentHash, modules: record.modules.length,
    ...(record.assets ? { assets: record.assets.count, assetBytes: record.assets.bytes } : {}),
    ...(record.assets?.binding ? { assetsBinding: record.assets.binding } : {}),
    ...(record.assets && record.assets.runWorkerFirst !== false ? { runWorkerFirst: record.assets.runWorkerFirst } : {}),
    ...(record.secrets?.names.length ? { secrets: record.secrets.names } : {}),
    configPath: record.usedConfigPath, wrangler: record.wrangler, wranglerSource: record.wranglerSource, ignored: record.ignored.map(i => i.key) }));
}
