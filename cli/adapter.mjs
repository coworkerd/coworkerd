import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { constants, existsSync, readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, open, link, rename, readFile, readdir, rm, writeFile } from "node:fs/promises";
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
  "jsx_factory", "jsx_fragment", "alias", "define", "keep_names", "upload_source_maps", "send_metrics", "assets",
  "durable_objects", "migrations", "d1_databases"]);
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
// A name within a tenant (a database): the same shape as a tenant or Worker name.
const resourceName = /^[a-z0-9][a-z0-9-]{0,62}$/;
// Local value files: kept out of the staged copy of a project (the build does not need them).
const localValues = name => (name === ".dev.vars" || name.startsWith(".dev.vars.") || name === ".env" || name.startsWith(".env.")) &&
  !name.endsWith(".example");
// What the staged copy leaves out (.git and the local value files, at any depth) is never a static asset either, and
// never read, whichever Wrangler builds the project: a project's own Wrangler builds in the project itself.
const leftOut = path => path.split("/").some(segment => segment === ".git" || localValues(segment));
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
  // Durable Objects: a namespace Binding names a class of this Worker; migrations declare, delete and rename classes.
  for (const key of Object.keys(raw.durable_objects ?? {})) if (key !== "bindings") found.push(`durable_objects.${key}`);
  // d1_databases: the Binding and the database it names (database_id is Cloudflare's account-scoped id; here a
  // database is named within its tenant, so the name is what counts and an id is ignored).
  for (const [index, binding] of (Array.isArray(raw.d1_databases) ? raw.d1_databases : []).entries()) {
    for (const key of Object.keys(binding ?? {}))
      if (!["binding", "database_name", "database_id", "preview_database_id", "migrations_dir", "migrations_table", "migrations_pattern"].includes(key))
        found.push(`d1_databases[${index}].${key}`);
  }
  for (const [index, binding] of (raw.durable_objects?.bindings ?? []).entries()) {
    for (const key of Object.keys(binding ?? {})) if (!["name", "class_name", "script_name"].includes(key)) found.push(`durable_objects.bindings[${index}].${key}`);
  }
  for (const [index, migration] of (Array.isArray(raw.migrations) ? raw.migrations : []).entries()) {
    for (const key of Object.keys(migration ?? {}))
      if (!["tag", "new_classes", "new_sqlite_classes", "deleted_classes", "renamed_classes", "transferred_classes"].includes(key))
        found.push(`migrations[${index}].${key}`);
  }
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
    // Durable Object keys outside the supported shape are refused whatever their value.
    if (key.startsWith("durable_objects.") || key.startsWith("migrations[") || key.startsWith("d1_databases[")) { rejected.push(key); continue; }
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
  await cp(projectDir, project, { ...copy, filter: source => !leftOut(basename(source)) });
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
    if (leftOut(path)) continue;
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
// The version's Durable Objects: wrangler's durable_objects.bindings (each a name and one of this Worker's classes; a
// script_name of another Worker is refused) and the classes its migrations leave, applied in order -- new_classes and
// new_sqlite_classes both declare a class (every class runs on SQLite), deleted_classes delete one, renamed_classes
// rename one (kept in their order: the control plane moves a class's namespace to its new name); transferred_classes
// (a class of another Worker) is refused. Each Binding's class is one the migrations leave declared.
export function durableObjectsOf(config, names) {
  const bindings = [];
  for (const [index, binding] of (config.durable_objects?.bindings ?? []).entries()) {
    const where = `durable_objects.bindings[${index}]`;
    if (typeof binding?.name !== "string" || !environmentName.test(binding.name)) throw new Error(`Unsupported ${where}.name: a JavaScript identifier`);
    if (typeof binding.class_name !== "string" || !environmentName.test(binding.class_name))
      throw new Error(`Unsupported ${where}.class_name: a JavaScript identifier`);
    if (binding.script_name !== undefined && !names.includes(binding.script_name))
      throw new Error(`Unsupported Wrangler configuration: ${where}.script_name names Worker ${binding.script_name}: ` +
        "a Durable Object class of another Worker is not supported (only this Worker's own classes)");
    bindings.push({ binding: binding.name, className: binding.class_name });
  }
  const declared = [], deleted = [], renamed = [];
  const className = (name, where) => {
    if (typeof name !== "string" || !environmentName.test(name) || name.startsWith("__coworkerd")) throw new Error(`Unsupported ${where}: ${JSON.stringify(name)}`);
    return name;
  };
  for (const [index, migration] of (Array.isArray(config.migrations) ? config.migrations : []).entries()) {
    const where = `migrations[${index}]`;
    if (typeof migration?.tag !== "string" || !migration.tag) throw new Error(`Unsupported ${where}: a migration needs its tag`);
    if ((migration.transferred_classes ?? []).length)
      throw new Error(`Unsupported Wrangler configuration: ${where}.transferred_classes: a Durable Object class of another Worker is not supported`);
    for (const name of [...(migration.new_classes ?? []), ...(migration.new_sqlite_classes ?? [])]) {
      className(name, `${where} new class`);
      if (declared.includes(name)) throw new Error(`Unsupported ${where}: class ${name} is declared twice`);
      declared.push(name);
      const at = deleted.indexOf(name);
      if (at >= 0) deleted.splice(at, 1);
    }
    for (const rename of migration.renamed_classes ?? []) {
      const from = className(rename?.from, `${where}.renamed_classes from`), to = className(rename?.to, `${where}.renamed_classes to`);
      const at = declared.indexOf(from);
      if (at < 0) throw new Error(`Unsupported ${where}: renamed class ${from} is not declared`);
      if (declared.includes(to)) throw new Error(`Unsupported ${where}: class ${to} is already declared`);
      declared[at] = to;
      renamed.push({ from, to });
    }
    for (const name of migration.deleted_classes ?? []) {
      className(name, `${where}.deleted_classes`);
      const at = declared.indexOf(name);
      if (at < 0) throw new Error(`Unsupported ${where}: deleted class ${name} is not declared`);
      declared.splice(at, 1);
      if (!deleted.includes(name)) deleted.push(name);
    }
  }
  for (const binding of bindings)
    if (!binding.database && !declared.includes(binding.className))
      throw new Error(`Unsupported durable_objects binding ${binding.binding}: class ${binding.className} is not declared by the migrations ` +
        "(new_classes or new_sqlite_classes)");
  // d1_databases: each Binding names one of the tenant's databases. Its class is the platform's own SQL Worker
  // class, which no version declares, so these Bindings go beside the Durable Object ones with the database named.
  for (const [index, binding] of (Array.isArray(config.d1_databases) ? config.d1_databases : []).entries()) {
    const where = `d1_databases[${index}]`;
    if (typeof binding?.binding !== "string" || !environmentName.test(binding.binding))
      throw new Error(`Unsupported ${where}.binding: a JavaScript identifier`);
    const name = binding.database_name;
    if (typeof name !== "string" || !resourceName.test(name))
      throw new Error(`Unsupported ${where}.database_name: a name of [a-z0-9][a-z0-9-]{0,62} (the database of this tenant)`);
    if (bindings.some(one => one.binding === binding.binding))
      throw new Error(`Unsupported ${where}.binding: ${binding.binding} is already a Durable Object Binding`);
    bindings.push({ binding: binding.binding, className: "__coworkerdSqlDatabase", database: name });
  }
  if (!bindings.length && !declared.length && !deleted.length && !renamed.length) return null;
  return { durableObjects: bindings, classes: { declared, deleted, renamed } };
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
  const durable = durableObjectsOf(config, [...new Set([id, config.name].filter(Boolean))]);
  const taken = new Set([...Object.keys(config.vars ?? {}), ...(config.services ?? []).map(s => s.binding),
    ...(assetsBinding !== undefined ? [assetsBinding] : [])]);
  const clashing = secretNames.filter(name => taken.has(name));
  if (clashing.length) throw new Error(`Secret names collide with vars or bindings: ${clashing.join(", ")}`);
  const durableClashing = (durable?.durableObjects ?? []).map(b => b.binding).filter(name => taken.has(name) || secretNames.includes(name));
  if (durableClashing.length) throw new Error(`Durable Object binding names collide with vars, secrets or bindings: ${durableClashing.join(", ")}`);
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
      ...(durable ? durable : {}),
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
// D1 files and their ledger, over the existing atomic /query array protocol.

export const MAX_QUERY_BYTES = 1024 * 1024;
const identifier = value => `"${value.replaceAll('"', '""')}"`;
const number = name => Number.parseInt(name.split("_")[0], 10);
export function compareMigrations(a, b) {
  const left = number(a), right = number(b);
  if (left !== right) {
    if (Number.isFinite(left) && Number.isFinite(right)) return left - right;
    if (Number.isFinite(left)) return -1;
    if (Number.isFinite(right)) return 1;
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

export async function migrationSettings({ database, configPath, dir, table, cwd = process.cwd() }) {
  let entry;
  if (configPath) {
    const config = unstable_readConfig({ config: configPath });
    entry = config.d1_databases?.find(item => item.database_name === database);
  }
  if (entry?.migrations_pattern !== undefined)
    throw new Error("migrations_pattern is not supported by cowctl d1 migrations; use top-level .sql files");
  const directory = dir !== undefined ? resolve(cwd, dir)
    : resolve(configPath ? dirname(configPath) : cwd, entry?.migrations_dir ?? "migrations");
  const ledger = table ?? entry?.migrations_table ?? "d1_migrations";
  if (typeof ledger !== "string" || !ledger.length || ledger.includes("\0"))
    throw new Error("the migration table must be a non-empty SQL identifier");
  return { directory, table: ledger };
}

export async function migrationFiles(directory) {
  let entries;
  try { entries = await readdir(directory, { withFileTypes: true }); }
  catch (error) { if (error.code === "ENOENT") return []; throw error; }
  if (entries.some(item => item.isDirectory()))
    throw new Error(`subdirectory migration layouts are not supported: ${directory}; use top-level .sql files`);
  return entries.filter(item => item.isFile() && item.name.endsWith(".sql")).map(item => item.name).sort(compareMigrations);
}

export async function createMigration(directory, description) {
  if (!description?.trim() || /[\\/\0]/.test(description))
    throw new Error("the migration description must be non-empty and contain no path separators");
  const files = await migrationFiles(directory);
  const next = Math.max(0, ...files.map(number).filter(Number.isFinite)) + 1;
  if (!Number.isSafeInteger(next)) throw new Error("the migration number exceeds the safe integer range");
  const name = `${String(next).padStart(4, "0")}_${description.replaceAll(" ", "_")}.sql`;
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, name), `-- Migration number: ${String(next).padStart(4, "0")} \t ${new Date().toISOString()}\n`, { flag: "wx" });
  return name;
}

function checked(answer) {
  const rows = Array.isArray(answer) ? answer : [answer];
  if (!rows.length || rows.some(row => !row?.success))
    throw new Error(rows.find(row => !row?.success)?.error ?? "the SQL query returned no successful answer");
  return rows;
}
async function sql(query, statement, params) {
  return checked(await query(JSON.stringify({ sql: statement, ...(params ? { params } : {}) })))[0];
}
export async function appliedMigrations(query, table, { initialize = false } = {}) {
  const escaped = identifier(table);
  if (initialize) await sql(query, `CREATE TABLE IF NOT EXISTS ${escaped} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)`);
  else {
    const exists = await sql(query, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", [table]);
    if (!exists.results?.length) return new Set();
  }
  const result = await sql(query, `SELECT name FROM ${escaped} ORDER BY id`);
  return new Set((result.results ?? []).map(row => row.name));
}
export async function pendingMigrations({ directory, table, query, initialize = false }) {
  const files = await migrationFiles(directory);
  const applied = await appliedMigrations(query, table, { initialize });
  return files.filter(name => !applied.has(name));
}

// Recognize a file with no statements. Actual SQL is neither split nor rewritten: SQLite handles it.
function hasStatement(text) {
  let rest = text.trimStart();
  while (rest) {
    if (rest.startsWith("--")) { const end = rest.indexOf("\n"); rest = end < 0 ? "" : rest.slice(end + 1).trimStart(); }
    else if (rest.startsWith("/*")) { const end = rest.indexOf("*/", 2); rest = end < 0 ? "" : rest.slice(end + 2).trimStart(); }
    else return true;
  }
  return false;
}
export function migrationBody(text, name, table) {
  // A final no-op gives prepare/exec a concrete last statement even after trailing comments.
  // A newline ends a line comment; the separator also handles a missing terminal semicolon.
  return JSON.stringify([{ sql: hasStatement(text) ? `${text}\n;\nSELECT 1` : "SELECT 1" },
    { sql: `INSERT INTO ${identifier(table)} (name) VALUES (?)`, params: [name] }]);
}
function uncertain(error) {
  return (error.status === 503 && error.code === "RESULT_UNCERTAIN") ||
    [error.name, error.code, error.cause?.name, error.cause?.code].some(value => ["TimeoutError", "ETIMEDOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT"].includes(value));
}
export async function applyMigrations({ directory, table, query, files, onApplied = () => {} }) {
  const applied = [];
  for (const name of files) {
    try {
      const body = migrationBody(await readFile(join(directory, name), "utf8"), name, table);
      if (Buffer.byteLength(body, "utf8") >= MAX_QUERY_BYTES)
        throw new Error("file is too large: the JSON request including the ledger reaches or exceeds 1 MiB; split the file");
      let recovered = false;
      try { checked(await query(body)); }
      catch (error) {
        if (!uncertain(error)) throw error;
        let ledger;
        try { ledger = await appliedMigrations(query, table); }
        catch (readError) { throw new Error(`result uncertain and the ledger could not be read (${readError.message}); run apply again; the write was not resent`); }
        if (!ledger.has(name)) throw new Error("result uncertain and the file is not in the ledger; run apply again; the write was not resent");
        recovered = true;
      }
      applied.push(name);
      onApplied(name, { recovered });
    } catch (error) {
      const failure = new Error(`${name}: ${error.message}`, { cause: error });
      failure.file = name; failure.applied = applied;
      throw failure;
    }
  }
  return applied;
}

// SQLite, in the SQL Worker, chooses statement boundaries. Keep incomplete text verbatim.
// Inspect only the last SQL token, without splitting or executing statements. An extra empty
// statement is refused by native ingest, so an already terminated file must not gain another ';'.
function sqlTail(text) {
  let quote = null, comment = null, last = "";
  for (let i = 0; i < text.length; i++) {
    const char = text[i], next = text[i + 1];
    if (comment === "line") { if (char === "\n" || char === "\r") comment = null; continue; }
    if (comment === "block") { if (char === "*" && next === "/") { comment = null; i++; } continue; }
    if (quote) {
      if (char === quote) { if (next === quote && quote !== "]") i++; else quote = null; }
      continue;
    }
    if (char === "-" && next === "-") { comment = "line"; i++; continue; }
    if (char === "/" && next === "*") { comment = "block"; i++; continue; }
    if (/\s/.test(char)) continue;
    last = char;
    if (["'", '"', "`", "["].includes(char)) quote = char === "[" ? "]" : char;
  }
  return last;
}

export const IMPORT_CHUNK_BYTES = 32 * 1024;
export const IMPORT_REQUEST_BYTES = MAX_QUERY_BYTES - 64 * 1024;
export async function importSql({ file, query, onProgress = () => {} }) {
  const report = { file: resolve(file), committedOffset: 0, statementCount: 0, chunks: 0, rowsRead: 0, rowsWritten: 0 };
  let pending = "", sentBytes = 0, sourceBytes = 0, handle, stream;
  const bodyOf = text => JSON.stringify({ ingest: text });
  const size = text => Buffer.byteLength(bodyOf(text), "utf8");
  const send = async text => {
    const answer = checked(await query(bodyOf(text)))[0], ingest = answer.ingest;
    if (typeof ingest?.remainder !== "string" || !text.endsWith(ingest.remainder) ||
        !Number.isSafeInteger(ingest.statementCount) || ingest.statementCount < 0)
      throw new Error("the SQL Worker returned an invalid ingest answer; current chunk commit is uncertain");
    pending = ingest.remainder;
    report.committedOffset = Math.min(sourceBytes, sentBytes - Buffer.byteLength(pending, "utf8"));
    report.statementCount += ingest.statementCount;
    report.rowsRead += ingest.rowsRead;
    report.rowsWritten += ingest.rowsWritten;
    report.chunks++;
    onProgress({ ...report });
  };
  try {
    handle = await open(report.file, "r");
    if (!(await handle.stat()).isFile()) throw new Error("the import source must be a regular UTF-8 file");
    stream = handle.createReadStream({ highWaterMark: IMPORT_CHUNK_BYTES, autoClose: false });
    async function* texts() {
      // Fatal decoding preserves the relationship between UTF-8 byte positions and SQLite's remainder.
      const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
      for await (const bytes of stream) { const text = decoder.decode(bytes, { stream: true }); if (text) yield text; }
      const tail = decoder.decode(); if (tail) yield tail;
    }
    const iterator = texts()[Symbol.asyncIterator]();
    let current = await iterator.next();
    while (!current.done) {
      const next = await iterator.next();
      sourceBytes += Buffer.byteLength(current.value, "utf8");
      // Look ahead to EOF: a small file, including a missing final semicolon, is one transaction.
      const tail = next.done ? sqlTail(pending + current.value) : null;
      if (tail === "") { pending = ""; break; } // EOF contains only whitespace and comments.
      let text = current.value + (next.done && tail !== ";" ? "\n;" : "");
      while (text) {
        let length = text.length;
        if (size(pending + text) > IMPORT_REQUEST_BYTES) {
          let low = 0, high = length;
          while (low < high) {
            const mid = Math.ceil((low + high) / 2);
            if (size(pending + text.slice(0, mid)) <= IMPORT_REQUEST_BYTES) low = mid;
            else high = mid - 1;
          }
          length = low;
          // Never cut a Unicode code point between its UTF-16 surrogate halves.
          if (length && /[\uD800-\uDBFF]/.test(text[length - 1])) length--;
          if (!length) {
            const bytes = Buffer.byteLength(pending, "utf8") + Buffer.byteLength(String.fromCodePoint(text.codePointAt(0)), "utf8");
            throw new Error(`statement at file byte ${report.committedOffset} is too large: at least ${bytes} source bytes; its JSON request exceeds ${IMPORT_REQUEST_BYTES} bytes (960 KiB); oversized request was not sent`);
          }
        }
        const part = text.slice(0, length); text = text.slice(length);
        sentBytes += Buffer.byteLength(part, "utf8");
        await send(pending + part);
      }
      current = next;
    }
    if (hasStatement(pending)) throw new Error("incomplete statement at end of file after appending a newline and semicolon");
    // SQLite may leave only whitespace/comments in its remainder. These contain no uncommitted SQL.
    report.committedOffset = sourceBytes;
    onProgress({ ...report });
    return report;
  } catch (error) {
    const isUncertain = uncertain(error) || error.message.includes("current chunk commit is uncertain");
    const reason = isUncertain ? `current chunk commit is uncertain; request was not resent: ${error.message}` : error.message;
    const failure = new Error(`${report.file}: committed through file byte ${report.committedOffset}; ${report.statementCount} statements executed; ${reason}`, { cause: error });
    failure.import = { ...report, uncertain: isUncertain, error: error.message };
    throw failure;
  } finally {
    stream?.destroy();
    await handle?.close();
  }
}

// D1 export uses database-produced SQL literals, never JavaScript representations of cell values.
export const EXPORT_PAGE_ROWS = 1000;
export const EXPORT_STATEMENT_BYTES = 100000;
const exportSchemaSql = "SELECT rowid,name,type,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid";
const exportStateStatements = [{ sql: "SELECT quote(total_changes()) AS changes" }, { sql: exportSchemaSql }];
const exportTooLarge = error => /D1_RESULT_TOO_LARGE|string or blob too big/i.test(error.message);
const exportLiteral = column => {
  const c = identifier(column);
  // SQLite quote(TEXT) stops at NUL. Let SQLite quote the complete bytes and construct a TEXT expression instead.
  return `CASE WHEN typeof(${c})='text' AND instr(${c},char(0))>0 THEN 'CAST('||quote(CAST(${c} AS BLOB))||' AS TEXT)' ELSE quote(${c}) END`;
};
async function exportAnswers(query, statements) {
  const body = JSON.stringify(statements);
  if (Buffer.byteLength(body) >= MAX_QUERY_BYTES) throw new Error("export query exceeds the 1 MiB request limit; select fewer tables with --table");
  return checked(await query(body));
}
async function exportState(query) {
  const answers = await exportAnswers(query, exportStateStatements);
  return { changes: answers[0].results[0].changes, schema: answers[1].results };
}
function exportTableList(schema, filter) {
  return schema.filter(o => o.type === "table" && (!filter.size || filter.has(o.name))).map(o => ({ ...o })).sort((a, b) =>
    Number(a.name === "sqlite_sequence") - Number(b.name === "sqlite_sequence") || a.rowid - b.rowid).filter(o => {
    if (/^CREATE\s+VIRTUAL\s+TABLE/i.test(o.sql)) throw new Error(`cannot export virtual table ${o.name}`);
    return !o.name.startsWith("_cf_") && (o.name === "sqlite_sequence" || /^sqlite_stat./.test(o.name) || !o.name.startsWith("sqlite_"));
  });
}
function exportSelect(table, { limit, after } = {}) {
  const values = table.columns.map((c, i) => `${exportLiteral(c.name)} AS v${i}`);
  const keys = table.keys.map((c, i) => `${exportLiteral(c)} AS k${i}`);
  const ids = table.keys.map(identifier);
  const where = after ? ` WHERE (${ids.join(",")}) > (${after.join(",")})` : "";
  return `SELECT ${[...values, ...keys].join(",")} FROM ${identifier(table.name)}${where} ORDER BY ${ids.join(",")}${limit ? ` LIMIT ${limit}` : ""}`;
}

export async function exportSql({ file, query, noSchema = false, noData = false, tables = [], force = false,
  allowWrites = false, onProgress = () => {} }) {
  if (noSchema && noData) throw new Error("--no-schema and --no-data cannot be combined");
  if (typeof file !== "string" || !file) throw new Error("export needs --output FILE");
  const output = resolve(file);
  if (!force && existsSync(output)) throw new Error(`${output} already exists; use --force to replace it`);
  const report = { file: output, mode: "atomic", rows: 0, tables: [], bytes: 0, pages: 0, retries: 0,
    initialPageRows: EXPORT_PAGE_ROWS, consistency: { checked: false, changesDetected: false, allowWrites },
    oversizedStatements: 0, oversizedTables: {} };
  let directory, handle;
  try {
    const start = await exportState(query), filter = new Set(tables);
    const plan = exportTableList(start.schema, filter);
    for (const table of plan) {
      if (noData) { table.columns = []; table.keys = []; continue; }
      table.columns = (await sql(query, `PRAGMA table_info(${identifier(table.name)})`)).results;
      const names = new Set(table.columns.map(c => c.name.toLowerCase()));
      const alias = ["_rowid_", "rowid", "oid"].find(name => !names.has(name));
      table.keys = !/\bWITHOUT\s+ROWID\b/i.test(table.sql) && alias ? [alias] :
        table.columns.filter(c => c.pk).sort((a, b) => a.pk - b.pk).map(c => c.name);
      if (!table.columns.length || !table.keys.length) throw new Error(`cannot paginate table ${table.name}: no accessible rowid or primary key`);
    }
    let snapshot;
    try {
      snapshot = await exportAnswers(query, [...exportStateStatements, ...(!noData ? plan.map(t => ({ sql: exportSelect(t) })) : [])]);
      if (canonical(snapshot[1].results) !== canonical(start.schema))
        throw new Error("database schema changed while planning export; stop writes and retry");
    } catch (error) {
      if (!exportTooLarge(error)) throw error;
      report.mode = "paged"; report.retries++;
      onProgress({ phase: "retry", pageRows: EXPORT_PAGE_ROWS, reason: error.message });
    }
    directory = await mkdtemp(join(dirname(output), ".cowctl-export-"));
    handle = await open(join(directory, "export.sql"), "wx", 0o600);
    const emit = async statement => { const text = statement + "\n"; await handle.writeFile(text); report.bytes += Buffer.byteLength(text); };
    await emit("PRAGMA defer_foreign_keys=TRUE;");
    for (const [index, table] of plan.entries()) {
      const item = { name: table.name, rows: 0 }; report.tables.push(item);
      if (!noSchema) {
        if (table.name === "sqlite_sequence") await emit("DELETE FROM sqlite_sequence;");
        else if (/^sqlite_stat./.test(table.name)) await emit("ANALYZE sqlite_schema;");
        else await emit((/CREATE TABLE ['"].*/.test(table.sql) ? `CREATE TABLE IF NOT EXISTS ${table.sql.substring(13)}` : table.sql) + ";");
      }
      if (noData) continue;
      const writeRows = async rows => {
        for (const row of rows) {
          const literals = table.columns.map((_, i) => row[`v${i}`]);
          if (literals.some(v => typeof v !== "string")) throw new Error(`invalid quoted value in table ${table.name}`);
          const statement = `INSERT INTO ${identifier(table.name)} (${table.columns.map(c => identifier(c.name)).join(",")}) VALUES(${literals.join(",")});`;
          if (Buffer.byteLength(statement) > EXPORT_STATEMENT_BYTES) {
            report.oversizedStatements++;
            report.oversizedTables[table.name] = (report.oversizedTables[table.name] ?? 0) + 1;
          }
          await emit(statement); item.rows++; report.rows++;
        }
      };
      if (snapshot) await writeRows(snapshot[index + 2].results);
      else {
        let after, pageRows = EXPORT_PAGE_ROWS;
        for (;;) {
          let rows;
          try { rows = (await sql(query, exportSelect(table, { limit: pageRows, after }))).results; }
          catch (error) {
            if (!exportTooLarge(error)) throw new Error(`table ${table.name}: ${error.message}`, { cause: error });
            report.retries++;
            if (pageRows === 1) {
              const ids = table.keys.map(identifier);
              const where = after ? ` WHERE (${ids.join(",")}) > (${after.join(",")})` : "";
              const row = (await sql(query, `SELECT ${table.keys.map((c, i) => `${exportLiteral(c)} AS k${i}`).join(",")} FROM ${identifier(table.name)}${where} ORDER BY ${ids.join(",")} LIMIT 1`)).results[0];
              throw new Error(`table ${table.name}, row key (${table.keys.map((_, i) => row?.[`k${i}`]).join(",")}): one row exceeds the 7340032-byte reply limit`);
            }
            pageRows = Math.max(1, Math.floor(pageRows / 2));
            onProgress({ phase: "retry", table: table.name, pageRows, reason: error.message });
            continue;
          }
          await writeRows(rows); report.pages++;
          onProgress({ phase: "page", table: table.name, rows: item.rows, pageRows });
          if (rows.length < pageRows) break;
          after = table.keys.map((_, i) => rows.at(-1)[`k${i}`]);
        }
      }
      onProgress({ phase: "table", table: table.name, rows: item.rows });
    }
    if (!noSchema) {
      const schema = snapshot ? snapshot[1].results : start.schema;
      const rest = schema.filter(o => ["index", "trigger", "view"].includes(o.type) &&
        !o.name.startsWith("_cf_") && !o.tbl_name.startsWith("_cf_") && (!filter.size || filter.has(o.name)))
        .sort((a, b) => a.type.localeCompare(b.type, "en") || a.rowid - b.rowid);
      for (const object of rest) await emit(object.sql + ";");
    }
    if (!snapshot) {
      const end = await exportState(query);
      report.consistency.checked = true;
      report.consistency.changesDetected = start.changes !== end.changes || canonical(start.schema) !== canonical(end.schema);
      if (report.consistency.changesDetected && !allowWrites)
        throw new Error("database changed during export; stop writes and retry, or use --allow-writes for a potentially inconsistent export");
      if (report.consistency.changesDetected) await emit("-- WARNING: database changed during export; --allow-writes accepted a potentially inconsistent file.");
    }
    await handle.close(); handle = undefined;
    const temporary = join(directory, "export.sql");
    if (force) await rename(temporary, output);
    else await link(temporary, output); // Atomic no-clobber publication, including a file created while exporting.
    return report;
  } catch (error) {
    error.export = { ...report, error: error.message };
    throw error;
  } finally {
    if (handle) await handle.close();
    if (directory) await rm(directory, { recursive: true, force: true });
  }
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
