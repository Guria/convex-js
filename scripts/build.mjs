// Builds @reatomized/convex from src/browser/reatom/index.ts.
//
// Copies the entry and every module it reaches into staged/, keeping their
// paths under src/. Imports of src/values and src/server become imports of
// `convex/values` and `convex/server`, the peer dependency, so the client
// shares ConvexError and function references with the app's `convex`. tsc
// then compiles staged/ to dist/, type-checking those imports against the
// public types of the `convex` dev dependency, and staged/ is removed.
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import process from "process";
import url from "url";

const root = path.join(path.dirname(url.fileURLToPath(import.meta.url)), "..");
const repoSrc = path.join(root, "src");
const staged = path.join(root, "staged");
const dist = path.join(root, "dist");
const entry = path.join(repoSrc, "browser", "reatom", "index.ts");
const peers = [
  [path.join(repoSrc, "values") + path.sep, "convex/values"],
  [path.join(repoSrc, "server") + path.sep, "convex/server"],
];

const specifier = /(\bfrom\s+|\bimport\s*\(\s*|\bimport\s+)"(\.{1,2}\/[^"]+)"/g;

function resolveSource(fromFile, spec) {
  const target = path.resolve(path.dirname(fromFile), spec);
  for (const candidate of [
    target.replace(/\.js$/, ".ts"),
    target.replace(/\.js$/, ".tsx"),
    path.join(target, "index.ts"),
  ]) {
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error(`Cannot resolve ${spec} from ${fromFile}`);
}

fs.rmSync(staged, { recursive: true, force: true });
fs.rmSync(dist, { recursive: true, force: true });

const queue = [entry];
const seen = new Set(queue);
while (queue.length > 0) {
  const file = queue.shift();
  const source = fs.readFileSync(file, "utf-8");
  const rewritten = source.replace(specifier, (match, prefix, spec) => {
    const target = resolveSource(file, spec);
    const peer = peers.find(([dir]) => target.startsWith(dir));
    if (peer) return `${prefix}"${peer[1]}"`;
    if (!seen.has(target)) {
      seen.add(target);
      queue.push(target);
    }
    return match;
  });
  const out = path.join(staged, path.relative(repoSrc, file));
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, rewritten);
}

const tsc = spawnSync(
  process.execPath,
  [
    path.join(root, "node_modules", "typescript", "bin", "tsc"),
    "-p",
    path.join(root, "tsconfig.build.json"),
  ],
  { stdio: "inherit" },
);
if (tsc.status !== 0) process.exit(tsc.status ?? 1);
fs.rmSync(staged, { recursive: true });

// Vendored modules with their own license, e.g. src/vendor/jwt-decode.
for (const file of seen) {
  const license = path.join(path.dirname(file), "LICENSE");
  if (fs.existsSync(license)) {
    const dir = path.join(dist, path.relative(repoSrc, path.dirname(file)));
    fs.copyFileSync(license, path.join(dir, "LICENSE"));
  }
}
process.stdout.write(`Built ${seen.size} modules into dist.\n`);
