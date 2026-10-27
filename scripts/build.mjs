// Compile only the package entry's import graph. Tests and upstream's client
// aren't package entries; public Convex imports stay external.
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import process from "process";
import { fileURLToPath } from "url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = path.join(root, "src");
const dist = path.join(root, "dist");
fs.rmSync(dist, { recursive: true, force: true });

const tsc = spawnSync(
  process.execPath,
  [
    path.join(root, "node_modules/typescript/bin/tsc"),
    "-p",
    path.join(root, "tsconfig.build.json"),
  ],
  { stdio: "inherit" },
);
if (tsc.status !== 0) process.exit(tsc.status ?? 1);

// Preserve licenses next to the emitted vendored modules, e.g. jwt-decode.
function copyLicenses(directory) {
  const license = path.join(src, path.relative(dist, directory), "LICENSE");
  if (fs.existsSync(license))
    fs.copyFileSync(license, path.join(directory, "LICENSE"));
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) copyLicenses(path.join(directory, entry.name));
  }
}
copyLicenses(dist);
