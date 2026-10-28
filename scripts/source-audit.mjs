// Classify local sources by the package entries and test entry points.
import fs from "fs";
import path from "path";
import process from "process";
import { fileURLToPath } from "url";
import ts from "typescript";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function auditSources() {
  const configPath = path.join(root, "tsconfig.json");
  const config = ts.readConfigFile(configPath, ts.sys.readFile);
  const { options, fileNames } = ts.parseJsonConfigFileContent(
    config.config,
    ts.sys,
    root,
  );
  const sources = fileNames.filter((file) =>
    file.startsWith(path.join(root, "src") + path.sep),
  );
  const imports = sources.flatMap((file) =>
    ts
      .preProcessFile(fs.readFileSync(file, "utf8"))
      .importedFiles.map(({ fileName }) => ({ file, specifier: fileName })),
  );
  const closure = (entries) => {
    const seen = new Set(entries);
    const queue = [...entries];
    while (queue.length) {
      const file = queue.shift();
      for (const { file: from, specifier } of imports) {
        if (from !== file || !specifier.startsWith(".")) continue;
        const resolved = ts.resolveModuleName(
          specifier,
          file,
          options,
          ts.sys,
        ).resolvedModule;
        if (!resolved)
          throw new Error(`Unresolved import ${specifier} from ${file}`);
        const target = resolved.resolvedFileName;
        if (!seen.has(target)) {
          seen.add(target);
          queue.push(target);
        }
      }
    }
    return seen;
  };
  const build = ts.readConfigFile(
    path.join(root, "tsconfig.build.json"),
    ts.sys.readFile,
  );
  const runtime = closure(
    build.config.files.map((file) => path.join(root, file)),
  );
  const tests = closure(sources.filter((file) => file.endsWith(".test.ts")));
  const relative = (files) =>
    [...files].map((file) => path.relative(root, file)).sort();
  return {
    runtime: relative(runtime),
    testOnly: relative(
      sources.filter((file) => !runtime.has(file) && tests.has(file)),
    ),
    unused: relative(
      sources.filter((file) => !runtime.has(file) && !tests.has(file)),
    ),
    peers: [
      ...new Set(
        imports
          .map(({ specifier }) => specifier)
          .filter((specifier) => specifier.startsWith("convex/")),
      ),
    ].sort(),
  };
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  process.stdout.write(JSON.stringify(auditSources(), null, 2) + "\n");
}
