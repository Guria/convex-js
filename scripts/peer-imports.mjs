// Reapply the public Convex imports after checking out upstream sources.
import fs from "fs";
import path from "path";
import process from "process";
import { fileURLToPath } from "url";
import ts from "typescript";

/** Replaces only module specifiers, leaving upstream code and comments intact. */
export function useConvexPeer(source, file, srcDir) {
  const tree = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true);
  const edits = [];
  const visit = (node) => {
    if (ts.isStringLiteralLike(node) && isModuleSpecifier(node)) {
      const peer = peerSpecifier(node.text, file, srcDir);
      if (peer)
        edits.push({ start: node.getStart(tree) + 1, end: node.end - 1, peer });
    }
    ts.forEachChild(node, visit);
  };
  visit(tree);
  for (const { start, end, peer } of edits.reverse()) {
    source = source.slice(0, start) + peer + source.slice(end);
  }
  return source;
}

function isModuleSpecifier(node) {
  const parent = node.parent;
  if (ts.isImportDeclaration(parent) || ts.isExportDeclaration(parent)) {
    return parent.moduleSpecifier === node;
  }
  if (ts.isLiteralTypeNode(parent)) return ts.isImportTypeNode(parent.parent);
  return (
    ts.isCallExpression(parent) &&
    parent.expression.kind === ts.SyntaxKind.ImportKeyword &&
    parent.arguments[0] === node
  );
}

function peerSpecifier(specifier, file, srcDir) {
  if (!specifier.startsWith(".")) return undefined;
  const relative = path.relative(
    srcDir,
    path.resolve(path.dirname(file), specifier),
  );
  if (relative === path.join("browser", "sync", "optimistic_updates.js")) {
    return "convex/browser";
  }
  const directory = relative.split(path.sep)[0];
  if (directory === "server" || directory === "values")
    return `convex/${directory}`;
  return undefined;
}

function rewriteDirectory(directory, srcDir) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) rewriteDirectory(file, srcDir);
    else if (/\.tsx?$/.test(entry.name)) {
      const source = fs.readFileSync(file, "utf8");
      const rewritten = useConvexPeer(source, file, srcDir);
      if (rewritten !== source) fs.writeFileSync(file, rewritten);
    }
  }
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const srcDir = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../src",
  );
  rewriteDirectory(srcDir, srcDir);
}
