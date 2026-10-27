import fs from "fs";
import { expect, test } from "vitest";
import { auditSources } from "./source-audit.mjs";

const audit = auditSources();

test("every remaining source is used by the package or the parity tests", () => {
  expect(audit.unused).toEqual([]);
  expect(audit.runtime).toContain("src/browser/reatom/client.ts");
});

test("reference clients and their managers are test-only", () => {
  expect(audit.runtime.filter((file) => /parity|\.test\./.test(file))).toEqual(
    [],
  );
  for (const name of [
    "client",
    "paginated_query_client",
    "authentication_manager",
    "local_state",
    "request_manager",
    "remote_query_set",
    "web_socket_manager",
    "optimistic_updates_impl",
  ]) {
    const file = `src/browser/sync/${name}.ts`;
    expect(audit.runtime).not.toContain(file);
    expect(audit.testOnly).toContain(file);
  }
});

test("server and values come from public peer exports, not copies or deep imports", () => {
  expect(fs.existsSync("src/server")).toBe(false);
  expect(fs.existsSync("src/values")).toBe(false);
  expect(fs.existsSync("src/type_utils.ts")).toBe(false);
  expect(audit.peers).toEqual([
    "convex/browser",
    "convex/server",
    "convex/values",
  ]);
});

test("the production build starts at the package entry, not the test tree", () => {
  const config = JSON.parse(fs.readFileSync("tsconfig.build.json", "utf8"));
  expect(config.files).toEqual(["src/browser/reatom/index.ts"]);
  expect(config.include).toBeUndefined();
});
