import path from "path";
import { expect, test } from "vitest";
import { useConvexPeer } from "./peer-imports.mjs";

const srcDir = path.resolve("src");
const file = path.join(srcDir, "browser/reatom/client.ts");
const rewrite = (source) => useConvexPeer(source, file, srcDir);

test("uses the public peer for imports, exports and import types", () => {
  const source = `import { getFunctionName } from "../../server/api.js";
import type { Value } from '../../values/value.js';
export type { UserIdentityAttributes } from "../../server/authentication.js";
type Args = import("../../server/api.js").FunctionArgs<Q>;
const values = import("../../values/index.js");
`;
  expect(rewrite(source)).toBe(`import { getFunctionName } from "convex/server";
import type { Value } from 'convex/values';
export type { UserIdentityAttributes } from "convex/server";
type Args = import("convex/server").FunctionArgs<Q>;
const values = import("convex/values");
`);
});

test("leaves comments, strings and other module paths unchanged", () => {
  const source = `// import { Value } from "../../values/index.js";
const example = 'from "../../server/api.js"';
import { Long } from "../../vendor/long.js";
import { result } from "./server/index.js";
import { ConvexError } from "convex/values";
import { type } from "../../../server/index.js";
`;
  expect(rewrite(source)).toBe(source);
});

test("is idempotent and supports different source depths", () => {
  const source = 'import { Value } from "../values/value.js";';
  const file = path.join(srcDir, "common/index.ts");
  const output = useConvexPeer(source, file, srcDir);
  expect(output).toBe('import { Value } from "convex/values";');
  expect(useConvexPeer(output, file, srcDir)).toBe(output);
});

test("uses the public browser types for optimistic updates", () => {
  expect(
    rewrite(
      'import type { OptimisticUpdate } from "../sync/optimistic_updates.js";',
    ),
  ).toBe('import type { OptimisticUpdate } from "convex/browser";');
  expect(
    rewrite(
      'export type { OptimisticLocalStore } from "../sync/optimistic_updates.js";',
    ),
  ).toBe('export type { OptimisticLocalStore } from "convex/browser";');
});
