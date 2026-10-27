# Contributing

The `reatom` branch holds `@reatomized/convex` and the
[convex-js](https://github.com/convex/convex-js) sources it imports. Files under
`src/` outside `src/browser/reatom` come from `upstream/main`. Their
implementation stays upstream's; imports of its server and values modules use
the public `convex/server` and `convex/values` peer entry points.

The branch develops against the pkg.pr.new preview of Reatom's `v1001` branch at
`reatom/reatom@5f42184`, which includes the fix that lets Node exit with Reatom
loaded.

```sh
npm install --allow-remote=root
npm run lint
npm run typecheck
npm test
npm run build
mise exec dprint@0.51.1 -- dprint check
```

`check.yml` runs the same on pull requests and pushes to `reatom`.

## Layout

| Path                                           | Holds                                                                                     |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `src/browser/reatom/index.ts`                  | The package's exports                                                                     |
| `src/browser/reatom/socket.ts`                 | WebSocket state machine, backoff, pauses for auth, inactivity, chunked transitions        |
| `src/browser/reatom/auth.ts`                   | Token fetch, server confirmation, scheduled refetch, reauthentication                     |
| `src/browser/reatom/client.ts`                 | Query entries, query set and identity versions, requests, optimistic updates, transitions |
| `src/browser/reatom/paginated.ts`              | Page lists derived from page query results, splits                                        |
| `src/browser/reatom/parity_*.ts`               | The parity tests' server and their `BaseConvexClient` and `PaginatedQueryClient` adapters |
| `src/browser/sync`, `src/common`, `src/vendor` | Upstream's protocol types and helpers the client reuses, and upstream's client            |
| `scripts/build.mjs`                            | The package build                                                                         |
| `scripts/source-audit.mjs`                     | Lists package, test-only and unused sources                                               |
| `scripts/peer-imports.mjs`                     | Reapplies public Convex imports after an upstream checkout                                |

`convex` is both a peer dependency and a pinned dev dependency. Source, tests
and emitted declarations import its public `convex/server` and `convex/values`
exports directly. There are no local server or values copies, deep imports into
`convex`, or build-time import rewrites. `OptimisticLocalStore` and
`OptimisticUpdate` also come directly from the public `convex/browser` types;
those type imports do not load its WebSocket client at runtime. This also keeps
`ConvexError` identity the same in tests, the package and its consumers.

Handles nothing subscribes to (the socket, timers, the chunk buffer, indexes by
token and query id) live in a per-context runtime object. Query entries are held
weakly, so an unused entry can be collected.

## Build

`tsconfig.build.json` names `src/browser/reatom/index.ts` as its only entry.
TypeScript follows the modules it imports and writes JavaScript and declarations
to `dist/`; tests and the upstream reference client are not build entries.
`scripts/build.mjs` clears `dist/`, runs that compilation, and copies vendored
licenses next to their emitted modules. There is no staging directory.

Typecheck and build resolve public Convex imports against the pinned dev
version. They fail if this client or an upstream helper imports a symbol the
installed `convex` package doesn't export.

## Tests

`src/browser/reatom/parity*.test.ts` run each scenario on upstream's
`BaseConvexClient` and `PaginatedQueryClient` from `src/browser/sync` and on the
Reatom client behind adapters with the same API, over one scripted server, and
expect the same trace: callbacks, query results, errors, promise outcomes,
connection states, and the messages the server receives. The server checks query
set and identity versions on every message; query sets are compared at
checkpoints, since the Reatom client batches them. The `deviations` tests pin
the differences the README lists.

`src/browser/reatom/client.test.ts` covers the Reatom API. Its function
references and `ConvexError` assertions come from the public `convex` package.
`node scripts/source-audit.mjs` lists the package's import graph, test-only
sources, and any unused sources. Its tests require no unused files and keep the
upstream reference clients and their managers out of the package graph.

The test-only copies of `BaseConvexClient`, `PaginatedQueryClient`, and the
managers they import remain to run the parity scenarios against upstream's
implementation. Those scenarios inspect private methods and transitions, and
`PaginatedQueryClient` has no public `convex` export. Removing these files would
require changing the tests or depending on private package paths. They are not
shipped. The native parity adapters and scripted server are also test-only.

`scripts/peer-imports.test.mjs` checks the upstream import rewrite, including
its idempotence and leaving comments and ordinary strings untouched.

To see both traces of a scenario:

```sh
PARITY_TRACE=/tmp/trace.json npx vitest run -t "<scenario>"
```

## Performance

Measured in the
[reatom-convex-auth bench](https://github.com/Guria/reatom-convex-auth/blob/exp/reatom-native-convex-client/packages/bench/RESULTS_ARTIFACTS.md)
at `a5c9bed`, 1000 components each with its own query, Chromium, medians:

| Scenario   | convex@1.48.0, convex/react | Reatom client, @reatom/jsx |
| ---------- | --------------------------: | -------------------------: |
| mount      |                     65.5 ms |                    9.24 ms |
| unmount    |                     9.87 ms |                    1.99 ms |
| update all |                     4.12 ms |                    3.15 ms |

## Syncing with upstream

`main` mirrors convex-js and holds nothing else, so branches for upstream PRs
start from it. To take upstream changes into `reatom`, update `main`, check out
upstream's version of every upstream file on `reatom`, and run the checks:

```sh
git fetch upstream
git push origin upstream/main:main
git checkout upstream/main -- $(git ls-files src ':!src/browser/reatom')
node scripts/peer-imports.mjs
npm run typecheck && npm test && npm run build
```

`peer-imports.mjs` changes only import and export specifiers that point at
upstream's `src/server` or `src/values`, including import types and dynamic
imports. It also replaces the upstream optimistic-update types with the public
`convex/browser` type exports. It leaves implementations, comments and other
paths unchanged, and running it twice makes no further edits.

When an upstream file starts importing a missing sync helper, typecheck names
it; check it out from `upstream/main` and re-run `peer-imports.mjs`. Do not
restore `src/server` or `src/values`. If a new import needs a non-public export,
review that dependency rather than adding a deep package import. When upstream
deletes or renames a file, `git checkout` stops at that path.

The client speaks the sync protocol of the version in `src/index.ts`. When a
sync with upstream changes that version, raise the `convex` peer range and dev
dependency in `package.json` to it.

## Releases

`@reatomized/convex` has its own semver in `package.json`.

`.github/workflows/release.yml` runs on every push to `reatom`. When npm doesn't
have the version in `package.json`, it tests, builds and publishes the package
through npm trusted publishing, then tags the commit `v<version>`.
