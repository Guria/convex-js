# Contributing

The `reatom` branch holds `@reatomized/convex` and the
[convex-js](https://github.com/convex/convex-js) sources it imports. Everything
under `src/` outside `src/browser/reatom` is upstream's code, copied verbatim
from `upstream/main`.

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
| `src/values`, `src/server`                     | Upstream's values and function references, which the package takes from `convex`          |
| `scripts/build.mjs`                            | The package build                                                                         |

The package doesn't ship `src/values` or `src/server`: the build points their
imports at the `convex` peer. They stay because the client and upstream's sync
code import them by relative path, which tsc and the tests resolve, and
upstream's client in the parity tests needs most of `src/server`.

Handles nothing subscribes to (the socket, timers, the chunk buffer, indexes by
token and query id) live in a per-context runtime object. Query entries are held
weakly, so an unused entry can be collected.

## Build

`scripts/build.mjs` copies `src/browser/reatom/index.ts` and every module it
reaches into `staged/`, then compiles that to `dist/` with
`tsconfig.build.json`. Imports of `src/values` and `src/server` become imports
of `convex/values` and `convex/server`, the peer dependency, so the client
shares `ConvexError` and function references with the app's `convex`. tsc checks
those imports against the public types of the `convex` dev dependency, so the
build fails if the client starts using something `convex` doesn't export.

## Tests

`src/browser/reatom/parity*.test.ts` run each scenario on upstream's
`BaseConvexClient` and `PaginatedQueryClient` from `src/browser/sync` and on the
Reatom client behind adapters with the same API, over one scripted server, and
expect the same trace: callbacks, query results, errors, promise outcomes,
connection states, and the messages the server receives. The server checks query
set and identity versions on every message; query sets are compared at
checkpoints, since the Reatom client batches them. The `deviations` tests pin
the differences the README lists.

`src/browser/reatom/client.test.ts` covers the Reatom API.

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
npm run typecheck && npm test && npm run build
```

When an upstream file starts importing one the branch doesn't have, typecheck
names it; check it out from `upstream/main` the same way. When upstream deletes
or renames a file, `git checkout` stops at that path.

The client speaks the sync protocol of the version in `src/index.ts`. When a
sync with upstream changes that version, raise the `convex` peer range and dev
dependency in `package.json` to it.

## Releases

`@reatomized/convex` has its own semver in `package.json`.

`.github/workflows/release.yml` runs on every push to `reatom`. When npm doesn't
have the version in `package.json`, it tests, builds and publishes the package
through npm trusted publishing, then tags the commit `v<version>`.
