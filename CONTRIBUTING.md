# The Reatom client

This branch adds a Convex sync client whose state lives in
[Reatom](https://github.com/reatom/reatom) v1001 atoms, published as
[`@reatomized/convex`](reatomized/README.md). The README there covers its API
and how it differs from `convex/browser`. Upstream's files are unchanged: the
client only adds `src/browser/reatom` and `reatomized/`.

The branch develops against the pkg.pr.new preview of Reatom's `v1001` branch at
`reatom/reatom@5f42184`, which includes the fix that lets Node exit with Reatom
loaded.

## Layout

| Path                                   | Holds                                                                                     |
| -------------------------------------- | ----------------------------------------------------------------------------------------- |
| `src/browser/reatom/index.ts`          | The package's exports                                                                     |
| `src/browser/reatom/socket.ts`         | WebSocket state machine, backoff, pauses for auth, inactivity, chunked transitions        |
| `src/browser/reatom/auth.ts`           | Token fetch, server confirmation, scheduled refetch, reauthentication                     |
| `src/browser/reatom/client.ts`         | Query entries, query set and identity versions, requests, optimistic updates, transitions |
| `src/browser/reatom/paginated.ts`      | Page lists derived from page query results, splits                                        |
| `src/browser/reatom/parity_*.ts`       | The parity tests' server and their `BaseConvexClient` and `PaginatedQueryClient` adapters |
| `reatomized/package.json`, `build.mjs` | The npm package and its build                                                             |

Handles nothing subscribes to (the socket, timers, the chunk buffer, indexes by
token and query id) live in a per-context runtime object. Query entries are held
weakly, so an unused entry can be collected.

The client reuses upstream's protocol types and helpers from `src/browser/sync`,
`src/common` and `src/vendor`. `reatomized/build.mjs` copies those into the
package and points imports of `src/values` and `src/server` at the `convex` peer
dependency. tsc then checks them against the public types of npm `convex` (the
`convex-upstream` dev dependency), so a build fails if the client starts using
something `convex` doesn't export.

## Tests

`src/browser/reatom/parity*.test.ts` run each scenario on upstream's
`BaseConvexClient` and `PaginatedQueryClient` from `src/browser/sync` and on the
Reatom client behind adapters with the same API, over one scripted server, and
expect the same trace: callbacks, query results, errors, promise outcomes,
connection states, and the messages the server receives. The server checks query
set and identity versions on every message; query sets are compared at
checkpoints, since the Reatom client batches them. The `deviations` tests pin
the differences the package README lists. After an upstream merge, the parity
tests compare against the merged code.

`src/browser/reatom/client.test.ts` covers the Reatom API.

```sh
npm install --legacy-peer-deps
npx vitest run src/browser/reatom
PARITY_TRACE=/tmp/trace.json npx vitest run -t "<scenario>"  # both traces
node reatomized/build.mjs
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

## Branches and releases

`main` mirrors [convex/convex-js](https://github.com/convex/convex-js) and holds
nothing else, so branches for upstream PRs start from it. `reatom` is upstream
plus this client.

To take new upstream changes, fast-forward `main` and merge it into `reatom`,
then run the tests:

```sh
git fetch upstream
git push origin upstream/main:main
git merge upstream/main
```

`@reatomized/convex` has its own semver in `reatomized/package.json`. Its
`convex` peer range starts at the upstream version whose sync protocol the
client speaks, the version in `src/index.ts`.

`.github/workflows/release.yml` runs on every push to `reatom`. When npm doesn't
have the version in `reatomized/package.json`, it tests, builds and publishes
the package through npm trusted publishing, then tags the commit `v<version>`.
