# convex/browser on Reatom

On this branch the sync client keeps its state in
[Reatom](https://github.com/reatom/reatom) v1001 atoms. `BaseConvexClient`,
`ConvexClient`, `PaginatedQueryClient` and everything in `convex/react` keep
their API and run on top of it. Reatom apps can use the atoms directly.

`@reatom/core` comes from the pkg.pr.new preview of the `v1001` branch at
`reatom/reatom@5f42184`, which includes the fix that lets Node exit with Reatom
loaded. The CLI runs `BaseConvexClient` in Node.

## The Reatom API

```ts
import { reatomConvexClient } from "convex/browser";
import { atom, computed } from "@reatom/core";

const convex = reatomConvexClient(import.meta.env.VITE_CONVEX_URL);

const noteId = atom<Id<"notes">>(initialId);
const note = computed(() =>
  convex.query(api.notes.get, { id: noteId() }).data(),
);
```

- `convex.query(ref, args)` returns the entry for that function and arguments,
  the same object for equal arguments. Its `result`, `data`, `error` and
  `status` are atoms. Reading one in a subscribed graph subscribes the query on
  the server; once nothing reads it, the query is unsubscribed.
- Query set changes made in one microtask go out as one `ModifyQuerySet`. In the
  example above, changing `noteId` sends one message that adds the new query and
  removes the old one.
- `convex.paginatedQuery(ref, args, { initialNumItems })` has `results`,
  `status` and `error` atoms and a `loadMore` action. Equal arguments share
  their pages; pass `id` for a separate page list.
- `convex.mutation(ref, args, { optimisticUpdate })` and
  `convex.action(ref, args)` return promises and run as Reatom actions.
- `connectionState`, `isAuthenticated` (`null` while a token waits for the
  server), `isRefreshingAuth`, `authClaims` and `socketState` are atoms.
  `setAuth`, `clearAuth` and `close` are actions.

State is per Reatom context: call the client inside a frame, and each context
gets its own connection. The socket opens on first use in a context.

To use `convex/react` hooks over the same connection:

```ts
const react = new ConvexReactClient(url, {
  baseClient: BaseConvexClient.fromReatom(convex),
});
```

`BaseConvexClient.fromReatom` must run inside a frame of the context the client
lives in. A `BaseConvexClient` built with `new` runs in the context current at
construction, or in a context of its own after `clearStack()`; `client.reatom`
and `client.reatomFrame` expose them.

## Layout

| Module                                       | Holds                                                                                     |
| -------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `src/browser/reatom/socket.ts`               | WebSocket state machine, backoff, pauses for auth, inactivity, chunked transitions        |
| `src/browser/reatom/auth.ts`                 | Token fetch, server confirmation, scheduled refetch, reauthentication                     |
| `src/browser/reatom/client.ts`               | Query entries, query set and identity versions, requests, optimistic updates, transitions |
| `src/browser/reatom/paginated.ts`            | Page lists derived from page query results, splits                                        |
| `src/browser/sync/client.ts`                 | `BaseConvexClient` over the Reatom client                                                 |
| `src/browser/sync/paginated_query_client.ts` | `PaginatedQueryClient` over the Reatom paginated queries                                  |

Handles nothing subscribes to (the socket, timers, the chunk buffer, indexes by
token and query id) live in a per-context runtime object. Query entries are held
weakly, so an unused entry can be collected.

## Where it differs from convex@1.48.0

- Query set changes are batched per microtask instead of sent one per subscribe
  and unsubscribe.
- A transition updates the queries it names. Optimistic updates replay only
  while some are pending. convex@1.48.0 rebuilds and diffs every result on each
  transition, which makes mounting N queries cost O(N²).
- `clearAuth` stops the auth flow. In convex@1.48.0 a refetch scheduled before
  `clearAuth` still runs and can authenticate the old session again.
- A failed page of a paginated query reaches `ConvexReactClient` watchers. In
  convex@1.48.0 it throws out of the WebSocket message handler before any
  watcher runs.
- Server results stay visible across a reconnect until the new connection
  answers.
- The minified `convex/browser` entry is 25.3 KiB gzip against 19.9 KiB. The
  Reatom functions the client uses are 5.2 KiB of that, shared with an app that
  already uses Reatom.

## Performance

Measured in the
[reatom-convex-auth bench](https://github.com/Guria/reatom-convex-auth/blob/exp/reatom-native-convex-client/packages/bench/RESULTS_ARTIFACTS.md)
at `a5c9bed`, 1000 components each with its own query, Chromium, medians:

| Scenario   | convex@1.48.0, convex/react | this branch, convex/react | this branch's atoms, @reatom/jsx |
| ---------- | --------------------------: | ------------------------: | -------------------------------: |
| mount      |                     65.5 ms |                   14.4 ms |                          9.24 ms |
| unmount    |                     9.87 ms |                   2.91 ms |                          1.99 ms |
| update all |                     4.12 ms |                   6.15 ms |                          3.15 ms |

Through `BaseConvexClient` each query is an atom, so convex/react pays an atom
per subscription and a write per update: updating 1000 queries at once is slower
than in convex@1.48.0. Read directly, the atoms mount faster than any other
column in that bench and update as fast as its native Reatom client.

## Tests

`src/browser/reatom/parity*.test.ts` and `src/react/parity_hooks.test.tsx` run
each scenario on npm `convex@1.48.0` (the `convex-upstream` dev dependency) and
on this package, over one scripted server, and expect the same trace: callbacks,
query results, errors, promise outcomes, connection states, rendered text, and
the messages the server receives. The server checks query set and identity
versions on every message; query sets are compared at checkpoints, since they
are batched here. The `deviations` tests pin the differences listed above.

```sh
npm install --legacy-peer-deps
npx vitest run
PARITY_TRACE=/tmp/trace.json npx vitest run -t "<scenario>"  # both traces
```

`src/browser/reatom/client.test.ts` covers the Reatom API.
