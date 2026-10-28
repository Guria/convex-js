# @reatomized/convex

A [Convex](https://convex.dev) sync client whose state lives in
[Reatom](https://github.com/reatom/reatom) v1001 atoms: queries, paginated
queries, the connection and auth. It replaces `convex/browser`'s WebSocket
client and nothing else, so keep the `convex` package for your functions, the
CLI and codegen.

```sh
npm install @reatomized/convex convex @reatom/core
```

`convex` and `@reatom/core` are peer dependencies. The client takes function
references and `ConvexError` from your `convex`, and its atoms live in your
app's Reatom context. It speaks the sync protocol of convex 1.48.0.

## Usage

```ts
import { reatomConvexClient } from "@reatomized/convex";
import { atom, computed } from "@reatom/core";
import { api } from "../convex/_generated/api";
import type { Id } from "../convex/_generated/dataModel";

const convex = reatomConvexClient(import.meta.env.VITE_CONVEX_URL);

const noteId = atom<Id<"notes"> | null>(null);
const note = computed(() => {
  const id = noteId();
  return id ? convex.query(api.notes.get, { id }).data() : undefined;
});
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
  `convex.action(ref, args)` return promises and run as Reatom actions. A failed
  call rejects with a `ConvexError` from `convex/values` when the function threw
  one.
- `connectionState`, `isAuthenticated` (`null` while a token waits for the
  server), `isRefreshingAuth`, `authClaims` and `socketState` are atoms.
  `setAuth`, `clearAuth` and `close` are actions.

State is per Reatom context: call the client inside a frame, and each context
gets its own connection. The socket opens on first use in a context.

There are no React hooks. Read the atoms through your Reatom view binding, such
as `@reatom/react` or `@reatom/jsx`.

## Auth

`reatomAuthState(convex, provider)` does what convex/react's
`ConvexProviderWithAuth` and `useConvexAuth` do. An auth library reports whether
it is loading and who is signed in, `reatomAuthState` sends that session's
token, and its `status` turns `authenticated` once the server accepts the token.
`status` is `loading`, `unauthenticated`, `authenticated` or `rejected`;
`isLoading`, `isAuthenticated` and `isRefreshing` are atoms too. The client
follows the provider while one of these atoms has a subscriber, so subscribe to
one at the root of the app.

Two entry points build the provider from an auth library:

```ts
// Convex Auth (@convex-dev/auth)
import { reatomConvexAuthProvider } from "@reatomized/convex/convex-auth";

const convexAuth = reatomConvexAuthProvider(convex);
const auth = reatomAuthState(convex, convexAuth);
await convexAuth.signIn("password", { email, password, flow: "signIn" });
await convexAuth.signIn("github"); // navigates to GitHub
await convexAuth.signOut();
```

`reatomConvexAuthProvider` implements the client side of `@convex-dev/auth`,
whose own client is a React provider. It uses the same storage keys, so sessions
carry over from `@convex-dev/auth/react`. On start it signs in with a `code`
from the URL, and it refreshes the token once across tabs.

```ts
// Better Auth with @convex-dev/better-auth
import { reatomBetterAuthProvider } from "@reatomized/convex/better-auth";
import {
  convexClient,
  crossDomainClient,
} from "@convex-dev/better-auth/client/plugins";
import { createAuthClient } from "better-auth/client";

const authClient = createAuthClient({
  baseURL: import.meta.env.VITE_CONVEX_SITE_URL,
  plugins: [convexClient(), crossDomainClient()],
});
const auth = reatomAuthState(convex, reatomBetterAuthProvider(authClient));
await authClient.signIn.email({ email, password });
```

Better Auth's client works without a framework, so you sign in through it;
`reatomBetterAuthProvider` follows its session store, fetches Convex tokens, and
trades the `ott` one-time token from a cross-domain OAuth redirect for the
session. Neither entry point adds dependencies.

For another library, pass any object with `isLoading` and `session` atoms and a
`fetchToken` function. `session` should change when the identity does, such as a
user or organization id. Pass an atom of providers to switch libraries at
runtime.

## Node

With `@reatom/core@1001.3.0` loaded, Node never exits on its own, so a script
that uses this client hangs after its work is done. The fix is on Reatom's
`v1001` branch; install that build in projects with such scripts (npm 12
installs from a URL only with `--allow-remote`):

```sh
npm install --allow-remote=root @reatom/core@https://pkg.pr.new/@reatom/core@5f42184
```

Node 22 and later have a global `WebSocket`. On Node 20, pass one, such as the
`ws` package's, as `webSocketConstructor`.

## Differences from convex/browser 1.48.0

- Query set changes are batched per microtask instead of sent one per subscribe
  and unsubscribe.
- A transition updates the queries it names. Optimistic updates replay only
  while some are pending. convex/browser rebuilds and diffs every result on each
  transition, which makes mounting N queries cost O(N²).
- `clearAuth` stops the auth flow. In convex/browser a refetch scheduled before
  `clearAuth` still runs and can authenticate the old session again.
- When a token fetch returns `null` for a signed-in client, the client sends the
  server `Authenticate` without a token. convex/browser drops that message while
  the socket is paused for the fetch, so the server keeps the old identity.
- With `expectAuth`, a socket that opened before the first token sends `Connect`
  once the token arrives. convex/browser never sends it, and `hasEverConnected`
  stays false.
- A failed page of a paginated query lands in its `error` atom. In
  convex/browser it throws out of the WebSocket message handler.
- Server results stay visible across a reconnect until the new connection
  answers.
- Minified, the client is 13.8 KiB gzip, or 19.0 KiB with the parts of
  `convex/values` and `convex/server` it uses, against 15.8 KiB for
  convex/browser's `BaseConvexClient`. The Reatom functions it uses add 4.6 KiB,
  shared with the rest of the app.

With 1000 components each running its own query in Chromium, mounting takes 9.24
ms against 65.5 ms for convex/react, unmounting 1.99 ms against 9.87 ms.
[CONTRIBUTING.md](https://github.com/reatomized/convex/blob/reatom/CONTRIBUTING.md#performance)
has the full table and how it was measured.

## Development

The repository is a fork of
[convex/convex-js](https://github.com/convex/convex-js) cut down to this client
and the upstream sync helpers it reuses. Server and value utilities are imported
directly from the official `convex` peer in both source and builds.
[CONTRIBUTING.md](https://github.com/reatomized/convex/blob/reatom/CONTRIBUTING.md)
covers the layout, the parity tests against upstream's client, syncing with
upstream, and releases.

Apache-2.0, like convex-js.
