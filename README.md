# reatom-convex

The [`convex`](https://www.npmjs.com/package/convex) package with its sync
client rewritten on [Reatom](https://github.com/reatom/reatom) v1001. Queries,
connection and auth state are atoms, and `ConvexClient`, `BaseConvexClient` and
`convex/react` keep their API on top of them. See
[REATOM.md](https://github.com/Guria/reatom-convex/blob/reatom/REATOM.md) for
the Reatom API.

Install it under the `convex` name, so `convex/*` imports and packages that
depend on `convex` get this build:

```sh
npm install convex@npm:reatom-convex @reatom/core
```

`@reatom/core` is a peer dependency, so the client's atoms live in the app's
Reatom context.

reatom-convex X.Y.N is built on convex X.Y.⌊N/100⌋: 1.48.0 to 1.48.99 are on
convex 1.48.0, and 1.48.100 is the first on convex 1.48.1.

With `@reatom/core@1001.3.0` loaded, Node never exits on its own, so a script
that imports `convex/browser` or `convex/react` hangs after its work is done.
Browsers, servers, Convex functions and the `convex` CLI are not affected. The
fix is on Reatom's `v1001` branch; install that build in projects with such
scripts (npm 12 installs from a URL only with `--allow-remote`):

```sh
npm install --allow-remote=root @reatom/core@https://pkg.pr.new/@reatom/core@5f42184
```

The rest of this README is upstream's.

# Convex

TypeScript backend SDK, client libraries, and CLI for Convex.

Convex is the backend application platform with everything you need to build
your product.

Get started at [docs.convex.dev](https://docs.convex.dev)!

Or see [Convex demos](https://github.com/convex/convex-demos).

Open discussions and issues in this repository about Convex
TypeScript/JavaScript clients, the Convex CLI, or the Convex platform in
general.

Also feel free to share feature requests, product feedback, or general questions
in the [Convex Discord Community](https://convex.dev/community).

# Structure

This package includes several entry points for building apps on Convex:

- [`convex/server`](https://docs.convex.dev/api/modules/server): SDK for
  defining a Convex backend functions, defining a database schema, etc.
- [`convex/react`](https://docs.convex.dev/api/modules/react): Hooks and a
  `ConvexReactClient` for integrating Convex into React applications.
- [`convex/browser`](https://docs.convex.dev/api/modules/browser): A
  `ConvexHttpClient` for using Convex in other browser environments.
- [`convex/values`](https://docs.convex.dev/api/modules/values): Utilities for
  working with values stored in Convex.
- [`convex/react-auth0`](https://docs.convex.dev/api/modules/react_auth0): A
  React component for authenticating users with Auth0.
- [`convex/react-clerk`](https://docs.convex.dev/api/modules/react_clerk): A
  React component for authenticating users with Clerk.
- [`convex/nextjs`](https://docs.convex.dev/api/modules/nextjs): Server-side
  helpers for SSR, usable by Next.js and other React frameworks.

This package also includes [`convex`](https://docs.convex.dev/using/cli), the
command-line interface for managing Convex projects.
