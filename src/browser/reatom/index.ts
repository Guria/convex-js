// The entry point of the @reatomized/convex package, built by scripts/build.mjs.
export { reatomConvexClient } from "./client.js";
export type {
  BaseConvexClientOptions as ReatomConvexClientOptions,
  ConnectionState,
  MutationOptions,
  QueryModification,
  QueryStatus,
  ReatomConvexClient,
  ReatomQuery,
  ReatomQueryOptions,
  Transition,
} from "./client.js";
export type { AuthTokenFetcher } from "./auth.js";
export { reatomAuthState } from "./auth_state.js";
export type {
  AuthProvider,
  AuthStatus,
  ReatomAuthState,
} from "./auth_state.js";
export type { PaginatedQueryState, ReatomPaginatedQuery } from "./paginated.js";
export type { SocketState } from "./socket.js";
export type { OptimisticLocalStore } from "convex/browser";
