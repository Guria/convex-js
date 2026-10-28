/**
 * Joins an auth library's session with the server's answer about its token,
 * like convex/react's `ConvexProviderWithAuth`: the library says who is signed
 * in, the client sends that session's token, and only a token the server
 * accepted counts as authenticated.
 */
import {
  atom,
  computed,
  effect,
  peek,
  withConnectHook,
  type AtomLike,
  type Computed,
} from "@reatom/core";
import type { AuthTokenFetcher } from "./auth.js";
import type { ReatomConvexClient } from "./client.js";

/**
 * The part of an auth library the client needs. `@reatomized/convex/convex-auth`
 * and `@reatomized/convex/better-auth` return one; for another library, build
 * it from that library's session.
 *
 * @public
 */
export type AuthProvider = {
  /** The library is still restoring or completing its session. */
  readonly isLoading: AtomLike<boolean, []>;
  /**
   * The signed-in session, `null` without one. Its value only needs to
   * identify the session: when it changes, for example to another user or
   * organization, the client sends a new token.
   */
  readonly session: AtomLike<string | null, []>;
  readonly fetchToken: AuthTokenFetcher;
};

/**
 * `rejected` means the library has a session but the server refused its
 * token, for example because convex/auth.config.ts doesn't list the issuer.
 *
 * @public
 */
export type AuthStatus =
  | "loading"
  | "unauthenticated"
  | "authenticated"
  | "rejected";

/** @public */
export type ReatomAuthState = {
  readonly status: Computed<AuthStatus>;
  readonly isLoading: Computed<boolean>;
  /** The server accepted the current session's token. */
  readonly isAuthenticated: Computed<boolean>;
  /** The server rejected an accepted token and a replacement is being fetched. */
  readonly isRefreshing: Computed<boolean>;
};

/**
 * Keeps the client's auth in step with `provider` while any of the returned
 * atoms has a subscriber. Pass an atom of providers to switch libraries at
 * runtime. Call `convex.setAuth` and `convex.clearAuth` only through this
 * state once it is in use.
 *
 * ```ts
 * const auth = reatomAuthState(convex, reatomConvexAuthProvider(convex));
 * const notes = computed(() =>
 *   auth.isAuthenticated() ? convex.query(api.notes.mine, {}).data() : [],
 * );
 * ```
 *
 * @public
 */
export function reatomAuthState(
  convex: ReatomConvexClient,
  provider: AuthProvider | AtomLike<AuthProvider, []>,
  name = `${convex.name}.authState`,
): ReatomAuthState {
  const current: () => AuthProvider =
    typeof provider === "function" ? provider : () => provider;
  // The provider and session whose token the client holds. Until it is the
  // current one, the server's answer is about an earlier session.
  const applied = atom<{ provider: AuthProvider; session: string } | null>(
    null,
    `${name}._applied`,
  );

  const sync = () =>
    effect(() => {
      const source = current();
      const session = source.isLoading() ? null : source.session();
      const last = peek(applied);
      if (last?.provider === source && last.session === session) return;
      if (session !== null) convex.setAuth(source.fetchToken);
      else if (last) convex.clearAuth();
      applied.set(session === null ? null : { provider: source, session });
    }, `${name}._sync`).unsubscribe;

  const status = computed((): AuthStatus => {
    const source = current();
    if (source.isLoading()) return "loading";
    const session = source.session();
    if (session === null) return "unauthenticated";
    const last = applied();
    if (last?.provider !== source || last.session !== session) return "loading";
    const accepted = convex.isAuthenticated();
    if (accepted === null) return "loading";
    return accepted ? "authenticated" : "rejected";
  }, `${name}.status`).extend(withConnectHook(sync));

  const isAuthenticated = computed(
    () => status() === "authenticated",
    `${name}.isAuthenticated`,
  );
  return {
    status,
    isLoading: computed(() => status() === "loading", `${name}.isLoading`),
    isAuthenticated,
    isRefreshing: computed(
      () => isAuthenticated() && convex.isRefreshingAuth(),
      `${name}.isRefreshing`,
    ),
  };
}
