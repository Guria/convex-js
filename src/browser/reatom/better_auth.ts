/**
 * The `@reatomized/convex/better-auth` entry: a better-auth session for
 * `reatomAuthState`, like `@convex-dev/better-auth/react`'s
 * `ConvexBetterAuthProvider`. Sign in and out through the better-auth client
 * itself; its session store tells this provider.
 */
import {
  action,
  atom,
  computed,
  peek,
  withAsync,
  withConnectHook,
  wrap,
  type Action,
  type AsyncExt,
  type AtomLike,
  type Computed,
} from "@reatom/core";
import type { AuthTokenFetcher } from "./auth.js";
import type { AuthProvider } from "./auth_state.js";

export type { AuthProvider } from "./auth_state.js";

type SessionState = {
  data: { session?: { id: string } | null } | null;
  isPending: boolean;
};
type SessionData<Client extends BetterAuthClient> = ReturnType<
  Client["useSession"]["get"]
>["data"];

/**
 * What the provider uses of a client from better-auth's `createAuthClient`
 * (`better-auth/client`) with `@convex-dev/better-auth`'s `convexClient()`
 * plugin, and `crossDomainClient()` when the Convex site is another origin.
 *
 * @public
 */
export type BetterAuthClient = {
  /** A nanostores store of the session. */
  useSession: {
    get(): SessionState;
    subscribe(listener: (value: SessionState) => void): () => void;
  };
  convex: {
    token(options: {
      fetchOptions: { throw: false };
    }): Promise<{ data?: { token?: string | null } | null }>;
  };
  getSession(options?: {
    fetchOptions?: { headers?: Record<string, string> };
  }): Promise<unknown>;
  crossDomain?: {
    oneTimeToken: {
      verify(args: { token: string }): Promise<{
        data?: { session?: { token: string } | null } | null;
        error?: { message?: string } | null;
      }>;
    };
  };
  updateSession?(): void;
};

/** @public */
export type BetterAuthOptions = {
  /** A Convex token from server rendering, used until the session loads. */
  initialToken?: string | null;
  /**
   * Whether an `ott` query param on start is a one-time token to sign in with;
   * `true` by default when the client has `crossDomainClient()`.
   */
  shouldHandleOneTimeToken?: boolean;
  /** Drops `ott` from the address; `history.replaceState` by default. */
  replaceURL?: (url: URL) => void | Promise<void>;
};

/** @public */
export type ReatomBetterAuth<Data = SessionState["data"]> = AuthProvider & {
  /** The session store's data: the session and user, `null` when signed out. */
  readonly data: Computed<Data>;
  /** The last Convex token, `null` without one. */
  readonly token: AtomLike<string | null, []>;
  /**
   * Trades the one-time token a cross-domain OAuth redirect comes back with
   * for the session. Runs on start for an `ott` in the URL; call it when the
   * app takes the token from the URL itself. Its `error` holds a failed trade.
   */
  readonly completeSignIn: Action<[oneTimeToken: string], Promise<void>> &
    AsyncExt<[oneTimeToken: string], void>;
};

/**
 * With `crossDomainClient()`, an OAuth sign-in comes back with a one-time
 * token in the `ott` query param; the provider trades it for the session.
 *
 * ```ts
 * const authClient = createAuthClient({
 *   baseURL: import.meta.env.VITE_CONVEX_SITE_URL,
 *   plugins: [convexClient(), crossDomainClient()],
 * });
 * const auth = reatomAuthState(convex, reatomBetterAuthProvider(authClient));
 * await authClient.signIn.email({ email, password });
 * ```
 *
 * @public
 */
export function reatomBetterAuthProvider<Client extends BetterAuthClient>(
  authClient: Client,
  options: BetterAuthOptions = {},
  name = "betterAuth",
): ReatomBetterAuth<SessionData<Client>> {
  const token = atom<string | null>(
    options.initialToken ?? null,
    `${name}.token`,
  );
  // Boxed, so the atom holds the request rather than its result.
  const pending = atom<{ request: Promise<string | null> } | null>(
    null,
    `${name}._pending`,
  );

  const state = atom(
    () =>
      authClient.useSession.get() as ReturnType<Client["useSession"]["get"]>,
    `${name}.sessionState`,
  ).extend(
    withConnectHook((target) => {
      void start();
      return authClient.useSession.subscribe(
        wrap((value: SessionState) => {
          target.set(value as ReturnType<Client["useSession"]["get"]>);
          if (!value.data && !value.isPending) token.set(null);
        }),
      );
    }),
  );

  const completeSignIn = action(async (oneTimeToken: string) => {
    if (!authClient.crossDomain)
      throw new Error("The better-auth client has no crossDomainClient()");
    const result = await wrap(
      authClient.crossDomain.oneTimeToken.verify({ token: oneTimeToken }),
    );
    const session = result.data?.session;
    if (!session)
      throw new Error(result.error?.message ?? "Invalid one-time token");
    await wrap(
      authClient.getSession({
        fetchOptions: { headers: { Authorization: `Bearer ${session.token}` } },
      }),
    );
    authClient.updateSession?.();
  }, `${name}.completeSignIn`).extend(withAsync());

  const started = atom(false, `${name}._started`);
  const start = action(async () => {
    if (peek(started)) return;
    started.set(true);
    const handle =
      options.shouldHandleOneTimeToken ?? authClient.crossDomain !== undefined;
    if (!handle || typeof location === "undefined") return;
    const url = new URL(location.href);
    const ott = url.searchParams.get("ott");
    if (!ott) return;
    url.searchParams.delete("ott");
    await wrap(
      (
        options.replaceURL ??
        ((next: URL) => history.replaceState({}, "", next))
      )(url),
    );
    try {
      await wrap(completeSignIn(ott));
    } catch {
      // Kept in `completeSignIn.error()`.
    }
  }, `${name}._start`);

  const fetchToken: AuthTokenFetcher = action(
    async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      const cached = token();
      if (cached && !forceRefreshToken) return cached;
      const inFlight = peek(pending);
      if (inFlight && !forceRefreshToken) return await wrap(inFlight.request);
      const request = authClient.convex
        .token({ fetchOptions: { throw: false } })
        .then(
          ({ data }) => data?.token || null,
          () => null,
        );
      const box = { request };
      pending.set(box);
      try {
        const value = await wrap(request);
        token.set(value);
        return value;
      } finally {
        if (peek(pending) === box) pending.set(null);
      }
    },
    `${name}.fetchToken`,
  );

  return {
    isLoading: computed(
      () => (state().isPending && token() === null) || !completeSignIn.ready(),
      `${name}.isLoading`,
    ),
    data: computed(() => state().data, `${name}.data`),
    // A token from server rendering stands in until the session loads.
    session: computed(() => {
      const id = state().data?.session?.id;
      if (id) return id;
      return token() === null ? null : "initialToken";
    }, `${name}.session`),
    fetchToken,
    token,
    completeSignIn,
  };
}
