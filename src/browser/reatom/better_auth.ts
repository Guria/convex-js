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
  withConnectHook,
  wrap,
  type AtomLike,
} from "@reatom/core";
import type { AuthTokenFetcher } from "./auth.js";
import type { AuthProvider } from "./auth_state.js";

export type { AuthProvider } from "./auth_state.js";

type SessionState = {
  data: { session?: { id: string } | null } | null;
  isPending: boolean;
};

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
      verify(args: {
        token: string;
      }): Promise<{ data?: { session?: { token: string } | null } | null }>;
    };
  };
  updateSession?(): void;
};

/** @public */
export type BetterAuthOptions = {
  /** A Convex token from server rendering, used until the session loads. */
  initialToken?: string | null;
  /** Drops `ott` from the address; `history.replaceState` by default. */
  replaceURL?: (url: URL) => void | Promise<void>;
};

/** @public */
export type ReatomBetterAuth = AuthProvider & {
  /** The last Convex token, `null` without one. */
  readonly token: AtomLike<string | null, []>;
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
export function reatomBetterAuthProvider(
  authClient: BetterAuthClient,
  options: BetterAuthOptions = {},
  name = "betterAuth",
): ReatomBetterAuth {
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
    () => authClient.useSession.get(),
    `${name}.sessionState`,
  ).extend(
    withConnectHook((target) => {
      void verifyOneTimeToken();
      return authClient.useSession.subscribe(
        wrap((value: SessionState) => {
          target.set(value);
          if (!value.data && !value.isPending) token.set(null);
        }),
      );
    }),
  );

  const verifyOneTimeToken = action(async () => {
    if (typeof location === "undefined" || !authClient.crossDomain) return;
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
    const result = await wrap(
      authClient.crossDomain.oneTimeToken.verify({ token: ott }),
    );
    const session = result.data?.session;
    if (!session) return;
    await wrap(
      authClient.getSession({
        fetchOptions: { headers: { Authorization: `Bearer ${session.token}` } },
      }),
    );
    authClient.updateSession?.();
  }, `${name}._verifyOneTimeToken`);

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
      () => state().isPending && token() === null,
      `${name}.isLoading`,
    ),
    // A token from server rendering stands in until the session loads.
    session: computed(() => {
      const id = state().data?.session?.id;
      if (id) return id;
      return token() === null ? null : "initialToken";
    }, `${name}.session`),
    fetchToken,
    token,
  };
}
