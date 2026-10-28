/**
 * The `@reatomized/convex/convex-auth` entry: the client side of Convex Auth
 * (`@convex-dev/auth`), whose own client ships only as a React provider.
 *
 * The `auth:signIn` action returns a JWT and a refresh token, trades a refresh
 * token for a new pair, and for OAuth returns the provider's URL and a
 * verifier; the backend sends the browser back with a `code` that, with the
 * verifier, signs in. `auth:signOut` ends the server session. Storage keys and
 * formats are `@convex-dev/auth/react`'s, so its sessions carry over.
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
} from "@reatom/core";
import { ConvexHttpClient } from "convex/browser";
import { makeFunctionReference } from "convex/server";
import type { Value } from "convex/values";
import { jwtDecode } from "../../vendor/jwt-decode/index.js";
import type { AuthTokenFetcher } from "./auth.js";
import type { AuthProvider } from "./auth_state.js";
import type { ReatomConvexClient } from "./client.js";

export type { AuthProvider } from "./auth_state.js";

const JWT_KEY = "__convexAuthJWT";
const REFRESH_TOKEN_KEY = "__convexAuthRefreshToken";
const VERIFIER_KEY = "__convexAuthOAuthVerifier";

// Retry delays for a code or refresh token exchange that hits a network error.
const RETRY_BACKOFF = [500, 2000];
const RETRY_JITTER = 100;

type Tokens = { token: string; refreshToken: string };
type SignInResult = {
  tokens?: Tokens | null;
  redirect?: string;
  verifier?: string;
};
const signInRef = makeFunctionReference<
  "action",
  Record<string, Value | undefined>,
  SignInResult
>("auth:signIn");
const signOutRef = makeFunctionReference<"action", Record<string, never>>(
  "auth:signOut",
);

/**
 * `localStorage`, `sessionStorage`, or an async store such as React Native's
 * AsyncStorage.
 *
 * @public
 */
export type ConvexAuthStorage = {
  getItem(
    key: string,
  ): string | null | undefined | Promise<string | null | undefined>;
  setItem(key: string, value: string): void | Promise<void>;
  removeItem(key: string): void | Promise<void>;
};

/** @public */
export type ConvexAuthOptions = {
  /** `localStorage` by default. */
  storage?: ConvexAuthStorage;
  /** Separates the keys of several deployments; the client's URL by default. */
  storageNamespace?: string;
  /**
   * Whether a `code` query param on start is a Convex Auth sign-in code;
   * `true` by default. Turn it off for pages where another library uses `code`.
   */
  shouldHandleCode?: boolean | (() => boolean);
  /** Drops `code` from the address; `history.replaceState` by default. */
  replaceURL?: (relativeUrl: string) => void | Promise<void>;
  /** Sends the browser to an OAuth provider; `location.assign` by default. */
  navigate?: (url: URL) => void | Promise<void>;
};

/** @public */
export type ConvexAuthSignInResult = {
  /** Tokens were issued and the session is starting. */
  signingIn: boolean;
  /** The OAuth provider's URL, after navigating to it. */
  redirect?: URL;
};

/** @public */
export type ReatomConvexAuth = AuthProvider & {
  /** The current JWT, `null` when signed out. */
  readonly token: AtomLike<string | null, []>;
  /** A refresh token is being exchanged. */
  readonly isRefreshingToken: AtomLike<boolean, []>;
  /**
   * Calls `auth:signIn`. `provider` is a provider id from convex/auth.ts, such
   * as `"password"` or `"github"`; `params` are its fields, such as `email`,
   * `password` and `flow`.
   */
  readonly signIn: Action<
    [
      provider?: string | undefined,
      params?: FormData | Record<string, Value> | undefined,
    ],
    Promise<ConvexAuthSignInResult>
  >;
  /**
   * Signs in with the `code` the backend sends the browser back with after
   * OAuth; `true` when tokens were issued. Runs on start for a `code` in the
   * URL; call it when the app takes the code from the URL itself. Its `error`
   * holds a failed exchange.
   */
  readonly completeSignIn: Action<[code: string], Promise<boolean>> &
    AsyncExt<[code: string], boolean>;
  /** Ends the session locally, even when `auth:signOut` fails. */
  readonly signOut: Action<[], Promise<void>>;
};

/**
 * A Convex Auth session for `reatomAuthState`. It restores the stored
 * session, signs in with a `code` in the URL, follows other tabs, and runs one
 * refresh token exchange at a time across tabs.
 *
 * ```ts
 * const convexAuth = reatomConvexAuthProvider(convex);
 * const auth = reatomAuthState(convex, convexAuth);
 * await convexAuth.signIn("password", { email, password, flow: "signIn" });
 * ```
 *
 * @public
 */
export function reatomConvexAuthProvider(
  convex: ReatomConvexClient,
  options: ConvexAuthOptions = {},
  name = `${convex.name}.convexAuth`,
): ReatomConvexAuth {
  const namespace = (options.storageNamespace ?? convex.url).replace(
    /[^a-zA-Z0-9]/g,
    "",
  );
  const key = (base: string) => `${base}_${namespace}`;
  const storage = () => {
    const value =
      options.storage ??
      (typeof localStorage === "undefined" ? undefined : localStorage);
    if (!value)
      throw new Error(
        "`localStorage` is not available, pass `storage` to reatomConvexAuthProvider",
      );
    return value;
  };
  const read = (base: string) => wrap(storage().getItem(key(base)));
  const write = (base: string, value: string | null) =>
    wrap(
      value === null
        ? storage().removeItem(key(base))
        : storage().setItem(key(base), value),
    );

  const token = atom<string | null>(null, `${name}.token`).extend(
    withConnectHook(() => {
      void start();
      const stopFollowing = followStorage();
      const stopWarning = warnOnUnload();
      return () => {
        stopFollowing?.();
        stopWarning?.();
      };
    }),
  );
  const restoring = atom(true, `${name}._restoring`).extend(
    withConnectHook(() => {
      void start();
    }),
  );
  const isRefreshingToken = atom(false, `${name}.isRefreshingToken`);
  const started = atom(false, `${name}._started`);

  // `sub` is `userId|sessionId`, so a refreshed token keeps the session.
  const session = computed(() => {
    const value = token();
    if (value === null) return null;
    try {
      const { sub } = jwtDecode<{ sub?: string }>(value);
      return sub ?? value;
    } catch {
      return value;
    }
  }, `${name}.session`);

  const apply = (value: string | null) => {
    token.set(value);
    restoring.set(false);
  };
  const store = async (tokens: Tokens | null) => {
    await write(JWT_KEY, tokens?.token ?? null);
    await write(REFRESH_TOKEN_KEY, tokens?.refreshToken ?? null);
    apply(tokens?.token ?? null);
  };

  // Over HTTP: the client asks for a fresh token while its socket is paused
  // or stopped for auth.
  const exchange = async (args: Record<string, Value | undefined>) => {
    for (let retry = 0; ; retry++) {
      try {
        return await wrap(
          new ConvexHttpClient(convex.url, { logger: false }).action(
            signInRef,
            args,
          ),
        );
      } catch (error) {
        // fetch rejects with a TypeError when the request doesn't go through.
        if (!(error instanceof TypeError) || retry >= RETRY_BACKOFF.length)
          throw error;
        const delay = RETRY_BACKOFF[retry]! + RETRY_JITTER * Math.random();
        await wrap(new Promise((resolve) => setTimeout(resolve, delay)));
      }
    }
  };

  const signIn = action(
    async (
      provider?: string,
      params?: FormData | Record<string, Value>,
    ): Promise<ConvexAuthSignInResult> => {
      const fields =
        params instanceof FormData
          ? Object.fromEntries(
              Array.from(params.entries(), ([field, value]) => [
                field,
                typeof value === "string" ? value : value.name,
              ]),
            )
          : (params ?? {});
      const verifier = (await read(VERIFIER_KEY)) ?? undefined;
      await write(VERIFIER_KEY, null);
      const result = await wrap(
        convex.action(signInRef, { provider, params: fields, verifier }),
      );
      if (result.redirect !== undefined) {
        const url = new URL(result.redirect);
        await write(VERIFIER_KEY, result.verifier ?? null);
        await wrap(
          (options.navigate ?? ((target: URL) => location.assign(target)))(url),
        );
        return { signingIn: false, redirect: url };
      }
      if (result.tokens !== undefined) {
        await wrap(store(result.tokens));
        return { signingIn: result.tokens !== null };
      }
      return { signingIn: false };
    },
    `${name}.signIn`,
  );

  const completeSignIn = action(async (code: string) => {
    const { signingIn } = await wrap(signIn(undefined, { code }));
    return signingIn;
  }, `${name}.completeSignIn`).extend(withAsync());

  const isLoading = computed(
    () => restoring() || !completeSignIn.ready(),
    `${name}.isLoading`,
  );

  const signOut = action(async () => {
    try {
      await wrap(convex.action(signOutRef, {}));
    } catch {
      // Usually the session has already ended on the server.
    }
    await wrap(store(null));
  }, `${name}.signOut`);

  const refresh = async (before: string | null) => {
    // Another tab refreshed while this one waited for the lock.
    if (peek(token) !== before) return peek(token);
    const refreshToken = (await read(REFRESH_TOKEN_KEY)) ?? null;
    if (refreshToken === null) return null;
    isRefreshingToken.set(true);
    try {
      const { tokens } = await wrap(exchange({ refreshToken }));
      await wrap(store(tokens ?? null));
    } finally {
      isRefreshingToken.set(false);
    }
    return peek(token);
  };

  const fetchToken: AuthTokenFetcher = action(
    async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      const before = token();
      if (!forceRefreshToken) return before;
      return await wrap(
        exclusive(
          key(REFRESH_TOKEN_KEY),
          wrap(() => refresh(before)),
        ),
      );
    },
    `${name}.fetchToken`,
  );

  const start = action(async () => {
    if (peek(started)) return;
    started.set(true);
    const handleCode =
      typeof options.shouldHandleCode === "function"
        ? options.shouldHandleCode()
        : (options.shouldHandleCode ?? true);
    const url =
      typeof location === "undefined" ? undefined : new URL(location.href);
    const code = url?.searchParams.get("code");
    if (url && code && handleCode) {
      url.searchParams.delete("code");
      await wrap(
        (
          options.replaceURL ??
          ((relative: string) => history.replaceState({}, "", relative))
        )(url.pathname + url.search + url.hash),
      );
      try {
        await wrap(completeSignIn(code));
      } catch {
        // Kept in `completeSignIn.error()`.
      } finally {
        restoring.set(false);
      }
      return;
    }
    apply((await read(JWT_KEY)) ?? null);
  }, `${name}._start`);

  // Follows the token other tabs or frames write to the same storage.
  const followStorage = () => {
    if (typeof window === "undefined") return undefined;
    const listener = wrap((event: StorageEvent) => {
      if (event.storageArea !== storage() || event.key !== key(JWT_KEY)) return;
      // Each key comes as its own event; the refresh token stays in storage.
      apply(event.newValue);
    });
    window.addEventListener("storage", listener);
    return () => window.removeEventListener("storage", listener);
  };

  // Leaving during a refresh token exchange can lose the new pair.
  const warnOnUnload = () => {
    if (typeof window === "undefined") return undefined;
    const listener = wrap((event: BeforeUnloadEvent) => {
      if (!peek(isRefreshingToken)) return;
      event.preventDefault();
      event.returnValue = true;
    });
    window.addEventListener("beforeunload", listener);
    return () => window.removeEventListener("beforeunload", listener);
  };

  return {
    isLoading,
    session,
    fetchToken,
    token,
    isRefreshingToken,
    signIn,
    completeSignIn,
    signOut,
  };
}

const queues = new Map<string, Promise<unknown>>();

/** Runs `callback` alone among the tabs, or among callers in this one. */
function exclusive<T>(lock: string, callback: () => Promise<T>): Promise<T> {
  if (typeof navigator !== "undefined" && navigator.locks)
    return navigator.locks.request(lock, callback);
  const previous = queues.get(lock) ?? Promise.resolve();
  const next = previous.then(callback, callback);
  queues.set(
    lock,
    next.catch(() => {}),
  );
  return next;
}
