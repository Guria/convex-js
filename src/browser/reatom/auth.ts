/**
 * Token fetch, server confirmation and refresh, with the state machine in an
 * atom. By default the client takes 3 steps:
 *   1. Fetch a possibly cached token and send it to the server.
 *   2. Once the server confirms it, fetch a fresh token without the cache.
 *   3. Refetch before each fresh token expires.
 *
 * With `initialAuthTokenReuse` step 2 is skipped: the cached token stays and
 * its refetch is scheduled from its remaining lifetime, estimated with the
 * server's clock skew. When the server rejects a token, the socket stops, a
 * fresh token is fetched and sent, and the socket restarts.
 */
import { action, atom, context, peek, wrap, type Frame } from "@reatom/core";
import { Logger } from "../logging.js";
import { AuthError, Transition } from "../sync/protocol.js";
import { jwtDecode } from "../../vendor/jwt-decode/index.js";

// setTimeout takes a 32-bit delay, about 24 days.
const MAXIMUM_REFRESH_DELAY = 20 * 24 * 60 * 60 * 1000; // 20 days

const MAX_TOKEN_CONFIRMATION_ATTEMPTS = 2;

/**
 * An async function returning a JWT. Depending on the auth providers
 * configured in convex/auth.config.ts, this may be a JWT-encoded OpenID
 * Connect Identity Token or a traditional JWT.
 *
 * `forceRefreshToken` is `true` if the server rejected a previously
 * returned token or the token is anticipated to expiring soon
 * based on its `exp` time.
 *
 * See {@link ConvexReactClient.setAuth}.
 *
 * @public
 */
export type AuthTokenFetcher = (args: {
  forceRefreshToken: boolean;
}) => Promise<string | null | undefined>;

export type AuthConfig = {
  fetchToken: AuthTokenFetcher;
  onAuthChange: (isAuthenticated: boolean) => void;
  onRefreshChange?: ((isRefreshing: boolean) => void) | undefined;
};

export type AuthState =
  | { state: "noAuth" }
  | {
      state: "waitingForServerConfirmationOfCachedToken";
      config: AuthConfig;
      hasRetried: boolean;
    }
  | { state: "initialRefetch"; config: AuthConfig }
  | {
      state: "waitingForServerConfirmationOfFreshToken";
      config: AuthConfig;
      hadAuth: boolean;
      token: string;
    }
  | {
      state: "waitingForScheduledRefetch";
      config: AuthConfig;
      refetchTokenTimeoutId: ReturnType<typeof setTimeout>;
    }
  // A valid token, but a fresh fetch returned the same one.
  | { state: "notRefetching"; config: AuthConfig };

/** The client's side of auth: its identity state and the socket. */
export type AuthSync = {
  isCurrentOrNewerAuthVersion(version: number): boolean;
  markAuthCompletion(): void;
  token(): string | undefined;
  hasAuth(): boolean;
  /** Sends `Authenticate` with the token. */
  authenticate(token: string): void;
  /** Forgets the token without sending anything; the socket is stopped. */
  dropAuth(): void;
  /** Forgets the token and sends `Authenticate` with none. */
  clearAuth(): void;
  stopSocket(): Promise<void>;
  tryRestartSocket(): void;
  pauseSocket(): void;
  resumeSocket(): void;
};

type AuthRuntime = {
  root: Frame;
  /** Bumped by `setConfig` and `stop`, so a token fetched meanwhile is dropped. */
  configVersion: number;
  lastRefreshChange: boolean;
  /** Attempts to confirm the latest fresh token. */
  tokenConfirmationAttempts: number;
};

export function reatomAuth(
  sync: AuthSync,
  options: {
    logger: Logger;
    refreshTokenLeewaySeconds: number;
    initialAuthTokenReuse: boolean;
  },
  name: string,
) {
  const { logger, refreshTokenLeewaySeconds, initialAuthTokenReuse } = options;
  const state = atom<AuthState>({ state: "noAuth" }, `${name}.state`);
  const runtime = atom(
    (): AuthRuntime => ({
      root: context(),
      configVersion: 0,
      lastRefreshChange: false,
      tokenConfirmationAttempts: 0,
    }),
    `${name}._runtime`,
  );

  const logVerbose = (message: string) =>
    logger.logVerbose(`${message} [v${peek(runtime).configVersion}]`);

  const setAuthState = (next: AuthState) => {
    const authStateForLog =
      next.state === "waitingForServerConfirmationOfFreshToken"
        ? {
            hadAuth: next.hadAuth,
            state: next.state,
            token: `...${next.token.slice(-7)}`,
          }
        : { state: next.state };
    logVerbose(`setting auth state to ${JSON.stringify(authStateForLog)}`);
    if (
      next.state === "waitingForScheduledRefetch" ||
      next.state === "notRefetching" ||
      next.state === "noAuth"
    ) {
      peek(runtime).tokenConfirmationAttempts = 0;
    }
    const current = state();
    if (current.state === "waitingForScheduledRefetch") {
      clearTimeout(current.refetchTokenTimeoutId);
    }
    state.set(next);
  };

  const notifyRefreshChange = (isRefreshing: boolean) => {
    const current = state();
    const rt = peek(runtime);
    if (
      current.state !== "noAuth" &&
      current.state !== "initialRefetch" &&
      current.config.onRefreshChange &&
      rt.lastRefreshChange !== isRefreshing
    ) {
      rt.lastRefreshChange = isRefreshing;
      current.config.onRefreshChange(isRefreshing);
    }
  };

  // The only way into `noAuth`; pairs any pending `onRefreshChange(true)`.
  const resetAuthState = () => {
    notifyRefreshChange(false);
    setAuthState({ state: "noAuth" });
  };

  const setAndReportAuthFailed = (
    onAuthChange: (authenticated: boolean) => void,
  ) => {
    onAuthChange(false);
    resetAuthState();
  };

  /** `isFromOutdatedConfig` when `setConfig` or `stop` ran during the fetch. */
  const fetchTokenAndGuardAgainstRace = async (
    fetchToken: AuthTokenFetcher,
    fetchArgs: { forceRefreshToken: boolean },
  ): Promise<
    | { isFromOutdatedConfig: true }
    | { isFromOutdatedConfig: false; value: string | null | undefined }
  > => {
    const rt = peek(runtime);
    const originalConfigVersion = ++rt.configVersion;
    logVerbose(`fetching token with config version ${originalConfigVersion}`);
    const token = await wrap(fetchToken(fetchArgs));
    if (rt.configVersion !== originalConfigVersion) {
      logVerbose(
        `stale config version, expected ${originalConfigVersion}, got ${rt.configVersion}`,
      );
      return { isFromOutdatedConfig: true };
    }
    return { isFromOutdatedConfig: false, value: token };
  };

  const decodeToken = (token: string) => {
    try {
      return jwtDecode(token);
    } catch (e) {
      logVerbose(
        `Error decoding token: ${e instanceof Error ? e.message : "Unknown error"}`,
      );
      return null;
    }
  };

  const scheduleTokenRefetch = (token: string, clientClockSkewMs?: number) => {
    const current = state();
    if (current.state === "noAuth") return;
    const decodedToken = decodeToken(token);
    if (!decodedToken) {
      // The server only confirms JWTs, so this should not happen.
      logger.error("Auth token is not a valid JWT, cannot refetch the token");
      return;
    }
    // iat: issued at, exp: expires at, both UTC seconds.
    const { iat, exp } = decodedToken as { iat?: number; exp?: number };
    if (!iat || !exp) {
      logger.error(
        "Auth token does not have required fields, cannot refetch the token",
      );
      return;
    }
    const fullLifetimeSeconds = exp - iat;
    if (fullLifetimeSeconds <= 2) {
      logger.error(
        "Auth token does not live long enough, cannot refetch the token",
      );
      return;
    }
    let tokenValiditySeconds: number;
    if (clientClockSkewMs !== undefined) {
      // A cached token's remaining lifetime, on the server's clock. The server
      // computes clientClockSkew = clientTs - serverReceiveTs. Connect latency
      // makes the client look further behind, so the refetch runs early.
      const estimatedServerNowSeconds = (Date.now() - clientClockSkewMs) / 1000;
      tokenValiditySeconds = Math.max(0, exp - estimatedServerNowSeconds);
    } else {
      // A fresh token lives `exp - iat` from now.
      tokenValiditySeconds = fullLifetimeSeconds;
    }
    let delay = Math.min(
      MAXIMUM_REFRESH_DELAY,
      (tokenValiditySeconds - refreshTokenLeewaySeconds) * 1000,
    );
    if (delay <= 0) {
      logger.warn(
        `Refetching auth token immediately, configured leeway ${refreshTokenLeewaySeconds}s is larger than the token's lifetime ${tokenValiditySeconds}s`,
      );
      delay = 0;
    }
    // Owned by the auth state machine, not a query subscription. Changing
    // config or stopping auth cancels this in resetAuthState.
    const refetchTokenTimeoutId = setTimeout(
      wrap(() => {
        logVerbose("running scheduled token refetch");
        void refetchToken();
      }, peek(runtime).root),
      delay,
    );
    setAuthState({
      state: "waitingForScheduledRefetch",
      refetchTokenTimeoutId,
      config: current.config,
    });
    logVerbose(`scheduled preemptive auth token refetching in ${delay}ms`);
  };

  // Force-fetches a token and schedules the next refetch once the server
  // confirms it, so an active client never has to reauthenticate.
  const refetchToken = action(async () => {
    const current = state();
    if (current.state === "noAuth") return;
    logVerbose("refetching auth token");
    const token = await wrap(
      fetchTokenAndGuardAgainstRace(current.config.fetchToken, {
        forceRefreshToken: true,
      }),
    );
    if (token.isFromOutdatedConfig) return;
    if (token.value) {
      if (sync.token() !== token.value) {
        setAuthState({
          state: "waitingForServerConfirmationOfFreshToken",
          hadAuth: sync.hasAuth(),
          token: token.value,
          config: current.config,
        });
        sync.authenticate(token.value);
      } else {
        setAuthState({ state: "notRefetching", config: current.config });
      }
    } else {
      logVerbose("refetching token failed");
      if (sync.hasAuth()) {
        sync.clearAuth();
      }
      setAndReportAuthFailed(current.config.onAuthChange);
    }
    // In case a scheduled refetch ran during a reauthentication.
    logVerbose("restarting WS after auth token fetch (if currently stopped)");
    sync.tryRestartSocket();
  }, `${name}._refetchToken`);

  // Like `refetchToken`, but with the socket stopped so requests don't retry
  // with the rejected token.
  const tryToReauthenticate = action(async (serverMessage: AuthError) => {
    logVerbose(`attempting to reauthenticate: ${serverMessage.error}`);
    const current = state();
    const rt = peek(runtime);
    if (
      // No way to fetch another token.
      current.state === "noAuth" ||
      // A fresh token failed too often; give up instead of retrying forever.
      (current.state === "waitingForServerConfirmationOfFreshToken" &&
        rt.tokenConfirmationAttempts >= MAX_TOKEN_CONFIRMATION_ATTEMPTS)
    ) {
      logger.error(
        `Failed to authenticate: "${serverMessage.error}", check your server auth config`,
      );
      if (sync.hasAuth()) {
        sync.dropAuth();
      }
      if (current.state !== "noAuth") {
        setAndReportAuthFailed(current.config.onAuthChange);
      }
      return;
    }
    if (current.state === "waitingForServerConfirmationOfFreshToken") {
      rt.tokenConfirmationAttempts++;
      logVerbose(
        `retrying reauthentication, ${MAX_TOKEN_CONFIRMATION_ATTEMPTS - rt.tokenConfirmationAttempts} attempts remaining`,
      );
    }
    notifyRefreshChange(true);
    await wrap(sync.stopSocket());
    const afterStop = state();
    // `setConfig` or `stop` ran during the await.
    if (afterStop.state === "noAuth") return;
    const token = await wrap(
      fetchTokenAndGuardAgainstRace(afterStop.config.fetchToken, {
        forceRefreshToken: true,
      }),
    );
    if (token.isFromOutdatedConfig) return;
    const latest = state();
    // Another auth error gave up meanwhile.
    if (latest.state === "noAuth") return;
    if (token.value && sync.token() !== token.value) {
      sync.authenticate(token.value);
      setAuthState({
        state: "waitingForServerConfirmationOfFreshToken",
        config: latest.config,
        token: token.value,
        hadAuth:
          latest.state === "notRefetching" ||
          latest.state === "waitingForScheduledRefetch",
      });
    } else {
      logVerbose("reauthentication failed, could not fetch a new token");
      if (sync.hasAuth()) {
        sync.dropAuth();
      }
      setAndReportAuthFailed(latest.config.onAuthChange);
    }
    sync.tryRestartSocket();
  }, `${name}._reauthenticate`);

  const setConfig = action(
    async (
      fetchToken: AuthTokenFetcher,
      onChange: (isAuthenticated: boolean) => void,
      onRefreshChange?: (isRefreshing: boolean) => void,
    ) => {
      resetAuthState();
      logVerbose("pausing WS for auth token fetch");
      sync.pauseSocket();
      const token = await wrap(
        fetchTokenAndGuardAgainstRace(fetchToken, {
          forceRefreshToken: false,
        }),
      );
      if (token.isFromOutdatedConfig) return;
      const config: AuthConfig = {
        fetchToken,
        onAuthChange: onChange,
        onRefreshChange,
      };
      if (token.value) {
        setAuthState({
          state: "waitingForServerConfirmationOfCachedToken",
          config,
          hasRetried: false,
        });
        sync.authenticate(token.value);
      } else {
        setAuthState({ state: "initialRefetch", config });
        // Try again with `forceRefreshToken: true`.
        await wrap(refetchToken());
      }
      logVerbose("resuming WS after auth token fetch");
      sync.resumeSocket();
    },
    `${name}.setConfig`,
  );

  return {
    /** The auth state machine. */
    state,

    setConfig,

    onTransition(serverMessage: Transition) {
      // Stale: the client has moved on to a newer identity.
      if (
        !sync.isCurrentOrNewerAuthVersion(serverMessage.endVersion.identity)
      ) {
        return;
      }
      // Not an answer to `Authenticate`.
      if (
        serverMessage.endVersion.identity <= serverMessage.startVersion.identity
      ) {
        return;
      }
      const current = state();
      logVerbose(`auth state is ${current.state} when handling transition`);
      // The server accepted the token: client and server identities agree.
      sync.markAuthCompletion();
      if (current.state === "waitingForServerConfirmationOfCachedToken") {
        logVerbose("server confirmed auth token is valid");
        const cachedToken = sync.token();
        if (initialAuthTokenReuse && cachedToken) {
          scheduleTokenRefetch(cachedToken, serverMessage.clientClockSkew);
        } else {
          void refetchToken();
        }
        current.config.onAuthChange(true);
        return;
      }
      if (current.state === "waitingForServerConfirmationOfFreshToken") {
        logVerbose("server confirmed new auth token is valid");
        notifyRefreshChange(false);
        scheduleTokenRefetch(current.token);
        peek(runtime).tokenConfirmationAttempts = 0;
        // Read after scheduling, as convex@1.48.0 does: once the refetch is
        // scheduled the state has no `hadAuth`, so every confirmed fresh
        // token reports `true`.
        const after = state();
        if (!("hadAuth" in after && after.hadAuth)) {
          current.config.onAuthChange(true);
        }
      }
    },

    onAuthError(serverMessage: AuthError) {
      const current = state().state;
      // An expired-token error while a new token is already on its way.
      if (
        serverMessage.authUpdateAttempted === false &&
        (current === "waitingForServerConfirmationOfFreshToken" ||
          current === "waitingForServerConfirmationOfCachedToken")
      ) {
        logVerbose("ignoring non-auth token expired error");
        return;
      }
      // The error reports the version the server stayed at, hence `+ 1`.
      if (!sync.isCurrentOrNewerAuthVersion(serverMessage.baseVersion + 1)) {
        logVerbose("ignoring auth error for previous auth attempt");
        return;
      }
      void tryToReauthenticate(serverMessage);
    },

    stop() {
      resetAuthState();
      // Drops a token fetch in progress.
      peek(runtime).configVersion++;
      logVerbose(`config version bumped to ${peek(runtime).configVersion}`);
    },
  };
}

export type ReatomAuth = ReturnType<typeof reatomAuth>;
