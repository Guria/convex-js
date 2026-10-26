import { Logger } from "../logging.js";
import { LocalSyncState } from "./local_state.js";
import { AuthError, IdentityVersion, Transition } from "./protocol.js";
import { action, atom, bind, Frame, wrap } from "./reatom.js";
import { jwtDecode } from "../../vendor/jwt-decode/index.js";

// setTimout uses 32 bit integer, so it can only
// schedule about 24 days in the future.
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

/**
 * What is provided to the client.
 */
type AuthConfig = {
  fetchToken: AuthTokenFetcher;
  onAuthChange: (isAuthenticated: boolean) => void;
  onRefreshChange?: ((isRefreshing: boolean) => void) | undefined;
};

/**
 * By default we take 3 steps:
 *   1. Fetch a possibly cached token and send it to the server
 *   2. Immediately fetch a fresh token without using a cache
 *   3. Repeat step 2 before the end of the fresh token's lifetime
 *
 * When `initialAuthTokenReuse` is enabled, step 2 is skipped: the
 * cached token is reused and a refetch is scheduled based on its
 * remaining lifetime (estimated via the server's clock skew). This
 * avoids a second Authenticate message that would cause the server
 * to re-execute all authenticated queries.
 *
 * If the server rejects a token before a scheduled refetch, we
 * immediately fetch a fresh token and retry.
 */
type AuthState =
  | { state: "noAuth" }
  | {
      state: "waitingForServerConfirmationOfCachedToken";
      config: AuthConfig;
      hasRetried: boolean;
    }
  | {
      state: "initialRefetch";
      config: AuthConfig;
    }
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
  // Special/weird state when we got a valid token
  // but could not fetch a new one.
  | {
      state: "notRefetching";
      config: AuthConfig;
    };

/**
 * Handles the state transitions for auth. The server is the source
 * of truth.
 */
export function reatomAuthenticationManager({
  syncState,
  callbacks,
  config: { logger, refreshTokenLeewaySeconds, initialAuthTokenReuse },
  root,
  name,
}: {
  // Shared by the BaseClient so that the auth manager can easily inspect it
  syncState: LocalSyncState;
  callbacks: {
    // Sends a message to the server
    authenticate: (token: string) => IdentityVersion;
    stopSocket: () => Promise<void>;
    tryRestartSocket: () => void;
    pauseSocket: () => void;
    resumeSocket: () => void;
    // Sends a message to the server
    clearAuth: () => void;
  };
  config: {
    refreshTokenLeewaySeconds: number;
    logger: Logger;
    initialAuthTokenReuse: boolean;
  };
  root: Frame;
  name: string;
}) {
  const authState = atom<AuthState>({ state: "noAuth" }, `${name}.authState`);
  // Used to detect races involving `setConfig` calls
  // while a token is being fetched.
  const configVersion = atom(0, `${name}.configVersion`);
  // Track last value to avoid redundant calls
  const lastRefreshChange = atom(false, `${name}.lastRefreshChange`);
  // Number of times we have attempted to confirm the latest token. We retry up
  // to `MAX_TOKEN_CONFIRMATION_ATTEMPTS` times.
  const tokenConfirmationAttempts = atom(
    0,
    `${name}.tokenConfirmationAttempts`,
  );

  const logVerbose = (message: string) =>
    logger.logVerbose(`${message} [v${configVersion()}]`);

  const notifyRefreshChange = (isRefreshing: boolean) => {
    const current = authState();
    if (
      current.state !== "noAuth" &&
      current.state !== "initialRefetch" &&
      current.config.onRefreshChange &&
      lastRefreshChange() !== isRefreshing
    ) {
      lastRefreshChange.set(isRefreshing);
      current.config.onRefreshChange(isRefreshing);
    }
  };

  const setAuthState = (newAuth: AuthState) => {
    const authStateForLog =
      newAuth.state === "waitingForServerConfirmationOfFreshToken"
        ? {
            hadAuth: newAuth.hadAuth,
            state: newAuth.state,
            token: `...${newAuth.token.slice(-7)}`,
          }
        : { state: newAuth.state };
    logVerbose(`setting auth state to ${JSON.stringify(authStateForLog)}`);
    switch (newAuth.state) {
      case "waitingForScheduledRefetch":
      case "notRefetching":
      case "noAuth":
        tokenConfirmationAttempts.set(0);
        break;
      case "waitingForServerConfirmationOfFreshToken":
      case "waitingForServerConfirmationOfCachedToken":
      case "initialRefetch":
        break;
      default: {
        newAuth satisfies never;
      }
    }
    const current = authState();
    if (current.state === "waitingForScheduledRefetch") {
      clearTimeout(current.refetchTokenTimeoutId);
    }
    authState.set(newAuth);
  };

  // The sole path to `state === "noAuth"`; consumers rely on this firing
  // `notifyRefreshChange(false)` to pair any in-flight `(true)`. May run
  // when refresh state is already false.
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

  // Protects against simultaneous calls to `setConfig`
  // while we're fetching a token
  const fetchTokenAndGuardAgainstRace = async (
    fetchToken: AuthTokenFetcher,
    fetchArgs: {
      forceRefreshToken: boolean;
    },
  ) => {
    const originalConfigVersion = configVersion() + 1;
    configVersion.set(originalConfigVersion);
    logVerbose(`fetching token with config version ${originalConfigVersion}`);
    const token = await wrap(fetchToken(fetchArgs));
    if (configVersion() !== originalConfigVersion) {
      // This is a stale config
      logVerbose(
        `stale config version, expected ${originalConfigVersion}, got ${configVersion()}`,
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

  // Force refetch the token and schedule another refetch
  // before the token expires - an active client should never
  // need to reauthenticate.
  const refetchToken = action(async () => {
    const initial = authState();
    if (initial.state === "noAuth") {
      return;
    }
    logVerbose("refetching auth token");
    const token = await wrap(
      fetchTokenAndGuardAgainstRace(initial.config.fetchToken, {
        forceRefreshToken: true,
      }),
    );
    if (token.isFromOutdatedConfig) {
      return;
    }
    const current = authState() as Exclude<AuthState, { state: "noAuth" }>;

    if (token.value) {
      if (syncState.isNewAuth(token.value)) {
        setAuthState({
          state: "waitingForServerConfirmationOfFreshToken",
          hadAuth: syncState.hasAuth(),
          token: token.value,
          config: current.config,
        });
        callbacks.authenticate(token.value);
      } else {
        setAuthState({
          state: "notRefetching",
          config: current.config,
        });
      }
    } else {
      logVerbose("refetching token failed");
      if (syncState.hasAuth()) {
        callbacks.clearAuth();
      }
      setAndReportAuthFailed(current.config.onAuthChange);
    }
    // Restart in case this refetch was triggered via schedule during
    // a reauthentication attempt.
    logVerbose("restarting WS after auth token fetch (if currently stopped)");
    callbacks.tryRestartSocket();
  }, `${name}.refetchToken`);

  const scheduleTokenRefetch = (token: string, clientClockSkewMs?: number) => {
    const current = authState();
    if (current.state === "noAuth") {
      return;
    }
    const decodedToken = decodeToken(token);
    if (!decodedToken) {
      // This is no longer really possible, because
      // we wait on server response before scheduling token refetch,
      // and the server currently requires JWT tokens.
      logger.error("Auth token is not a valid JWT, cannot refetch the token");
      return;
    }
    // iat: issued at time, UTC seconds timestamp at which the JWT was issued
    // exp: expiration time, UTC seconds timestamp at which the JWT will expire
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
      // For cached tokens we use the server's clock skew estimate to
      // compute remaining lifetime. The server computes
      // clientClockSkew = clientTs - serverReceiveTs, so it's positive
      // when the client is ahead and negative when behind. To recover
      // server time: serverNow = Date.now() - clientClockSkew.
      // Both Date.now() and setTimeout use the same client clock, so
      // the scheduled delay is accurate regardless of absolute offset.
      // Connect latency inflates the skew toward "client behind," which
      // is conservative — we schedule the refetch slightly early.
      const estimatedServerNowSeconds = (Date.now() - clientClockSkewMs) / 1000;
      tokenValiditySeconds = exp - estimatedServerNowSeconds;
      if (tokenValiditySeconds <= 0) {
        tokenValiditySeconds = 0;
      }
    } else {
      // For freshly-fetched tokens, `exp - iat` is the full lifetime
      // starting from now.
      tokenValiditySeconds = fullLifetimeSeconds;
    }
    // Attempt to refresh the token `refreshTokenLeewaySeconds` before it expires,
    // or immediately if the token is already expiring soon.
    let delay = Math.min(
      MAXIMUM_REFRESH_DELAY,
      (tokenValiditySeconds - refreshTokenLeewaySeconds) * 1000,
    );
    if (delay <= 0) {
      // Refetch immediately, but this might be due to configuring a `refreshTokenLeewaySeconds`
      // that is too large compared to the token's actual lifetime.
      logger.warn(
        `Refetching auth token immediately, configured leeway ${refreshTokenLeewaySeconds}s is larger than the token's lifetime ${tokenValiditySeconds}s`,
      );
      delay = 0;
    }
    const refetchTokenTimeoutId = setTimeout(
      bind(() => {
        logVerbose("running scheduled token refetch");
        void refetchToken();
      }, root),
      delay,
    );
    setAuthState({
      state: "waitingForScheduledRefetch",
      refetchTokenTimeoutId,
      config: current.config,
    });
    logVerbose(`scheduled preemptive auth token refetching in ${delay}ms`);
  };

  // This is similar to `refetchToken` defined above, in fact we
  // don't represent them as different states, but it is different
  // in that we pause the WebSocket so that mutations
  // don't retry with bad auth.
  const tryToReauthenticate = action(async (serverMessage: AuthError) => {
    logVerbose(`attempting to reauthenticate: ${serverMessage.error}`);
    const initial = authState();
    if (
      // No way to fetch another token, kaboom
      initial.state === "noAuth" ||
      // We failed on a fresh token. After a small number of retries, we give up
      // and clear the auth state to avoid infinite retries.
      (initial.state === "waitingForServerConfirmationOfFreshToken" &&
        tokenConfirmationAttempts() >= MAX_TOKEN_CONFIRMATION_ATTEMPTS)
    ) {
      logger.error(
        `Failed to authenticate: "${serverMessage.error}", check your server auth config`,
      );
      if (syncState.hasAuth()) {
        syncState.clearAuth();
      }
      if (initial.state !== "noAuth") {
        setAndReportAuthFailed(initial.config.onAuthChange);
      }
      return;
    }
    if (initial.state === "waitingForServerConfirmationOfFreshToken") {
      tokenConfirmationAttempts.set((attempts) => attempts + 1);
      logVerbose(
        `retrying reauthentication, ${MAX_TOKEN_CONFIRMATION_ATTEMPTS - tokenConfirmationAttempts()} attempts remaining`,
      );
    }

    notifyRefreshChange(true);
    await wrap(callbacks.stopSocket());
    const afterStop = authState();
    if (afterStop.state === "noAuth") {
      // setConfig() or stop() ran during the await.
      return;
    }
    const token = await wrap(
      fetchTokenAndGuardAgainstRace(afterStop.config.fetchToken, {
        forceRefreshToken: true,
      }),
    );
    if (token.isFromOutdatedConfig) {
      return;
    }
    const current = authState() as Exclude<AuthState, { state: "noAuth" }>;

    if (token.value && syncState.isNewAuth(token.value)) {
      callbacks.authenticate(token.value);
      setAuthState({
        state: "waitingForServerConfirmationOfFreshToken",
        config: current.config,
        token: token.value,
        hadAuth:
          current.state === "notRefetching" ||
          current.state === "waitingForScheduledRefetch",
      });
    } else {
      logVerbose("reauthentication failed, could not fetch a new token");
      if (syncState.hasAuth()) {
        syncState.clearAuth();
      }
      setAndReportAuthFailed(current.config.onAuthChange);
    }
    callbacks.tryRestartSocket();
  }, `${name}.tryToReauthenticate`);

  return {
    authState,

    setConfig: action(
      async (
        fetchToken: AuthTokenFetcher,
        onChange: (isAuthenticated: boolean) => void,
        onRefreshChange?: (isRefreshing: boolean) => void,
      ) => {
        resetAuthState();
        logVerbose("pausing WS for auth token fetch");
        callbacks.pauseSocket();
        const token = await wrap(
          fetchTokenAndGuardAgainstRace(fetchToken, {
            forceRefreshToken: false,
          }),
        );
        if (token.isFromOutdatedConfig) {
          return;
        }
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
          callbacks.authenticate(token.value);
        } else {
          setAuthState({
            state: "initialRefetch",
            config,
          });
          // Try again with `forceRefreshToken: true`
          await wrap(refetchToken());
        }
        logVerbose("resuming WS after auth token fetch");
        callbacks.resumeSocket();
      },
      `${name}.setConfig`,
    ),

    onTransition: action((serverMessage: Transition) => {
      if (
        !syncState.isCurrentOrNewerAuthVersion(
          serverMessage.endVersion.identity,
        )
      ) {
        // This is a stale transition - client has moved on to
        // a newer auth version.
        return;
      }
      if (
        serverMessage.endVersion.identity <= serverMessage.startVersion.identity
      ) {
        // This transition did not change auth - it is not a response to Authenticate.
        return;
      }

      const current = authState();
      logVerbose(`auth state is ${current.state} when handling transition`);

      // This transition advanced the auth version, which means the token used was valid
      // and the client and server auth states are in sync.
      syncState.markAuthCompletion();

      if (current.state === "waitingForServerConfirmationOfCachedToken") {
        logVerbose("server confirmed auth token is valid");
        const cachedToken = syncState.getAuth()?.value;
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
        tokenConfirmationAttempts.set(0);
        // Read after scheduling: once a refetch is scheduled the state has no
        // `hadAuth`, so the change is reported.
        const scheduled = authState() as { hadAuth?: boolean };
        if (!scheduled.hadAuth) {
          current.config.onAuthChange(true);
        }
      }
    }, `${name}.onTransition`),

    onAuthError: action((serverMessage: AuthError) => {
      const current = authState().state;
      // If the AuthError is not due to updating the token, and we're currently
      // waiting on the result of a token update, ignore.
      if (
        serverMessage.authUpdateAttempted === false &&
        (current === "waitingForServerConfirmationOfFreshToken" ||
          current === "waitingForServerConfirmationOfCachedToken")
      ) {
        logVerbose("ignoring non-auth token expired error");
        return;
      }
      const { baseVersion } = serverMessage;
      // Versioned AuthErrors are ignored if the client advanced to
      // a newer auth identity
      // Error are reporting the previous version, since the server
      // didn't advance, hence `+ 1`.
      if (!syncState.isCurrentOrNewerAuthVersion(baseVersion + 1)) {
        logVerbose("ignoring auth error for previous auth attempt");
        return;
      }
      void tryToReauthenticate(serverMessage);
    }, `${name}.onAuthError`),

    stop: action(() => {
      resetAuthState();
      // Bump this in case we are mid-token-fetch when we get stopped
      configVersion.set((version) => version + 1);
      logVerbose(`config version bumped to ${configVersion()}`);
    }, `${name}.stop`),
  };
}

export type AuthenticationManager = ReturnType<
  typeof reatomAuthenticationManager
>;
