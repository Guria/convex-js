import { Logger } from "../logging.js";
import {
  ClientMessage,
  encodeClientMessage,
  parseServerMessage,
  ServerMessage,
  Transition,
  TransitionChunk,
} from "./protocol.js";
import { action, atom, bind, Frame } from "./reatom.js";

const CLOSE_NORMAL = 1000;
const CLOSE_GOING_AWAY = 1001;
const CLOSE_NO_STATUS = 1005;
/** Convex-specific close code representing a "404 Not Found".
 * The edge Onramp accepts websocket upgrades before confirming that the
 * intended destination exists, so this code is sent once we've discovered that
 * the destination does not exist.
 */
const CLOSE_NOT_FOUND = 4040;

/**
 * The various states our WebSocket can be in:
 *
 * - "disconnected": We don't have a WebSocket, but plan to create one.
 * - "connecting": We have created the WebSocket and are waiting for the
 *   `onOpen` callback.
 * - "ready": We have an open WebSocket.
 * - "stopped": The WebSocket was closed and a new one can be created via `.restart()`.
 * - "terminated": We have closed the WebSocket and will never create a new one.
 *
 *
 * WebSocket State Machine
 * -----------------------
 * initialState: disconnected
 * validTransitions:
 *   disconnected:
 *     new WebSocket() -> connecting
 *     terminate() -> terminated
 *   connecting:
 *     onopen -> ready
 *     close() -> disconnected
 *     terminate() -> terminated
 *   ready:
 *     close() -> disconnected
 *     stop() -> stopped
 *     terminate() -> terminated
 *   stopped:
 *     restart() -> connecting
 *     terminate() -> terminated
 * terminalStates:
 *   terminated
 *
 * The `connecting` and `ready` state have a sub-state-machine for pausing.
 */
export type SocketState =
  | { state: "disconnected" }
  | { state: "connecting"; ws: WebSocket; paused: "yes" | "no" }
  | { state: "ready"; ws: WebSocket; paused: "yes" | "no" | "uninitialized" }
  | { state: "stopped" }
  | { state: "terminated" };

export type ReconnectMetadata = {
  connectionCount: number;
  lastCloseReason: string | null;
  clientTs: number;
};

export type OnMessageResponse = {
  hasSyncedPastLastReconnect: boolean;
};

let firstTime: number | undefined;
function monotonicMillis() {
  if (firstTime === undefined) {
    firstTime = Date.now();
  }
  if (typeof performance === "undefined" || !performance.now) {
    return Date.now();
  }
  return Math.round(firstTime + performance.now());
}

function prettyNow() {
  return `t=${Math.round((monotonicMillis() - firstTime!) / 100) / 10}s`;
}

const serverDisconnectErrors = {
  // A known error, e.g. during a restart or push
  InternalServerError: { timeout: 1000 },
  // ErrorMetadata::overloaded() messages that we realy should back off
  SubscriptionsWorkerFullError: { timeout: 3000 },
  TooManyConcurrentRequests: { timeout: 3000 },
  CommitterFullError: { timeout: 3000 },
  AwsTooManyRequestsException: { timeout: 3000 },
  ExecuteFullError: { timeout: 3000 },
  SystemTimeoutError: { timeout: 3000 },
  ExpiredInQueue: { timeout: 3000 },
  // ErrorMetadata::feature_temporarily_unavailable() that typically indicate a deploy just happened
  VectorIndexesUnavailable: { timeout: 1000 },
  SearchIndexesUnavailable: { timeout: 1000 },
  TableSummariesUnavailable: { timeout: 1000 },
  // More ErrorMetadata::overloaded()
  VectorIndexTooLarge: { timeout: 3000 },
  SearchIndexTooLarge: { timeout: 3000 },
  TooManyWritesInTimePeriod: { timeout: 3000 },
} as const satisfies Record<string, { timeout: number }>;

type ServerDisconnectError = keyof typeof serverDisconnectErrors | "Unknown";

function classifyDisconnectError(s?: string): ServerDisconnectError {
  if (s === undefined) return "Unknown";
  // startsWith so more info could be at the end (although currently there isn't)

  for (const prefix of Object.keys(
    serverDisconnectErrors,
  ) as ServerDisconnectError[]) {
    if (s.startsWith(prefix)) {
      return prefix;
    }
  }
  return "Unknown";
}

type TransitionChunkBuffer = {
  chunks: readonly string[];
  totalParts: number;
  transitionId: string;
};

type ScheduledReconnect = {
  timeout: ReturnType<typeof setTimeout>;
  scheduledAt: number;
  backoffMs: number;
};

/** Upon HTTPS/WSS failure, the first jittered backoff duration, in ms. */
const DEFAULT_INITIAL_BACKOFF = 1000;
/** We backoff exponentially, but we need to cap that--this is the jittered max. */
const MAX_BACKOFF = 16000;
// Ping messages (sync protocol Pings, not WebSocket protocol Pings) are
// sent every 15s in the absence of other messages. But a single large
// Transition or other downstream message can hog the line so this
// threshold is set higher to prevent clients from giving up.
const SERVER_INACTIVITY_THRESHOLD = 60000;

/**
 * A WebSocket that handles errors, reconnection, pausing and message parsing.
 * Its state lives in atoms; socket events and timers run as actions in
 * `root`.
 */
export function reatomWebSocketManager({
  uri,
  callbacks,
  webSocketConstructor,
  logger,
  debug,
  root,
  name,
}: {
  uri: string;
  callbacks: {
    onOpen: (reconnectMetadata: ReconnectMetadata) => void;
    onResume: () => void;
    onMessage: (message: ServerMessage) => OnMessageResponse;
    onServerDisconnectError?: ((message: string) => void) | undefined;
  };
  webSocketConstructor: typeof WebSocket;
  logger: Logger;
  debug: boolean;
  root: Frame;
  name: string;
}) {
  const socket = atom<SocketState>({ state: "disconnected" }, `${name}.socket`);
  const connectionCount = atom(0, `${name}.connectionCount`);
  const hasEverConnected = atom(false, `${name}.hasEverConnected`);
  /** How many times have we failed consecutively? */
  const retries = atom(0, `${name}.retries`);
  const lastCloseReason = atom<
    | "InitialConnect"
    | "OnCloseInvoked"
    | (string & {}) // a full serverErrorReason (not just the prefix) or a new one
    | null
  >("InitialConnect", `${name}.lastCloseReason`);
  /** The split-up Transition currently being received. */
  const transitionChunkBuffer = atom<TransitionChunkBuffer | null>(
    null,
    `${name}.transitionChunkBuffer`,
  );
  const scheduledReconnect = atom<ScheduledReconnect | null>(
    null,
    `${name}.scheduledReconnect`,
  );
  /** Pending event to send after reconnecting due to network recovery */
  const pendingNetworkRecoveryInfo = atom<{ timeSavedMs: number } | null>(
    null,
    `${name}.pendingNetworkRecoveryInfo`,
  );

  // Timer and listener handles.
  let reconnectDueToServerInactivityTimeout: ReturnType<
    typeof setTimeout
  > | null = null;
  let networkOnlineHandler: (() => void) | null = null;

  const logVerbose = (message: string) => logger.logVerbose(message);

  const setSocketState = (state: SocketState) => {
    socket.set(state);
    logVerbose(
      `socket state changed: ${state.state}, paused: ${
        "paused" in state ? state.paused : undefined
      }`,
    );
  };

  const setupNetworkListener = () => {
    // Only set up listener if we're in a browser environment with addEventListener
    // (React Native has window but not addEventListener)
    if (
      typeof window === "undefined" ||
      typeof window.addEventListener !== "function"
    ) {
      return;
    }
    // Avoid registering duplicate listeners
    if (networkOnlineHandler !== null) {
      return;
    }
    networkOnlineHandler = bind(() => {
      logVerbose("network online event detected");
      tryReconnectImmediately();
    }, root);
    window.addEventListener("online", networkOnlineHandler);
    logVerbose("network online event listener registered");
  };

  const cleanupNetworkListener = () => {
    if (
      networkOnlineHandler &&
      typeof window !== "undefined" &&
      typeof window.removeEventListener === "function"
    ) {
      window.removeEventListener("online", networkOnlineHandler);
      networkOnlineHandler = null;
      logVerbose("network online event listener removed");
    }
  };

  const assembleTransition = (chunk: TransitionChunk): Transition | null => {
    const buffer = transitionChunkBuffer();
    if (
      chunk.partNumber < 0 ||
      chunk.partNumber >= chunk.totalParts ||
      chunk.totalParts === 0 ||
      (buffer &&
        (buffer.totalParts !== chunk.totalParts ||
          buffer.transitionId !== chunk.transitionId))
    ) {
      // Throwing an error doesn't crash the client, so clear the buffer.
      transitionChunkBuffer.set(null);
      throw new Error("Invalid TransitionChunk");
    }
    const chunks = buffer?.chunks ?? [];
    if (chunk.partNumber !== chunks.length) {
      // Throwing an error doesn't crash the client, so clear the buffer.
      transitionChunkBuffer.set(null);
      throw new Error(
        `TransitionChunk received out of order: expected part ${chunks.length}, got ${chunk.partNumber}`,
      );
    }
    const next = [...chunks, chunk.chunk];
    if (next.length < chunk.totalParts) {
      transitionChunkBuffer.set({
        chunks: next,
        totalParts: chunk.totalParts,
        transitionId: chunk.transitionId,
      });
      return null;
    }
    transitionChunkBuffer.set(null);
    const transition = parseServerMessage(JSON.parse(next.join("")));
    if (transition.type !== "Transition") {
      throw new Error(
        `Expected Transition, got ${transition.type} after assembling chunks`,
      );
    }
    return transition;
  };

  const resetServerInactivityTimeout = () => {
    if (socket().state === "terminated") {
      // Don't reset any timers if we were trying to terminate.
      return;
    }
    if (reconnectDueToServerInactivityTimeout !== null) {
      clearTimeout(reconnectDueToServerInactivityTimeout);
      reconnectDueToServerInactivityTimeout = null;
    }
    reconnectDueToServerInactivityTimeout = setTimeout(
      bind(() => closeAndReconnect("InactiveServer"), root),
      SERVER_INACTIVITY_THRESHOLD,
    );
  };

  const nextBackoff = (reason: "client" | ServerDisconnectError): number => {
    const initialBackoff: number =
      reason === "client"
        ? 100 // There's no evidence of a server problem, retry quickly
        : reason === "Unknown"
          ? DEFAULT_INITIAL_BACKOFF
          : serverDisconnectErrors[reason].timeout;
    const baseBackoff = initialBackoff * Math.pow(2, retries());
    retries.set((count) => count + 1);
    const actualBackoff = Math.min(baseBackoff, MAX_BACKOFF);
    const jitter = actualBackoff * (Math.random() - 0.5);
    return actualBackoff + jitter;
  };

  const reportLargeTransition = ({
    transition,
    messageLength,
  }: {
    transition: Transition;
    messageLength: number;
  }) => {
    if (
      transition.clientClockSkew === undefined ||
      transition.serverTs === undefined
    ) {
      return;
    }
    const transitionTransitTime =
      monotonicMillis() - // client time now
      // clientClockSkew = (server time + upstream latency) - client time
      // clientClockSkew is "how many milliseconds behind (slow) is the client clock"
      // but the latency of the Connect message inflates this, making it appear further behind
      transition.clientClockSkew -
      transition.serverTs / 1_000_000; // server time when transition was sent
    const prettyTransitionTime = `${Math.round(transitionTransitTime)}ms`;
    const prettyMessageMB = `${Math.round(messageLength / 10_000) / 100}MB`;
    const bytesPerSecond = messageLength / (transitionTransitTime / 1000);
    const prettyBytesPerSecond = `${Math.round(bytesPerSecond / 10_000) / 100}MB per second`;
    logVerbose(
      `received ${prettyMessageMB} transition in ${prettyTransitionTime} at ${prettyBytesPerSecond}`,
    );
    // Warnings that will show up for *all users*, so don't be too aggressive.
    // These can be silenced (along with reconnection messages) by setting `logger: false` in client options.
    if (messageLength > 20_000_000) {
      // Big enough that the developer should be made aware of this.
      logger.log(
        `received query results totaling more that 20MB (${prettyMessageMB}) which will take a long time to download on slower connections`,
      );
    } else if (transitionTransitTime > 20_000) {
      // Long enough that a pattern of these should be interesting to a developer, but be aware that
      // weak connections, putting clients to sleep, backgrounding etc. could all cause this too.
      logger.log(
        `received query results totaling ${prettyMessageMB} which took more than 20s to arrive (${prettyTransitionTime})`,
      );
    }
    if (debug) {
      // debug means "reportDebugInfoToConvex" is set so this can be aggressive.
      sendMessage({
        type: "Event",
        eventType: "ClientReceivedTransition",
        event: { transitionTransitTime, messageLength },
      });
    }
  };

  const handleOpen = action((ws: WebSocket) => {
    logger.logVerbose("begin ws.onopen");
    const current = socket();
    if (current.state !== "connecting") {
      throw new Error("onopen called with socket not in connecting state");
    }
    const paused = current.paused === "yes" ? "uninitialized" : "no";
    setSocketState({ state: "ready", ws, paused });
    resetServerInactivityTimeout();
    if (paused === "no") {
      hasEverConnected.set(true);
      callbacks.onOpen({
        connectionCount: connectionCount(),
        lastCloseReason: lastCloseReason(),
        clientTs: monotonicMillis(),
      });
    }

    const reason = lastCloseReason();
    if (reason !== "InitialConnect") {
      if (reason) {
        logger.log(
          "WebSocket reconnected at",
          prettyNow(),
          "after disconnect due to",
          reason,
        );
      } else {
        logger.log("WebSocket reconnected at", prettyNow());
      }
    }

    connectionCount.set((count) => count + 1);
    lastCloseReason.set(null);

    // Send event for network recovery reconnect if applicable
    const recovery = pendingNetworkRecoveryInfo();
    if (recovery !== null) {
      pendingNetworkRecoveryInfo.set(null);
      sendMessage({
        type: "Event",
        eventType: "NetworkRecoveryReconnect",
        event: { timeSavedMs: recovery.timeSavedMs },
      });
      logger.log(
        `Network recovery reconnect saved ~${Math.round(recovery.timeSavedMs / 1000)}s of waiting`,
      );
    }
  }, `${name}.handleOpen`);

  const handleMessage = action((data: string) => {
    resetServerInactivityTimeout();
    const messageLength = data.length;
    let serverMessage = parseServerMessage(JSON.parse(data));
    logVerbose(`received ws message with type ${serverMessage.type}`);

    // Ping's only purpose is to reset the server inactivity timer.
    if (serverMessage.type === "Ping") {
      return;
    }

    // TransitionChunks never reach the main client logic.
    if (serverMessage.type === "TransitionChunk") {
      const transition = assembleTransition(serverMessage);
      if (!transition) {
        return;
      }
      serverMessage = transition;
      logVerbose(`assembled full ws message of type ${serverMessage.type}`);
    }

    if (transitionChunkBuffer() !== null) {
      transitionChunkBuffer.set(null);
      logger.log(
        `Received unexpected ${serverMessage.type} while buffering TransitionChunks`,
      );
    }

    if (serverMessage.type === "Transition") {
      reportLargeTransition({ messageLength, transition: serverMessage });
    }
    const response = callbacks.onMessage(serverMessage);
    if (response.hasSyncedPastLastReconnect) {
      // Reset backoff to 0 once all outstanding requests are complete.
      retries.set(0);
    }
  }, `${name}.handleMessage`);

  const handleClose = action((event: CloseEvent) => {
    logVerbose("begin ws.onclose");
    transitionChunkBuffer.set(null);
    if (lastCloseReason() === null) {
      // event.reason is often an empty string
      lastCloseReason.set(event.reason || `closed with code ${event.code}`);
    }
    if (
      event.code !== CLOSE_NORMAL &&
      event.code !== CLOSE_GOING_AWAY && // This commonly gets fired on mobile apps when the app is backgrounded
      event.code !== CLOSE_NO_STATUS &&
      event.code !== CLOSE_NOT_FOUND // Note that we want to retry on a 404, as it can be transient during a push.
    ) {
      let msg = `WebSocket closed with code ${event.code}`;
      if (event.reason) {
        msg += `: ${event.reason}`;
      }
      logger.log(msg);
      if (callbacks.onServerDisconnectError && event.reason) {
        // This callback is a unstable API, InternalServerErrors in particular may be removed
        // since they reflect expected temporary downtime. But until a quantitative measure
        // of uptime is reported this unstable API errs on the inclusive side.
        callbacks.onServerDisconnectError(msg);
      }
    }
    scheduleReconnect(classifyDisconnectError(event.reason));
  }, `${name}.handleClose`);

  const connect = action(() => {
    const current = socket().state;
    if (current === "terminated") {
      return;
    }
    if (current !== "disconnected" && current !== "stopped") {
      throw new Error(
        "Didn't start connection from disconnected state: " + current,
      );
    }

    const ws = new webSocketConstructor(uri);
    logVerbose("constructed WebSocket");
    setSocketState({ state: "connecting", ws, paused: "no" });

    // Kick off server inactivity timer before WebSocket connection is established
    // so we can detect cases where handshake fails.
    // The `onopen` event only fires after the connection is established:
    // Source: https://datatracker.ietf.org/doc/html/rfc6455#page-19:~:text=_The%20WebSocket%20Connection%20is%20Established_,-and
    resetServerInactivityTimeout();

    ws.onopen = bind(() => handleOpen(ws), root);
    // NB: The WebSocket API calls `onclose` even if connection fails, so we can route all error paths through `onclose`.
    ws.onerror = bind((error: Event) => {
      transitionChunkBuffer.set(null);
      const message = (error as ErrorEvent).message;
      if (message) {
        logger.log(`WebSocket error message: ${message}`);
      }
    }, root);
    ws.onmessage = bind(
      (message: MessageEvent) => handleMessage(message.data),
      root,
    );
    ws.onclose = bind(handleClose, root);
  }, `${name}.connect`);

  const scheduleReconnect = action(
    (reason: "client" | ServerDisconnectError) => {
      // Cancel any existing scheduled reconnect to avoid multiple reconnects
      const scheduled = scheduledReconnect();
      if (scheduled) {
        clearTimeout(scheduled.timeout);
        scheduledReconnect.set(null);
      }

      socket.set({ state: "disconnected" });
      const backoff = nextBackoff(reason);
      logger.log(`Attempting reconnect in ${Math.round(backoff)}ms`);

      const scheduledAt = monotonicMillis();
      const timeout: ReturnType<typeof setTimeout> = setTimeout(
        bind(() => {
          // Only proceed if this timeout hasn't been cleared
          if (scheduledReconnect()?.timeout === timeout) {
            scheduledReconnect.set(null);
            connect();
          }
        }, root),
        backoff,
      );
      scheduledReconnect.set({ timeout, scheduledAt, backoffMs: backoff });
    },
    `${name}.scheduleReconnect`,
  );

  /**
   * Close the WebSocket and schedule a reconnect.
   *
   * This should be used when we hit an error and would like to restart the session.
   */
  const closeAndReconnect = action((closeReason: string) => {
    logVerbose(`begin closeAndReconnect with reason ${closeReason}`);
    const current = socket().state;
    if (current !== "connecting" && current !== "ready") {
      // Nothing to do if we don't have a WebSocket.
      return;
    }
    lastCloseReason.set(closeReason);
    // Close the old socket asynchronously, we'll open a new socket in reconnect.
    void close();
    scheduleReconnect("client");
  }, `${name}.closeAndReconnect`);

  /**
   * Close the WebSocket, being careful to clear the onclose handler to avoid re-entrant
   * calls. Use this instead of directly calling `ws.close()`
   *
   * It is the callers responsibility to update the state after this method is called so that the
   * closed socket is not accessible or used again after this method is called
   */
  const close = (): Promise<void> => {
    transitionChunkBuffer.set(null);
    const current = socket();
    if (current.state !== "connecting" && current.state !== "ready") {
      // Nothing to do if we don't have a WebSocket.
      return Promise.resolve();
    }
    const ws = current.ws;
    // Messages can still be received after close but we're not interested.
    ws.onmessage = (_message) => {
      logVerbose("Ignoring message received after close");
    };
    if (current.state === "connecting") {
      return new Promise((r) => {
        ws.onclose = () => {
          logVerbose("Closed after connecting");
          r();
        };
        ws.onopen = () => {
          logVerbose("Opened after connecting");
          ws.close();
        };
      });
    }
    logVerbose("ws.close called");
    const result: Promise<void> = new Promise((r) => {
      ws.onclose = () => {
        r();
      };
    });
    ws.close();
    return result;
  };

  /**
   * @param message - A ClientMessage to send.
   * @returns Whether the message (might have been) sent.
   */
  const sendMessage = action((message: ClientMessage): boolean => {
    const messageForLog = {
      type: message.type,
      ...(message.type === "Authenticate" && message.tokenType === "User"
        ? {
            value: `...${message.value.slice(-7)}`,
          }
        : {}),
    };
    const current = socket();
    if (current.state === "ready" && current.paused === "no") {
      const request = JSON.stringify(encodeClientMessage(message));
      let sent = false;
      try {
        current.ws.send(request);
        sent = true;
      } catch (error: any) {
        logger.log(
          `Failed to send message on WebSocket, reconnecting: ${error}`,
        );
        closeAndReconnect("FailedToSendMessage");
      }
      logVerbose(
        `${sent ? "sent" : "failed to send"} message with type ${message.type}: ${JSON.stringify(
          messageForLog,
        )}`,
      );
      return true;
    }
    logVerbose(
      `message not sent (socket state: ${current.state}, paused: ${"paused" in current ? current.paused : undefined}): ${JSON.stringify(
        messageForLog,
      )}`,
    );
    return false;
  }, `${name}.sendMessage`);

  /**
   * Close the WebSocket and do not reconnect.
   * @returns A Promise that resolves when the WebSocket `onClose` callback is called.
   */
  const terminate = action((): Promise<void> => {
    if (reconnectDueToServerInactivityTimeout) {
      clearTimeout(reconnectDueToServerInactivityTimeout);
    }
    const scheduled = scheduledReconnect();
    if (scheduled) {
      clearTimeout(scheduled.timeout);
      scheduledReconnect.set(null);
    }
    cleanupNetworkListener();
    const result = close();
    setSocketState({ state: "terminated" });
    return result;
  }, `${name}.terminate`);

  const stop = action((): Promise<void> => {
    if (socket().state === "terminated") {
      // If we're terminating we ignore stop
      return Promise.resolve();
    }
    cleanupNetworkListener();
    const result = close();
    socket.set({ state: "stopped" });
    return result;
  }, `${name}.stop`);

  /**
   * Create a new WebSocket after a previous `stop()`, unless `terminate()` was
   * called before.
   */
  const tryRestart = action(() => {
    if (socket().state !== "stopped") {
      logger.logVerbose("Restart called without stopping first");
      return;
    }
    setupNetworkListener();
    connect();
  }, `${name}.tryRestart`);

  const pause = action(() => {
    const current = socket();
    if (current.state === "connecting" || current.state === "ready") {
      socket.set({ ...current, paused: "yes" });
    }
  }, `${name}.pause`);

  /**
   * Try to reconnect immediately, canceling any scheduled reconnect.
   * This is useful when detecting network recovery.
   * Only takes action if we're in disconnected state (waiting to reconnect).
   */
  const tryReconnectImmediately = action(() => {
    logVerbose("tryReconnectImmediately called");
    // Only reconnect if we're in disconnected state (waiting to reconnect)
    const current = socket().state;
    if (current !== "disconnected") {
      logVerbose(
        `tryReconnectImmediately called but socket state is ${current}, no action taken`,
      );
      return;
    }
    // Track how much time we saved by reconnecting immediately
    let timeSavedMs: number | null = null;
    const scheduled = scheduledReconnect();
    if (scheduled) {
      const elapsed = monotonicMillis() - scheduled.scheduledAt;
      timeSavedMs = Math.max(0, scheduled.backoffMs - elapsed);
      logVerbose(
        `would have waited ${Math.round(timeSavedMs)}ms more (backoff was ${Math.round(scheduled.backoffMs)}ms, elapsed ${Math.round(elapsed)}ms)`,
      );
      // Cancel the scheduled reconnect
      clearTimeout(scheduled.timeout);
      scheduledReconnect.set(null);
      logVerbose("canceled scheduled reconnect");
    }
    logger.log("Network recovery detected, reconnecting immediately");
    // Store the time saved to send as an event after we connect
    pendingNetworkRecoveryInfo.set(
      timeSavedMs !== null ? { timeSavedMs } : null,
    );
    connect();
  }, `${name}.tryReconnectImmediately`);

  /**
   * Resume the state machine if previously paused.
   */
  const resume = action(() => {
    const current = socket();
    if (current.state === "connecting") {
      socket.set({ ...current, paused: "no" });
    } else if (current.state === "ready") {
      if (current.paused === "uninitialized") {
        socket.set({ ...current, paused: "no" });
        hasEverConnected.set(true);
        callbacks.onOpen({
          connectionCount: connectionCount(),
          lastCloseReason: lastCloseReason(),
          clientTs: monotonicMillis(),
        });
      } else if (current.paused === "yes") {
        socket.set({ ...current, paused: "no" });
        callbacks.onResume();
      }
    }
    // Ignore resume if not paused, perhaps we already resumed.
  }, `${name}.resume`);

  return {
    socket,
    connectionCount,
    hasEverConnected,
    retries,
    /** Opens the first WebSocket. */
    start: action(() => {
      setupNetworkListener();
      connect();
    }, `${name}.start`),
    sendMessage,
    terminate,
    stop,
    tryRestart,
    pause,
    resume,
    tryReconnectImmediately,
  };
}

export type WebSocketManager = ReturnType<typeof reatomWebSocketManager>;
