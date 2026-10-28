/**
 * The WebSocket half of the client: connect, reconnect with backoff, pause for
 * auth, server inactivity and chunked transitions. The state machine is the
 * one documented in `../sync/web_socket_manager.ts` of convex@1.48.0:
 *
 *   disconnected -> connecting -> ready -> disconnected (on close)
 *   ready -> stopped (stop()) -> connecting (tryRestart())
 *   any -> terminated (terminate())
 *
 * `connecting` and `ready` carry a pause flag for auth token fetches. Socket
 * state, counters and retries are atoms; the socket handle, timers and the
 * chunk buffer live in a per-context runtime object nothing subscribes to.
 */
import { atom, context, peek, wrap, type Frame } from "@reatom/core";
import { Logger } from "../logging.js";
import {
  ClientMessage,
  encodeClientMessage,
  parseServerMessage,
  ServerMessage,
  Transition,
  TransitionChunk,
} from "../sync/protocol.js";

const CLOSE_NORMAL = 1000;
const CLOSE_GOING_AWAY = 1001;
const CLOSE_NO_STATUS = 1005;
/** Convex's "404 Not Found": retried, since it can be transient during a push. */
const CLOSE_NOT_FOUND = 4040;

export type SocketState =
  | { state: "disconnected" }
  | { state: "connecting"; paused: "yes" | "no" }
  | { state: "ready"; paused: "yes" | "no" | "uninitialized" }
  | { state: "stopped" }
  | { state: "terminated" };

export type ReconnectMetadata = {
  connectionCount: number;
  lastCloseReason: string | null;
  clientTs: number;
};

export type SocketCallbacks = {
  onOpen: (reconnectMetadata: ReconnectMetadata) => void;
  onResume: () => void;
  /** Returns whether the client has synced past the last reconnect. */
  onMessage: (message: ServerMessage) => boolean;
  onServerDisconnectError?: ((message: string) => void) | undefined;
};

const serverDisconnectErrors = {
  // A known error, e.g. during a restart or push
  InternalServerError: { timeout: 1000 },
  // ErrorMetadata::overloaded() messages that we really should back off
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
  // startsWith so more info could be at the end
  for (const prefix of Object.keys(
    serverDisconnectErrors,
  ) as ServerDisconnectError[]) {
    if (s.startsWith(prefix)) {
      return prefix;
    }
  }
  return "Unknown";
}

let firstTime: number | undefined;
export function monotonicMillis() {
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

const DEFAULT_INITIAL_BACKOFF = 1000;
const MAX_BACKOFF = 16000;
// Pings arrive every 15s without other traffic, but one large message can hog
// the line, so the threshold is set higher.
const SERVER_INACTIVITY_THRESHOLD = 60000;

type SocketRuntime = {
  root: Frame;
  ws: WebSocket | null;
  lastCloseReason:
    | "InitialConnect"
    | "OnCloseInvoked"
    | (string & {}) // a full server error reason or a client one
    | null;
  transitionChunkBuffer: {
    chunks: string[];
    totalParts: number;
    transitionId: string;
  } | null;
  inactivityTimeout: ReturnType<typeof setTimeout> | null;
  scheduledReconnect: {
    timeout: ReturnType<typeof setTimeout>;
    scheduledAt: number;
    backoffMs: number;
  } | null;
  networkOnlineHandler: (() => void) | null;
  /** Sent as an event once the network recovery reconnect opens. */
  pendingNetworkRecoveryInfo: { timeSavedMs: number } | null;
};

export function reatomSocket(
  uri: string,
  options: {
    webSocketConstructor: typeof WebSocket;
    logger: Logger;
    debug: boolean;
  },
  callbacks: SocketCallbacks,
  name: string,
) {
  const { webSocketConstructor, logger, debug } = options;

  const state = atom<SocketState>({ state: "disconnected" }, `${name}.state`);
  const connectionCount = atom(0, `${name}.connectionCount`);
  /** Consecutive failed connections, reset once the client syncs past a reconnect. */
  const retries = atom(0, `${name}._retries`);
  const hasEverConnected = atom(false, `${name}.hasEverConnected`);
  const runtime = atom(
    (): SocketRuntime => ({
      root: context(),
      ws: null,
      lastCloseReason: "InitialConnect",
      transitionChunkBuffer: null,
      inactivityTimeout: null,
      scheduledReconnect: null,
      networkOnlineHandler: null,
      pendingNetworkRecoveryInfo: null,
    }),
    `${name}._runtime`,
  );

  // Socket events and timers arrive outside any frame.
  const inRoot = <Params extends any[]>(fn: (...params: Params) => void) =>
    wrap(fn, peek(runtime).root);

  const logVerbose = (message: string) => logger.logVerbose(message);

  const setState = (next: SocketState) => {
    state.set(next);
    logVerbose(
      `socket state changed: ${next.state}, paused: ${
        "paused" in next ? next.paused : undefined
      }`,
    );
  };

  const setupNetworkListener = () => {
    // React Native has window but not addEventListener.
    if (
      typeof window === "undefined" ||
      typeof window.addEventListener !== "function"
    ) {
      return;
    }
    const rt = peek(runtime);
    if (rt.networkOnlineHandler !== null) return;
    rt.networkOnlineHandler = inRoot(() => {
      logVerbose("network online event detected");
      tryReconnectImmediately();
    });
    window.addEventListener("online", rt.networkOnlineHandler);
    logVerbose("network online event listener registered");
  };

  const cleanupNetworkListener = () => {
    const rt = peek(runtime);
    if (
      rt.networkOnlineHandler &&
      typeof window !== "undefined" &&
      typeof window.removeEventListener === "function"
    ) {
      window.removeEventListener("online", rt.networkOnlineHandler);
      rt.networkOnlineHandler = null;
      logVerbose("network online event listener removed");
    }
  };

  const assembleTransition = (chunk: TransitionChunk): Transition | null => {
    const rt = peek(runtime);
    const buffer = rt.transitionChunkBuffer;
    if (
      chunk.partNumber < 0 ||
      chunk.partNumber >= chunk.totalParts ||
      chunk.totalParts === 0 ||
      (buffer &&
        (buffer.totalParts !== chunk.totalParts ||
          buffer.transitionId !== chunk.transitionId))
    ) {
      // Throwing doesn't crash the client, so clear the buffer.
      rt.transitionChunkBuffer = null;
      throw new Error("Invalid TransitionChunk");
    }
    const current = (rt.transitionChunkBuffer = buffer ?? {
      chunks: [],
      totalParts: chunk.totalParts,
      transitionId: chunk.transitionId,
    });
    if (chunk.partNumber !== current.chunks.length) {
      const expectedLength = current.chunks.length;
      rt.transitionChunkBuffer = null;
      throw new Error(
        `TransitionChunk received out of order: expected part ${expectedLength}, got ${chunk.partNumber}`,
      );
    }
    current.chunks.push(chunk.chunk);
    if (current.chunks.length < chunk.totalParts) return null;
    rt.transitionChunkBuffer = null;
    const transition = parseServerMessage(JSON.parse(current.chunks.join("")));
    if (transition.type !== "Transition") {
      throw new Error(
        `Expected Transition, got ${transition.type} after assembling chunks`,
      );
    }
    return transition;
  };

  // Socket timers outlive individual subscribers. terminate clears both
  // inactivity and reconnect timers; each reschedule cancels its predecessor.
  const resetServerInactivityTimeout = () => {
    if (state().state === "terminated") return;
    const rt = peek(runtime);
    if (rt.inactivityTimeout !== null) {
      clearTimeout(rt.inactivityTimeout);
      rt.inactivityTimeout = null;
    }
    rt.inactivityTimeout = setTimeout(
      inRoot(() => closeAndReconnect("InactiveServer")),
      SERVER_INACTIVITY_THRESHOLD,
    );
  };

  const reportLargeTransition = (
    transition: Transition,
    messageLength: number,
  ) => {
    if (
      transition.clientClockSkew === undefined ||
      transition.serverTs === undefined
    ) {
      return;
    }
    // clientClockSkew is how far behind the client clock is, inflated by the
    // latency of the Connect message.
    const transitionTransitTime =
      monotonicMillis() -
      transition.clientClockSkew -
      transition.serverTs / 1_000_000;
    const prettyTransitionTime = `${Math.round(transitionTransitTime)}ms`;
    const prettyMessageMB = `${Math.round(messageLength / 10_000) / 100}MB`;
    const bytesPerSecond = messageLength / (transitionTransitTime / 1000);
    const prettyBytesPerSecond = `${Math.round(bytesPerSecond / 10_000) / 100}MB per second`;
    logVerbose(
      `received ${prettyMessageMB} transition in ${prettyTransitionTime} at ${prettyBytesPerSecond}`,
    );
    // Shown to all users, so not too aggressive. `logger: false` silences them.
    if (messageLength > 20_000_000) {
      logger.log(
        `received query results totaling more that 20MB (${prettyMessageMB}) which will take a long time to download on slower connections`,
      );
    } else if (transitionTransitTime > 20_000) {
      logger.log(
        `received query results totaling ${prettyMessageMB} which took more than 20s to arrive (${prettyTransitionTime})`,
      );
    }
    if (debug) {
      send({
        type: "Event",
        eventType: "ClientReceivedTransition",
        event: { transitionTransitTime, messageLength },
      });
    }
  };

  const onOpen = (ws: WebSocket) => {
    logger.logVerbose("begin ws.onopen");
    const rt = peek(runtime);
    const current = state();
    if (current.state !== "connecting") {
      throw new Error("onopen called with socket not in connecting state");
    }
    const paused = current.paused === "yes" ? "uninitialized" : "no";
    rt.ws = ws;
    setState({ state: "ready", paused });
    resetServerInactivityTimeout();
    if (paused === "no") {
      hasEverConnected.set(true);
      callbacks.onOpen({
        connectionCount: connectionCount(),
        lastCloseReason: rt.lastCloseReason,
        clientTs: monotonicMillis(),
      });
    }
    if (rt.lastCloseReason !== "InitialConnect") {
      if (rt.lastCloseReason) {
        logger.log(
          "WebSocket reconnected at",
          prettyNow(),
          "after disconnect due to",
          rt.lastCloseReason,
        );
      } else {
        logger.log("WebSocket reconnected at", prettyNow());
      }
    }
    connectionCount.set((count) => count + 1);
    rt.lastCloseReason = null;
    if (rt.pendingNetworkRecoveryInfo !== null) {
      const { timeSavedMs } = rt.pendingNetworkRecoveryInfo;
      rt.pendingNetworkRecoveryInfo = null;
      send({
        type: "Event",
        eventType: "NetworkRecoveryReconnect",
        event: { timeSavedMs },
      });
      logger.log(
        `Network recovery reconnect saved ~${Math.round(timeSavedMs / 1000)}s of waiting`,
      );
    }
  };

  const onMessage = (data: string) => {
    resetServerInactivityTimeout();
    const rt = peek(runtime);
    const messageLength = data.length;
    let serverMessage = parseServerMessage(JSON.parse(data));
    logVerbose(`received ws message with type ${serverMessage.type}`);
    // A Ping only resets the inactivity timer.
    if (serverMessage.type === "Ping") return;
    if (serverMessage.type === "TransitionChunk") {
      const transition = assembleTransition(serverMessage);
      if (!transition) return;
      serverMessage = transition;
      logVerbose(`assembled full ws message of type ${serverMessage.type}`);
    }
    if (rt.transitionChunkBuffer !== null) {
      rt.transitionChunkBuffer = null;
      logger.log(
        `Received unexpected ${serverMessage.type} while buffering TransitionChunks`,
      );
    }
    if (serverMessage.type === "Transition") {
      reportLargeTransition(serverMessage, messageLength);
    }
    if (callbacks.onMessage(serverMessage)) {
      // Reset backoff once all outstanding work is complete.
      retries.set(0);
    }
  };

  const onClose = (event: CloseEvent) => {
    logVerbose("begin ws.onclose");
    const rt = peek(runtime);
    rt.transitionChunkBuffer = null;
    if (rt.lastCloseReason === null) {
      // event.reason is often an empty string
      rt.lastCloseReason = event.reason || `closed with code ${event.code}`;
    }
    if (
      event.code !== CLOSE_NORMAL &&
      event.code !== CLOSE_GOING_AWAY && // common when a mobile app is backgrounded
      event.code !== CLOSE_NO_STATUS &&
      event.code !== CLOSE_NOT_FOUND
    ) {
      let msg = `WebSocket closed with code ${event.code}`;
      if (event.reason) {
        msg += `: ${event.reason}`;
      }
      logger.log(msg);
      if (callbacks.onServerDisconnectError && event.reason) {
        callbacks.onServerDisconnectError(msg);
      }
    }
    scheduleReconnect(classifyDisconnectError(event.reason));
  };

  const connect = () => {
    const current = state().state;
    if (current === "terminated") return;
    if (current !== "disconnected" && current !== "stopped") {
      throw new Error(
        "Didn't start connection from disconnected state: " + current,
      );
    }
    const rt = peek(runtime);
    const ws = new webSocketConstructor(uri);
    logVerbose("constructed WebSocket");
    rt.ws = ws;
    setState({ state: "connecting", paused: "no" });
    // Started before onopen, so a failed handshake times out too.
    resetServerInactivityTimeout();
    ws.onopen = inRoot(() => onOpen(ws));
    // The WebSocket API calls `onclose` even if connecting fails, so every
    // error path goes through `onclose`.
    ws.onerror = inRoot((error: Event) => {
      rt.transitionChunkBuffer = null;
      const message = (error as ErrorEvent).message;
      if (message) {
        logger.log(`WebSocket error message: ${message}`);
      }
    });
    ws.onmessage = inRoot((message: MessageEvent) => onMessage(message.data));
    ws.onclose = inRoot(onClose);
  };

  const nextBackoff = (reason: "client" | ServerDisconnectError): number => {
    const initialBackoff: number =
      reason === "client"
        ? 100 // No evidence of a server problem, retry quickly
        : reason === "Unknown"
          ? DEFAULT_INITIAL_BACKOFF
          : serverDisconnectErrors[reason].timeout;
    const baseBackoff = initialBackoff * Math.pow(2, retries());
    retries.set((count) => count + 1);
    const actualBackoff = Math.min(baseBackoff, MAX_BACKOFF);
    const jitter = actualBackoff * (Math.random() - 0.5);
    return actualBackoff + jitter;
  };

  const scheduleReconnect = (reason: "client" | ServerDisconnectError) => {
    const rt = peek(runtime);
    if (rt.scheduledReconnect) {
      clearTimeout(rt.scheduledReconnect.timeout);
      rt.scheduledReconnect = null;
    }
    rt.ws = null;
    state.set({ state: "disconnected" });
    const backoff = nextBackoff(reason);
    logger.log(`Attempting reconnect in ${Math.round(backoff)}ms`);
    const scheduledAt = monotonicMillis();
    const timeout = setTimeout(
      inRoot(() => {
        if (rt.scheduledReconnect?.timeout === timeout) {
          rt.scheduledReconnect = null;
          connect();
        }
      }),
      backoff,
    );
    rt.scheduledReconnect = { timeout, scheduledAt, backoffMs: backoff };
  };

  /**
   * Detaches the current socket and closes it, clearing its handlers so a
   * late `onclose` does not reconnect. The caller sets the next state.
   */
  const close = (): Promise<void> => {
    const rt = peek(runtime);
    rt.transitionChunkBuffer = null;
    const current = state().state;
    const ws = rt.ws;
    if (ws === null || (current !== "connecting" && current !== "ready")) {
      return Promise.resolve();
    }
    rt.ws = null;
    // Messages can still arrive after close but nothing wants them.
    ws.onmessage = () => {
      logVerbose("Ignoring message received after close");
    };
    if (current === "connecting") {
      return new Promise((resolve) => {
        ws.onclose = () => {
          logVerbose("Closed after connecting");
          resolve();
        };
        ws.onopen = () => {
          logVerbose("Opened after connecting");
          ws.close();
        };
      });
    }
    logVerbose("ws.close called");
    const result = new Promise<void>((resolve) => {
      ws.onclose = () => resolve();
    });
    ws.close();
    return result;
  };

  /** Closes the socket after an error and schedules a reconnect. */
  const closeAndReconnect = (closeReason: string) => {
    logVerbose(`begin closeAndReconnect with reason ${closeReason}`);
    const current = state().state;
    if (current !== "connecting" && current !== "ready") return;
    peek(runtime).lastCloseReason = closeReason;
    void close();
    scheduleReconnect("client");
  };

  const tryReconnectImmediately = () => {
    logVerbose("tryReconnectImmediately called");
    const current = state().state;
    // Only while waiting to reconnect.
    if (current !== "disconnected") {
      logVerbose(
        `tryReconnectImmediately called but socket state is ${current}, no action taken`,
      );
      return;
    }
    const rt = peek(runtime);
    let timeSavedMs: number | null = null;
    if (rt.scheduledReconnect) {
      const elapsed = monotonicMillis() - rt.scheduledReconnect.scheduledAt;
      timeSavedMs = Math.max(0, rt.scheduledReconnect.backoffMs - elapsed);
      logVerbose(
        `would have waited ${Math.round(timeSavedMs)}ms more (backoff was ${Math.round(rt.scheduledReconnect.backoffMs)}ms, elapsed ${Math.round(elapsed)}ms)`,
      );
      clearTimeout(rt.scheduledReconnect.timeout);
      rt.scheduledReconnect = null;
      logVerbose("canceled scheduled reconnect");
    }
    logger.log("Network recovery detected, reconnecting immediately");
    rt.pendingNetworkRecoveryInfo =
      timeSavedMs !== null ? { timeSavedMs } : null;
    connect();
  };

  /** Sends when open and not paused; returns whether the message might have been sent. */
  const send = (message: ClientMessage): boolean => {
    const messageForLog = {
      type: message.type,
      ...(message.type === "Authenticate" && message.tokenType === "User"
        ? { value: `...${message.value.slice(-7)}` }
        : {}),
    };
    const current = state();
    const ws = peek(runtime).ws;
    if (current.state === "ready" && current.paused === "no" && ws) {
      const request = JSON.stringify(encodeClientMessage(message));
      let sent = false;
      try {
        ws.send(request);
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
  };

  const clearTimers = () => {
    const rt = peek(runtime);
    if (rt.inactivityTimeout !== null) {
      clearTimeout(rt.inactivityTimeout);
      rt.inactivityTimeout = null;
    }
    if (rt.scheduledReconnect) {
      clearTimeout(rt.scheduledReconnect.timeout);
      rt.scheduledReconnect = null;
    }
  };

  return {
    /** The state machine position, without the socket handle. */
    state,
    /** Times a socket reached `ready`. */
    connectionCount,
    retries,
    hasEverConnected,
    send,
    /** Opens the first socket. */
    start() {
      setupNetworkListener();
      connect();
    },
    /** Closes the socket until `tryRestart`. */
    stop(): Promise<void> {
      if (state().state === "terminated") return Promise.resolve();
      cleanupNetworkListener();
      const result = close();
      clearTimers();
      state.set({ state: "stopped" });
      return result;
    },
    /** Reconnects after `stop`, unless terminated meanwhile. */
    tryRestart() {
      if (state().state !== "stopped") {
        logger.logVerbose("Restart called without stopping first");
        return;
      }
      setupNetworkListener();
      connect();
    },
    pause() {
      const current = state();
      // Disconnected or stopped sockets have nothing to pause. A ready socket
      // that hasn't sent its handshake still needs `onOpen` on resume.
      if (current.state === "connecting") {
        state.set({ ...current, paused: "yes" });
      } else if (
        current.state === "ready" &&
        current.paused !== "uninitialized"
      ) {
        state.set({ ...current, paused: "yes" });
      }
    },
    resume() {
      const current = state();
      if (current.state === "connecting") {
        state.set({ state: "connecting", paused: "no" });
      } else if (current.state === "ready") {
        if (current.paused === "uninitialized") {
          state.set({ state: "ready", paused: "no" });
          hasEverConnected.set(true);
          callbacks.onOpen({
            connectionCount: connectionCount(),
            lastCloseReason: peek(runtime).lastCloseReason,
            clientTs: monotonicMillis(),
          });
        } else if (current.paused === "yes") {
          state.set({ state: "ready", paused: "no" });
          callbacks.onResume();
        }
      }
    },
    /** Closes the socket for good. */
    terminate(): Promise<void> {
      clearTimers();
      cleanupNetworkListener();
      const result = close();
      setState({ state: "terminated" });
      return result;
    },
  };
}

export type ReatomSocket = ReturnType<typeof reatomSocket>;
