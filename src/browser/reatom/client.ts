/**
 * A Convex sync client whose state lives in Reatom atoms.
 *
 * The query set is the set of connected query atoms: reading a query's
 * `result` in a subscribed graph subscribes it on the server, and losing the
 * last reader unsubscribes it. Query set changes made in one microtask go out
 * as one `ModifyQuerySet`. A server transition updates only the queries it
 * names; optimistic updates are replayed only while some are pending.
 *
 * State is per Reatom context: call the client's methods inside a frame of
 * the context that owns it.
 */
import {
  action,
  atom,
  computed,
  context,
  noop,
  peek,
  wrap,
  type Action,
  type Atom,
  type AtomLike,
  type Computed,
  type Frame,
} from "@reatom/core";
import { version } from "../../index.js";
import { parseArgs, validateDeploymentUrl } from "../../common/index.js";
import {
  FunctionArgs,
  FunctionReference,
  FunctionReference_future,
  FunctionReturnType,
  getFunctionName,
} from "convex/server";
import { UserIdentityAttributes } from "convex/server";
import { convexToJson, jsonToConvex, Value } from "convex/values";
import { jwtDecode } from "../../vendor/jwt-decode/index.js";
import { Long } from "../../vendor/long.js";
import {
  instantiateDefaultLogger,
  instantiateNoopLogger,
  logFatalError,
  logForFunction,
  Logger,
} from "../logging.js";
import { FunctionResult } from "../sync/function_result.js";
import {
  getMarksReport,
  mark as markPerformance,
  MarkName,
} from "../sync/metrics.js";
import { OptimisticLocalStore, OptimisticUpdate } from "convex/browser";
import {
  ActionRequest,
  ActionResponse,
  AddQuery,
  Authenticate,
  ClientMessage,
  IdentityVersion,
  MutationRequest,
  MutationResponse,
  QueryId,
  QueryJournal,
  QuerySetModification,
  RemoveQuery,
  RequestId,
  ServerMessage,
  StateVersion,
  Transition as ServerTransition,
  TS,
} from "../sync/protocol.js";
import { newSessionId } from "../sync/session.js";
import {
  canonicalizeUdfPath,
  QueryToken,
  serializePaginatedPathAndArgs,
} from "../sync/udf_path_utils.js";
import { AuthSync, AuthTokenFetcher, reatomAuth } from "./auth.js";
import { withConnectionHooks } from "./connection_hooks.js";
import { functionError } from "./errors.js";
import {
  OptimisticLocalStoreImpl,
  OptimisticUpdateAndId,
  Overlay,
  ServerResults,
} from "./optimistic.js";
import {
  PaginatedOptions,
  reatomPaginatedQuery,
  ReatomPaginatedQuery,
} from "./paginated.js";
import { reatomSocket, ReconnectMetadata, SocketState } from "./socket.js";

/**
 * Options for {@link BaseConvexClient}.
 *
 * @public
 */
export interface BaseConvexClientOptions {
  /**
   * Whether to prompt the user if they have unsaved changes pending
   * when navigating away or closing a web page.
   *
   * This is only possible when the `window` object exists, i.e. in a browser.
   *
   * The default value is `true` in browsers.
   */
  unsavedChangesWarning?: boolean;
  /**
   * Specifies an alternate
   * [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
   * constructor to use for client communication with the Convex cloud.
   * The default behavior is to use `WebSocket` from the global environment.
   */
  webSocketConstructor?: typeof WebSocket;
  /**
   * Adds additional logging for debugging purposes.
   *
   * The default value is `false`.
   */
  verbose?: boolean;
  /**
   * A logger, `true`, or `false`. If not provided or `true`, logs to the console.
   * If `false`, logs are not printed anywhere.
   *
   * You can construct your own logger to customize logging to log elsewhere.
   * A logger is an object with 4 methods: log(), warn(), error(), and logVerbose().
   * These methods can receive multiple arguments of any types, like console.log().
   */
  logger?: Logger | boolean;
  /**
   * Sends additional metrics to Convex for debugging purposes.
   *
   * The default value is `false`.
   */
  reportDebugInfoToConvex?: boolean;
  /**
   * This API is experimental: it may change or disappear.
   *
   * A function to call on receiving abnormal WebSocket close messages from the
   * connected Convex deployment. The content of these messages is not stable,
   * it is an implementation detail that may change.
   *
   * Consider this API an observability stopgap until higher level codes with
   * recommendations on what to do are available, which could be a more stable
   * interface instead of `string`.
   *
   * Check `connectionState` for more quantitative metrics about connection status.
   */
  onServerDisconnectError?: (message: string) => void;
  /**
   * Skip validating that the Convex deployment URL looks like
   * `https://happy-animal-123.convex.cloud` or localhost.
   *
   * This can be useful if running a self-hosted Convex backend that uses a different
   * URL.
   *
   * The default value is `false`
   */
  skipConvexDeploymentUrlCheck?: boolean;
  /**
   * If using auth, the number of seconds before a token expires that we should refresh it.
   *
   * The default value is `10`.
   */
  authRefreshTokenLeewaySeconds?: number;
  /**
   * This API is experimental: it may change or disappear.
   *
   * Whether query, mutation, and action requests should be held back
   * until the first auth token can be sent.
   *
   * Opting into this behavior works well for pages that should
   * only be viewed by authenticated clients.
   *
   * Defaults to false, not waiting for an auth token.
   */
  expectAuth?: boolean;
  /**
   * This API is experimental: it may change or disappear.
   *
   * When true, the client reuses the initial cached auth token instead
   * of immediately fetching a fresh one. This avoids a second
   * Authenticate message that causes the server to re-execute all
   * authenticated queries.
   *
   * The cached token's remaining lifetime is estimated using the
   * server's clock skew measurement, and a refresh is scheduled
   * before it expires.
   *
   * Defaults to false, preserving the original behavior of immediately
   * fetching a fresh token after the cached token is confirmed.
   */
  initialAuthTokenReuse?: boolean;
}

/**
 * State describing the client's connection with the Convex backend.
 *
 * @public
 */
export type ConnectionState = {
  hasInflightRequests: boolean;
  isWebSocketConnected: boolean;
  timeOfOldestInflightRequest: Date | null;
  /**
   * True if the client has ever opened a WebSocket to the "ready" state.
   */
  hasEverConnected: boolean;
  /**
   * The number of times this client has connected to the Convex backend.
   *
   * A number of things can cause the client to reconnect -- server errors,
   * bad internet, auth expiring. But this number being high is an indication
   * that the client is having trouble keeping a stable connection.
   */
  connectionCount: number;
  /**
   * The number of times this client has tried (and failed) to connect to the Convex backend.
   */
  connectionRetries: number;
  /**
   * The number of mutations currently in flight.
   */
  inflightMutations: number;
  /**
   * The number of actions currently in flight.
   */
  inflightActions: number;
};

/**
 * Options for {@link BaseConvexClient.subscribe}.
 *
 * @public
 */
export interface SubscribeOptions {
  /**
   * An (optional) journal produced from a previous execution of this query
   * function.
   *
   * If there is an existing subscription to a query function with the same
   * name and arguments, this journal will have no effect.
   */
  journal?: QueryJournal;

  /**
   * @internal
   */
  componentPath?: string | undefined;
}

/**
 * Options for {@link BaseConvexClient.mutation}.
 *
 * @public
 */
export interface MutationOptions {
  /**
   * An optimistic update to apply along with this mutation.
   *
   * An optimistic update locally updates queries while a mutation is pending.
   * Once the mutation completes, the update will be rolled back.
   */
  optimisticUpdate?: OptimisticUpdate<any> | undefined;
}

/**
 * Type describing updates to a query within a `Transition`.
 *
 * @public
 */
export type QueryModification =
  // `undefined` generally comes from an optimistic update setting the query to be loading
  { kind: "Updated"; result: FunctionResult | undefined } | { kind: "Removed" };

/**
 * Object describing a transition passed into the `onTransition` handler.
 *
 * These can be from receiving a transition from the server, or from applying an
 * optimistic update locally.
 *
 * @public
 */
export type Transition = {
  queries: Array<{ token: QueryToken; modification: QueryModification }>;
  reflectedMutations: Array<{ requestId: RequestId; result: FunctionResult }>;
  timestamp: TS;
};

/**
 * The status of a {@link ReatomQuery}.
 *
 * @public
 */
export type QueryStatus = "loading" | "success" | "error";

/**
 * One query function and arguments on a {@link ReatomConvexClient}.
 *
 * Reading any of its atoms inside a subscribed Reatom graph subscribes the
 * query on the server; it is unsubscribed once nothing reads it. Equal
 * queries share one entry and one server subscription.
 *
 * @public
 */
export interface ReatomQuery<T = Value> {
  readonly token: QueryToken;
  /** The canonical function path. */
  readonly udfPath: string;
  readonly args: Record<string, Value>;
  /**
   * The function result with optimistic updates applied, `undefined` while
   * loading.
   */
  readonly result: AtomLike<FunctionResult | undefined, []>;
  /** The value, `undefined` while loading or after an error. */
  readonly data: Computed<T | undefined>;
  /** The error the query failed with, a `ConvexError` when it carries data. */
  readonly error: Computed<Error | undefined>;
  readonly status: Computed<QueryStatus>;
}

/**
 * Options for {@link ReatomConvexClient.query}.
 *
 * @public
 */
export type ReatomQueryOptions = SubscribeOptions;

type AuthToken =
  | { tokenType: "User"; value: string }
  | {
      tokenType: "Admin";
      value: string;
      impersonating?: UserIdentityAttributes | undefined;
    };

type Request = {
  message: MutationRequest | ActionRequest;
  requestedAt: Date;
  resolve: (result: FunctionResult) => void;
} & (
  | { status: "NotSent" | "Requested" }
  | { status: "Completed"; result: FunctionResult; ts: TS }
);

type QueryRef =
  | FunctionReference<"query">
  | FunctionReference_future<"query">
  | string;
type MutationRef =
  | FunctionReference<"mutation">
  | FunctionReference_future<"mutation">
  | string;
type ActionRef =
  | FunctionReference<"action">
  | FunctionReference_future<"action">
  | string;
type AnyRef = FunctionReference<any, any> | FunctionReference_future<any, any>;
type ReturnOf<Ref> = Ref extends AnyRef ? FunctionReturnType<Ref> : any;
type PaginatedItemOf<Ref> =
  ReturnOf<Ref> extends { page: Array<infer Item> } ? Item : any;
type ArgsOf<Ref> = Ref extends AnyRef
  ? FunctionArgs<Ref>
  : Record<string, Value>;

const functionName = (ref: { toString(): string } | string): string =>
  typeof ref === "string" ? ref : getFunctionName(ref as any);

const INITIAL_VERSION: StateVersion = {
  querySet: 0,
  ts: Long.fromNumber(0),
  identity: 0,
};

type QueryEntry = ReatomQuery & {
  /** The runtime of the context the entry belongs to. */
  readonly rt: ClientRuntime;
  /** Whether some Reatom subscriber reads `result`. */
  connected: boolean;
  /** Subscriptions through `BaseConvexClient`, which bypass Reatom's. */
  retained: number;
  readonly result: Atom<FunctionResult | undefined>;
  readonly componentPath: string | undefined;
  /** Assigned while the server knows the query or is about to. */
  id: QueryId | null;
  /** A query set change not sent yet. */
  op: "Add" | "Remove" | null;
  /** The last server result on this subscription. */
  server: FunctionResult | undefined;
  /** The result behind `result`: `server` or an optimistic one. */
  visible: FunctionResult | undefined;
  journal: QueryJournal | undefined;
};

/** Entries by token, held weakly where the runtime allows it. */
class WeakCache<Value extends { readonly token: string }> {
  private readonly refs = new Map<string, { deref(): Value | undefined }>();
  private readonly registry =
    typeof FinalizationRegistry === "undefined"
      ? null
      : new FinalizationRegistry<string>((token) => {
          if (!this.refs.get(token)?.deref()) this.refs.delete(token);
        });

  get(token: string) {
    return this.refs.get(token)?.deref();
  }

  set(value: Value) {
    if (typeof WeakRef === "undefined") {
      this.refs.set(value.token, { deref: () => value });
      return;
    }
    this.refs.set(value.token, new WeakRef(value));
    this.registry?.register(value, value.token);
  }

  /** Forgets an unused value where it can't be collected. */
  release(value: Value) {
    if (typeof WeakRef === "undefined" && this.get(value.token) === value) {
      this.refs.delete(value.token);
    }
  }
}

/** Handles, indexes and counters that belong to one context's client. */
type ClientRuntime = {
  root: Frame;
  started: boolean;
  closed: boolean;
  sessionId: string;
  entries: WeakCache<QueryEntry>;
  paginated: WeakCache<ReatomPaginatedQuery>;
  byId: Map<QueryId, QueryEntry>;
  nextQueryId: QueryId;
  /** Entries with a query set change to send, by query id. */
  pending: Map<QueryId, QueryEntry>;
  flushScheduled: boolean;
  flush: () => void;
  /** Paused for an auth token: query set and identity changes wait. */
  paused: boolean;
  /** Queries re-sent on reconnect the server has not answered yet. */
  outstandingQueriesOlderThanRestart: Set<QueryId>;
  outstandingAuthOlderThanRestart: boolean;
  /** Auth was cleared while paused; resume tells the server. */
  clearAuthOnResume: boolean;
  requestsOlderThanRestart: Set<RequestId>;
  overlay: Overlay;
  nextRequestId: RequestId;
  firstMessageReceived: boolean;
  transitionHandlers: Set<(transition: Transition) => void>;
  onBeforeUnload: ((event: BeforeUnloadEvent) => void) | null;
};

function webSocketUri(address: string) {
  const i = address.search("://");
  if (i === -1) {
    throw new Error("Provided address was not an absolute URL.");
  }
  const origin = address.substring(i + 3);
  const protocol = address.substring(0, i);
  let wsProtocol;
  if (protocol === "http") {
    wsProtocol = "ws";
  } else if (protocol === "https") {
    wsProtocol = "wss";
  } else {
    throw new Error(`Unknown parent protocol ${protocol}`);
  }
  return `${wsProtocol}://${origin}/api/${version}/sync`;
}

/**
 * Create a Convex client whose state lives in Reatom atoms.
 *
 * The client connects on first use in a context: a query read, a request,
 * `setAuth` or a `connectionState` subscriber. Call its methods inside a
 * Reatom frame; each context gets its own connection.
 *
 * ```ts
 * const convex = reatomConvexClient(import.meta.env.VITE_CONVEX_URL);
 * const notes = computed(() => convex.query(api.notes.list, {}).data());
 * ```
 *
 * @param address - The url of your Convex deployment, e.g.
 * `https://small-mouse-123.convex.cloud`.
 * @param options - See {@link BaseConvexClientOptions}.
 * @param name - A prefix for the client's atom names.
 *
 * @public
 */
export function reatomConvexClient(
  address: string,
  options: BaseConvexClientOptions = {},
  name = "convex",
): ReatomConvexClient {
  if (typeof address === "object") {
    throw new Error(
      "Passing a ClientConfig object is no longer supported. Pass the URL of the Convex deployment as a string directly.",
    );
  }
  if (options.skipConvexDeploymentUrlCheck !== true) {
    validateDeploymentUrl(address);
  }
  if (!options.webSocketConstructor && typeof WebSocket === "undefined") {
    throw new Error(
      "No WebSocket global variable defined! To use Convex in an environment without WebSocket try the HTTP client: https://docs.convex.dev/api/classes/browser.ConvexHttpClient",
    );
  }
  const webSocketConstructor = options.webSocketConstructor || WebSocket;
  const debug = options.reportDebugInfoToConvex ?? false;
  const logger =
    options.logger === false
      ? instantiateNoopLogger({ verbose: options.verbose ?? false })
      : options.logger !== true && options.logger
        ? options.logger
        : instantiateDefaultLogger({ verbose: options.verbose ?? false });
  const wsUri = webSocketUri(address);
  const hasWindow =
    typeof window !== "undefined" &&
    typeof window.addEventListener !== "undefined";
  if (!hasWindow && options.unsavedChangesWarning === true) {
    throw new Error(
      "unsavedChangesWarning requested, but window.addEventListener not found! Remove {unsavedChangesWarning: true} from Convex client options.",
    );
  }

  const querySetVersion = atom(0, `${name}.querySetVersion`);
  const identityVersion = atom<IdentityVersion>(0, `${name}.identityVersion`);
  /** The version of the query results received on the current connection. */
  const remoteVersion = atom(INITIAL_VERSION, `${name}.remoteVersion`);
  const maxObservedTimestamp = atom<TS | undefined>(
    undefined,
    `${name}.maxObservedTimestamp`,
  );
  /** The token the server is told about. */
  const authToken = atom<AuthToken | undefined>(undefined, `${name}.authToken`);
  /**
   * Server side of the two-phase auth: `null` while a token waits for the
   * server, then whether it accepted it. `false` without auth.
   */
  const isAuthenticated = atom<boolean | null>(
    false,
    `${name}.isAuthenticated`,
  );
  /** The server rejected a confirmed token and a replacement is being fetched. */
  const isRefreshingAuth = atom(false, `${name}.isRefreshingAuth`);
  /** Mutations and actions sent or waiting to be sent. */
  const requests = atom<ReadonlyMap<RequestId, Request>>(
    new Map(),
    `${name}.requests`,
  );
  /** Optimistic updates of mutations the client has not seen reflected yet. */
  const optimisticUpdates = atom<readonly OptimisticUpdateAndId[]>(
    [],
    `${name}.optimisticUpdates`,
  );

  const runtime = atom(
    (): ClientRuntime => ({
      root: context(),
      started: false,
      closed: false,
      sessionId: newSessionId(),
      entries: new WeakCache(),
      paginated: new WeakCache(),
      byId: new Map(),
      nextQueryId: 0,
      pending: new Map(),
      flushScheduled: false,
      flush: noop,
      paused: false,
      outstandingQueriesOlderThanRestart: new Set(),
      outstandingAuthOlderThanRestart: false,
      clearAuthOnResume: false,
      requestsOlderThanRestart: new Set(),
      overlay: new Map(),
      nextRequestId: 0,
      firstMessageReceived: false,
      transitionHandlers: new Set(),
      onBeforeUnload: null,
    }),
    `${name}._runtime`,
  );

  const lookup = (token: QueryToken) => peek(runtime).entries.get(token);

  /** Shows `raw` behind `entry.result`; returns whether it changed. */
  const setVisible = (entry: QueryEntry, raw: FunctionResult | undefined) => {
    if (entry.visible === raw) return false;
    entry.visible = raw;
    entry.result.set(raw);
    return true;
  };

  const emitTransition = (transition: () => Transition) => {
    const handlers = peek(runtime).transitionHandlers;
    if (handlers.size === 0) return;
    const value = transition();
    for (const handler of handlers) handler(value);
  };

  const mark = (markName: MarkName) => {
    if (debug) markPerformance(markName, peek(runtime).sessionId);
  };

  // Optimistic updates

  const serverResults: ServerResults = {
    get(token) {
      const entry = lookup(token);
      return entry?.id === null ? undefined : entry?.server;
    },
    *all() {
      for (const entry of peek(runtime).byId.values()) {
        if (entry.server === undefined) continue;
        yield {
          token: entry.token,
          udfPath: entry.udfPath,
          args: entry.args,
          result: entry.server,
        };
      }
    },
  };

  /**
   * Shows the server results of `changed` under the optimistic updates still
   * pending, dropping those of `completed` requests.
   */
  const ingest = (
    changed: QueryEntry[],
    completed: Map<RequestId, FunctionResult>,
    // Queries the server removed: shown, but not reported, as in convex@1.48.0.
    removed: ReadonlySet<QueryEntry> = new Set(),
  ) => {
    const rt = peek(runtime);
    let updates = optimisticUpdates();
    if (completed.size > 0 && updates.length > 0) {
      updates = optimisticUpdates.set(
        updates.filter(({ mutationId }) => !completed.has(mutationId)),
      );
    }
    const queries: Transition["queries"] = [];
    const show = (entry: QueryEntry, raw: FunctionResult | undefined) => {
      if (setVisible(entry, raw) && !removed.has(entry)) {
        queries.push({
          token: entry.token,
          modification: { kind: "Updated", result: raw },
        });
      }
    };
    if (updates.length === 0 && rt.overlay.size === 0) {
      for (const entry of changed) show(entry, entry.server);
    } else {
      const previous = rt.overlay;
      const overlay: Overlay = (rt.overlay = new Map());
      const store = new OptimisticLocalStoreImpl(overlay, serverResults);
      for (const { update } of updates) update(store);
      const tokens = new Set([...previous.keys(), ...overlay.keys()]);
      for (const entry of changed) tokens.add(entry.token);
      for (const token of tokens) {
        const entry = lookup(token);
        const local = overlay.get(token);
        if (entry) {
          show(entry, local ? local.result : entry.server);
        } else if (local && local.result !== previous.get(token)?.result) {
          queries.push({
            token,
            modification: { kind: "Updated", result: local.result },
          });
        }
      }
    }
    emitTransition(() => ({
      queries,
      reflectedMutations: Array.from(completed, ([requestId, result]) => ({
        requestId,
        result,
      })),
      timestamp: remoteVersion().ts,
    }));
  };

  const applyOptimisticUpdate = (
    update: OptimisticUpdateAndId["update"],
    mutationId: RequestId,
  ) => {
    optimisticUpdates.set((list) => [...list, { update, mutationId }]);
    const { overlay } = peek(runtime);
    const store = new OptimisticLocalStoreImpl(overlay, serverResults);
    update(store);
    const queries: Transition["queries"] = [];
    for (const token of store.modifiedQueries) {
      const local = overlay.get(token)!;
      const entry = lookup(token);
      if (entry) setVisible(entry, local.result);
      queries.push({
        token,
        modification: { kind: "Updated", result: local.result },
      });
    }
    emitTransition(() => ({
      queries,
      reflectedMutations: [],
      timestamp: remoteVersion().ts,
    }));
  };

  // Query set

  const addQuery = (entry: QueryEntry): AddQuery => ({
    type: "Add",
    queryId: entry.id!,
    udfPath: entry.udfPath,
    args: [convexToJson(entry.args)],
    journal: entry.journal,
    componentPath: entry.componentPath,
  });

  /** The server no longer has `entry`, or never will. */
  const forget = (entry: QueryEntry) => {
    const { rt } = entry;
    rt.byId.delete(entry.id!);
    rt.outstandingQueriesOlderThanRestart.delete(entry.id!);
    entry.id = null;
    entry.op = null;
    entry.server = undefined;
    entry.journal = undefined;
    setVisible(entry, rt.overlay.get(entry.token)?.result);
    rt.entries.release(entry);
  };

  const takePending = (rt: ClientRuntime) => {
    const modifications: Array<AddQuery | RemoveQuery> = [];
    for (const entry of rt.pending.values()) {
      if (entry.op === "Add") {
        modifications.push(addQuery(entry));
        entry.op = null;
      } else {
        modifications.push({ type: "Remove", queryId: entry.id! });
        forget(entry);
      }
    }
    rt.pending.clear();
    return modifications;
  };

  const flush = () => {
    const rt = peek(runtime);
    rt.flushScheduled = false;
    if (rt.pending.size === 0 || rt.paused || rt.closed) return;
    const baseVersion = querySetVersion();
    const modification: QuerySetModification = {
      type: "ModifyQuerySet",
      baseVersion,
      newVersion: baseVersion + 1,
      modifications: takePending(rt),
    };
    querySetVersion.set(baseVersion + 1);
    socket.send(modification);
  };

  const scheduleFlush = (rt: ClientRuntime) => {
    if (rt.flushScheduled) return;
    rt.flushScheduled = true;
    if (rt.flush === noop) rt.flush = wrap(flush, rt.root);
    queueMicrotask(rt.flush);
  };

  const connectEntry = (entry: QueryEntry) => {
    const { rt } = entry;
    if (!rt.started) start();
    if (rt.closed) return;
    // Resubscribed before its removal went out: the server still has it.
    if (entry.op === "Remove") {
      entry.op = null;
      rt.pending.delete(entry.id!);
      return;
    }
    const queryId = rt.nextQueryId++;
    entry.id = queryId;
    entry.op = "Add";
    rt.byId.set(queryId, entry);
    rt.pending.set(queryId, entry);
    scheduleFlush(rt);
  };

  const disconnectEntry = (entry: QueryEntry) => {
    const { rt } = entry;
    if (entry.id === null) return;
    // Never sent: nothing to remove.
    if (entry.op === "Add") {
      rt.pending.delete(entry.id);
      forget(entry);
      return;
    }
    rt.outstandingQueriesOlderThanRestart.delete(entry.id);
    entry.op = "Remove";
    rt.pending.set(entry.id, entry);
    scheduleFlush(rt);
  };

  const createEntry = (
    rt: ClientRuntime,
    token: QueryToken,
    udfPath: string,
    args: Record<string, Value>,
    queryOptions: ReatomQueryOptions | undefined,
  ): QueryEntry => {
    const visible = rt.overlay.get(token)?.result;
    const result = atom<FunctionResult | undefined>(
      visible,
      `${name}.query#${udfPath}`,
    );
    let data: Computed<Value | undefined> | undefined;
    let error: Computed<Error | undefined> | undefined;
    let status: Computed<QueryStatus> | undefined;
    const entry: QueryEntry = {
      rt,
      connected: false,
      retained: 0,
      token,
      udfPath,
      args,
      result,
      componentPath: queryOptions?.componentPath,
      id: null,
      op: null,
      server: undefined,
      visible,
      journal: queryOptions?.journal,
      get data() {
        return (data ??= computed(() => {
          const current = result();
          return current?.success ? current.value : undefined;
        }, `${result.name}.data`));
      },
      get error() {
        return (error ??= computed(() => {
          const current = result();
          return current && !current.success
            ? functionError("query", udfPath, current)
            : undefined;
        }, `${result.name}.error`));
      },
      get status() {
        return (status ??= computed((): QueryStatus => {
          const current = result();
          return current === undefined
            ? "loading"
            : current.success
              ? "success"
              : "error";
        }, `${result.name}.status`));
      },
    };
    result.extend(
      withConnectionHooks(
        () => {
          entry.connected = true;
          if (entry.retained === 0) connectEntry(entry);
        },
        () => {
          entry.connected = false;
          if (entry.retained === 0) disconnectEntry(entry);
        },
      ),
    );
    rt.entries.set(entry);
    return entry;
  };

  /** The entry for a query, created on first use. */
  const queryEntry = (
    udfPath: string,
    args: Record<string, Value>,
    queryOptions?: ReatomQueryOptions,
  ): QueryEntry => {
    const rt = peek(runtime);
    const canonical = canonicalizeUdfPath(udfPath);
    // `serializePathAndArgs` without canonicalizing the path again.
    const token = JSON.stringify({
      udfPath: canonical,
      args: convexToJson(args),
    }) as QueryToken;
    const existing = rt.entries.get(token);
    if (existing === undefined) {
      return createEntry(rt, token, canonical, args, queryOptions);
    }
    // A journal only applies to a new subscription.
    if (existing.id === null && queryOptions?.journal !== undefined) {
      existing.journal = queryOptions.journal;
    }
    return existing;
  };

  // Identity

  const setAuthToken = (token: AuthToken): Authenticate => {
    authToken.set(token);
    const baseVersion = identityVersion();
    if (!peek(runtime).paused) identityVersion.set(baseVersion + 1);
    return { type: "Authenticate", baseVersion, ...token } as Authenticate;
  };

  const clearAuthToken = (): Authenticate => {
    const rt = peek(runtime);
    authToken.set(undefined);
    rt.outstandingAuthOlderThanRestart = false;
    const baseVersion = identityVersion();
    if (!rt.paused) identityVersion.set(baseVersion + 1);
    return { type: "Authenticate", tokenType: "None", baseVersion };
  };

  const authSync: AuthSync = {
    isCurrentOrNewerAuthVersion: (version) => version >= identityVersion(),
    markAuthCompletion: () => {
      peek(runtime).outstandingAuthOlderThanRestart = false;
    },
    token: () => authToken()?.value,
    hasAuth: () => authToken() !== undefined,
    authenticate: (value) => {
      socket.send(setAuthToken({ tokenType: "User", value }));
    },
    dropAuth: () => void clearAuthToken(),
    clearAuth: () => {
      // A paused socket drops the message.
      if (peek(runtime).paused) peek(runtime).clearAuthOnResume = true;
      socket.send(clearAuthToken());
    },
    stopSocket: () => socket.stop(),
    tryRestartSocket: () => socket.tryRestart(),
    pauseSocket: () => {
      socket.pause();
      peek(runtime).paused = true;
    },
    resumeSocket: () => socket.resume(),
  };

  // Requests

  const updateRequests = (
    update: (requests: Map<RequestId, Request>) => void,
  ) =>
    requests.set((current) => {
      const next = new Map(current);
      update(next);
      return next;
    });

  const request = (message: MutationRequest | ActionRequest) => {
    const sent = socket.send(message);
    return new Promise<FunctionResult>((resolve) => {
      updateRequests((all) =>
        all.set(message.requestId, {
          message,
          status: sent ? "Requested" : "NotSent",
          requestedAt: new Date(),
          resolve,
        }),
      );
    });
  };

  /** Returns the result when the request is done and its update can be dropped. */
  const onResponse = (
    response: MutationResponse | ActionResponse,
  ): { requestId: RequestId; result: FunctionResult } | null => {
    const rt = peek(runtime);
    const current = requests().get(response.requestId);
    // A request no longer tracked, e.g. a mutation answered again after a
    // reconnect, or one `restart` already re-sent and completed.
    if (current === undefined || current.status === "Completed") return null;
    const udfType = current.message.type === "Mutation" ? "mutation" : "action";
    const udfPath = current.message.udfPath;
    for (const line of response.logLines) {
      logForFunction(logger, "info", udfType, udfPath, line);
    }
    let result: FunctionResult;
    if (response.success) {
      result = {
        success: true,
        logLines: response.logLines,
        value: jsonToConvex(response.result),
      };
    } else {
      const errorMessage = response.result as string;
      logForFunction(logger, "error", udfType, udfPath, errorMessage);
      result = {
        success: false,
        errorMessage,
        errorData:
          response.errorData !== undefined
            ? jsonToConvex(response.errorData)
            : undefined,
        logLines: response.logLines,
      };
    }
    // A failed mutation changed nothing, and actions are not ordered with
    // queries, so both resolve now.
    if (response.type === "ActionResponse" || !response.success) {
      current.resolve(result);
      updateRequests((all) => all.delete(response.requestId));
      rt.requestsOlderThanRestart.delete(response.requestId);
      return { requestId: response.requestId, result };
    }
    // A mutation resolves once a transition reaches its timestamp, so the
    // caller reads its own write.
    updateRequests((all) =>
      all.set(response.requestId, {
        ...current,
        status: "Completed",
        result,
        ts: response.ts,
      }),
    );
    return null;
  };

  const removeCompleted = (ts: TS) => {
    const rt = peek(runtime);
    const completed = new Map<RequestId, FunctionResult>();
    for (const [requestId, current] of requests()) {
      if (current.status !== "Completed" || !current.ts.lessThanOrEqual(ts)) {
        continue;
      }
      current.resolve(current.result);
      completed.set(requestId, current.result);
      rt.requestsOlderThanRestart.delete(requestId);
    }
    if (completed.size > 0) {
      updateRequests((all) => completed.forEach((_, id) => all.delete(id)));
    }
    return completed;
  };

  /**
   * Requests to send on a new connection. Mutations are idempotent and go
   * again, even completed ones, so the server transitions the client past
   * them. In-flight actions might have run, so they fail.
   */
  const restartRequests = () => {
    const rt = peek(runtime);
    rt.requestsOlderThanRestart = new Set(requests().keys());
    const messages: ClientMessage[] = [];
    const lost: Request[] = [];
    updateRequests((all) => {
      for (const [requestId, current] of all) {
        if (current.status === "NotSent") {
          all.set(requestId, { ...current, status: "Requested" });
          messages.push(current.message);
        } else if (current.message.type === "Mutation") {
          messages.push(current.message);
        } else {
          all.delete(requestId);
          rt.requestsOlderThanRestart.delete(requestId);
          lost.push(current);
        }
      }
    });
    for (const { resolve } of lost) {
      resolve({
        success: false,
        errorMessage: "Connection lost while action was in flight",
        logLines: [],
      });
    }
    return messages;
  };

  const resumeRequests = () => {
    const messages: ClientMessage[] = [];
    updateRequests((all) => {
      for (const [requestId, current] of all) {
        if (current.status !== "NotSent") continue;
        all.set(requestId, { ...current, status: "Requested" });
        messages.push(current.message);
      }
    });
    return messages;
  };

  const hasIncompleteRequests = () => {
    for (const current of requests().values()) {
      if (current.status === "Requested") return true;
    }
    return false;
  };

  const timeOfOldestInflightRequest = (
    pending: ReadonlyMap<RequestId, Request>,
  ) => {
    if (pending.size === 0) return null;
    let oldest = Date.now();
    for (const current of pending.values()) {
      if (current.status !== "Completed") {
        oldest = Math.min(oldest, current.requestedAt.getTime());
      }
    }
    return new Date(oldest);
  };

  const tryReportLongDisconnect = () => {
    if (!debug) return;
    const oldest = timeOfOldestInflightRequest(requests());
    if (oldest === null || Date.now() - oldest.getTime() <= 60 * 1000) return;
    fetch(`${address}/api/debug_event`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Convex-Client": `npm-${version}`,
      },
      body: JSON.stringify({ event: "LongWebsocketDisconnect" }),
    })
      .then((response) => {
        if (!response.ok) {
          logger.warn("Analytics request failed with response:", response.body);
        }
      })
      .catch((error) => {
        logger.warn("Analytics response failed with error:", error);
      });
  };

  // Socket

  const observedTimestamp = (observedTs: TS) => {
    const max = maxObservedTimestamp();
    if (max === undefined || max.lessThanOrEqual(observedTs)) {
      maxObservedTimestamp.set(observedTs);
    }
  };

  const hasSyncedPastLastReconnect = () => {
    const rt = peek(runtime);
    return (
      rt.requestsOlderThanRestart.size === 0 &&
      rt.outstandingQueriesOlderThanRestart.size === 0 &&
      !rt.outstandingAuthOlderThanRestart
    );
  };

  const onOpen = (reconnectMetadata: ReconnectMetadata) => {
    const rt = peek(runtime);
    mark("convexWebSocketOpen");
    socket.send({
      ...reconnectMetadata,
      type: "Connect",
      sessionId: rt.sessionId,
      maxObservedTimestamp: maxObservedTimestamp(),
    });
    // A new connection starts from an empty query set and no identity:
    // re-add every query, resend mutations and reauthenticate.
    remoteVersion.set(INITIAL_VERSION);
    rt.paused = false;
    rt.clearAuthOnResume = false;
    for (const entry of rt.pending.values()) {
      if (entry.op === "Remove") forget(entry);
      else entry.op = null;
    }
    rt.pending.clear();
    rt.outstandingQueriesOlderThanRestart = new Set(rt.byId.keys());
    const querySet: QuerySetModification = {
      type: "ModifyQuerySet",
      baseVersion: 0,
      newVersion: 1,
      modifications: Array.from(rt.byId.values(), addQuery),
    };
    querySetVersion.set(1);
    const token = authToken();
    if (token === undefined) {
      // The server starts without an identity too.
      identityVersion.set(0);
    } else {
      rt.outstandingAuthOlderThanRestart = true;
      identityVersion.set(1);
      socket.send({
        type: "Authenticate",
        baseVersion: 0,
        ...token,
      } as Authenticate);
    }
    socket.send(querySet);
    for (const message of restartRequests()) socket.send(message);
  };

  const onResume = () => {
    const rt = peek(runtime);
    let querySet: QuerySetModification | undefined;
    if (rt.pending.size > 0) {
      const baseVersion = querySetVersion();
      querySet = {
        type: "ModifyQuerySet",
        baseVersion,
        newVersion: baseVersion + 1,
        modifications: takePending(rt),
      };
      querySetVersion.set(baseVersion + 1);
    }
    const token = authToken();
    let authenticate: Authenticate | undefined;
    if (token !== undefined) {
      const baseVersion = identityVersion();
      identityVersion.set(baseVersion + 1);
      authenticate = {
        type: "Authenticate",
        baseVersion,
        ...token,
      } as Authenticate;
    } else if (rt.clearAuthOnResume) {
      const baseVersion = identityVersion();
      identityVersion.set(baseVersion + 1);
      authenticate = { type: "Authenticate", tokenType: "None", baseVersion };
    }
    rt.clearAuthOnResume = false;
    rt.paused = false;
    if (authenticate) socket.send(authenticate);
    if (querySet) socket.send(querySet);
    for (const message of resumeRequests()) socket.send(message);
  };

  const onTransition = (message: ServerTransition) => {
    const rt = peek(runtime);
    observedTimestamp(message.endVersion.ts);
    auth.onTransition(message);
    const start = message.startVersion;
    const current = remoteVersion();
    if (
      current.querySet !== start.querySet ||
      current.ts.notEquals(start.ts) ||
      current.identity !== start.identity
    ) {
      throw new Error(
        `Invalid start version: ${start.ts.toString()}:${start.querySet}:${start.identity}, transitioning from ${current.ts.toString()}:${current.querySet}:${current.identity}`,
      );
    }
    const changed: QueryEntry[] = [];
    const removed = new Set<QueryEntry>();
    for (const modification of message.modifications) {
      rt.outstandingQueriesOlderThanRestart.delete(modification.queryId);
      // Results for queries already unsubscribed are dropped.
      const entry = rt.byId.get(modification.queryId);
      if (entry === undefined) continue;
      switch (modification.type) {
        case "QueryUpdated": {
          for (const line of modification.logLines) {
            logForFunction(logger, "info", "query", entry.udfPath, line);
          }
          entry.server = {
            success: true,
            value: jsonToConvex(modification.value ?? null),
            logLines: modification.logLines,
          };
          if (modification.journal !== undefined) {
            entry.journal = modification.journal;
          }
          break;
        }
        case "QueryFailed": {
          for (const line of modification.logLines) {
            logForFunction(logger, "info", "query", entry.udfPath, line);
          }
          const { errorData } = modification;
          entry.server = {
            success: false,
            errorMessage: modification.errorMessage,
            errorData:
              errorData !== undefined ? jsonToConvex(errorData) : undefined,
            logLines: modification.logLines,
          };
          if (modification.journal !== undefined) {
            entry.journal = modification.journal;
          }
          break;
        }
        case "QueryRemoved": {
          entry.server = undefined;
          removed.add(entry);
          break;
        }
        default: {
          modification satisfies never;
          throw new Error(`Invalid modification ${(modification as any).type}`);
        }
      }
      changed.push(entry);
    }
    remoteVersion.set(message.endVersion);
    ingest(changed, removeCompleted(message.endVersion.ts), removed);
  };

  const onMessage = (serverMessage: ServerMessage) => {
    const rt = peek(runtime);
    // Once per client: metrics grow with reconnects.
    if (!rt.firstMessageReceived) {
      rt.firstMessageReceived = true;
      mark("convexFirstMessageReceived");
      if (debug) {
        socket.send({
          type: "Event",
          eventType: "ClientConnect",
          event: getMarksReport(rt.sessionId),
        });
      }
    }
    switch (serverMessage.type) {
      case "Transition": {
        onTransition(serverMessage);
        break;
      }
      case "MutationResponse": {
        if (serverMessage.success) {
          observedTimestamp(serverMessage.ts);
        }
        const completed = onResponse(serverMessage);
        if (completed !== null) {
          ingest([], new Map([[completed.requestId, completed.result]]));
        }
        break;
      }
      case "ActionResponse": {
        onResponse(serverMessage);
        break;
      }
      case "AuthError": {
        auth.onAuthError(serverMessage);
        break;
      }
      case "FatalError": {
        const error = logFatalError(logger, serverMessage.error);
        void socket.terminate();
        throw error;
      }
      default: {
        serverMessage satisfies never;
      }
    }
    return hasSyncedPastLastReconnect();
  };

  const socket = reatomSocket(
    wsUri,
    { webSocketConstructor, logger, debug },
    {
      onOpen,
      onResume,
      onMessage,
      onServerDisconnectError: options.onServerDisconnectError,
    },
    `${name}.socket`,
  );

  const auth = reatomAuth(
    authSync,
    {
      logger,
      refreshTokenLeewaySeconds: options.authRefreshTokenLeewaySeconds ?? 10,
      initialAuthTokenReuse: options.initialAuthTokenReuse ?? false,
    },
    `${name}.auth`,
  );

  /** Opens the socket on first use in this context. */
  const start = () => {
    const rt = peek(runtime);
    if (rt.started || rt.closed) return;
    rt.started = true;
    if (hasWindow && options.unsavedChangesWarning !== false) {
      // Prompt on tab close while requests are unconfirmed.
      rt.onBeforeUnload = wrap((e: BeforeUnloadEvent) => {
        if (!hasIncompleteRequests()) return;
        e.preventDefault();
        // Most browsers don't show this message, but some still need it set.
        const confirmationMessage =
          "Are you sure you want to leave? Your changes may not be saved.";
        ((e || (window as any).event) as any).returnValue = confirmationMessage;
        return confirmationMessage;
      }, rt.root);
      window.addEventListener("beforeunload", rt.onBeforeUnload);
    }
    socket.start();
    mark("convexClientConstructed");
    // Wait for the first auth token.
    if (options.expectAuth) authSync.pauseSocket();
  };

  const connectionState = computed((): ConnectionState => {
    const pending = requests();
    let inflightMutations = 0;
    for (const current of pending.values()) {
      if (current.message.type === "Mutation") inflightMutations++;
    }
    return {
      hasInflightRequests: pending.size > 0,
      isWebSocketConnected: socket.state().state === "ready",
      hasEverConnected: socket.hasEverConnected(),
      connectionCount: socket.connectionCount(),
      connectionRetries: socket.retries(),
      timeOfOldestInflightRequest: timeOfOldestInflightRequest(pending),
      inflightMutations,
      inflightActions: pending.size - inflightMutations,
    };
  }, `${name}.connectionState`).extend(withConnectionHooks(start, noop));

  const authClaims = computed(
    (): { token: string; decoded: Record<string, any> } | undefined => {
      const token = authToken();
      if (token?.tokenType !== "User") return undefined;
      let decoded: Record<string, any>;
      try {
        decoded = jwtDecode(token.value);
      } catch {
        decoded = {};
      }
      return { token: token.value, decoded };
    },
    `${name}.authClaims`,
  );

  const enqueueMutation = (
    udfPath: string,
    args?: Record<string, Value>,
    mutationOptions?: MutationOptions,
    componentPath?: string,
  ): { requestId: RequestId; mutationPromise: Promise<FunctionResult> } => {
    start();
    const mutationArgs = parseArgs(args);
    tryReportLongDisconnect();
    const rt = peek(runtime);
    const requestId = rt.nextRequestId++;
    const optimisticUpdate = mutationOptions?.optimisticUpdate;
    if (optimisticUpdate !== undefined) {
      applyOptimisticUpdate((store: OptimisticLocalStore) => {
        const result: unknown = optimisticUpdate(store, mutationArgs);
        if (result instanceof Promise) {
          logger.warn(
            "Optimistic update handler returned a Promise. Optimistic updates should be synchronous.",
          );
        }
      }, requestId);
    }
    const mutationPromise = request({
      type: "Mutation",
      requestId,
      udfPath,
      componentPath,
      args: [convexToJson(mutationArgs)],
    });
    return { requestId, mutationPromise };
  };

  const actionInternal = (
    udfPath: string,
    args?: Record<string, Value>,
    componentPath?: string,
  ): Promise<FunctionResult> => {
    start();
    const actionArgs = parseArgs(args);
    const requestId = peek(runtime).nextRequestId++;
    tryReportLongDisconnect();
    return request({
      type: "Action",
      requestId,
      udfPath,
      componentPath,
      args: [convexToJson(actionArgs)],
    });
  };

  const runMutation = action(
    async (
      udfPath: string,
      args?: Record<string, Value>,
      mutationOptions?: MutationOptions,
    ): Promise<any> => {
      const result = await wrap(
        enqueueMutation(udfPath, args, mutationOptions).mutationPromise,
      );
      if (!result.success) throw functionError("mutation", udfPath, result);
      return result.value;
    },
    `${name}.mutation`,
  );

  const runAction = action(
    async (udfPath: string, args?: Record<string, Value>): Promise<any> => {
      const result = await wrap(actionInternal(udfPath, args));
      if (!result.success) throw functionError("action", udfPath, result);
      return result.value;
    },
    `${name}.action`,
  );

  const setAuth = action(
    (
      fetchToken: AuthTokenFetcher,
      onChange?: (isAuthenticated: boolean) => void,
      onRefreshChange?: (isRefreshing: boolean) => void,
    ) => {
      start();
      isAuthenticated.set(null);
      isRefreshingAuth.set(false);
      // The flow outlives the caller's frame, so it runs in the root.
      wrap(
        () =>
          void auth.setConfig(
            fetchToken,
            (value) => {
              isAuthenticated.set(value);
              onChange?.(value);
            },
            (value) => {
              isRefreshingAuth.set(value);
              onRefreshChange?.(value);
            },
          ),
        peek(runtime).root,
      )();
    },
    `${name}.setAuth`,
  );

  const clearAuth = action(() => {
    start();
    auth.stop();
    // Forget the token before resuming a pause for a token fetch, so the
    // resume doesn't send it, then tell the server.
    authToken.set(undefined);
    socket.resume();
    socket.tryRestart();
    authSync.clearAuth();
    isAuthenticated.set(false);
    isRefreshingAuth.set(false);
  }, `${name}.clearAuth`);

  const setAdminAuth = action(
    (value: string, fakeUserIdentity?: UserIdentityAttributes) => {
      start();
      socket.send(
        setAuthToken({
          tokenType: "Admin",
          value,
          impersonating: fakeUserIdentity,
        }),
      );
    },
    `${name}.setAdminAuth`,
  );

  const close = action((): Promise<void> => {
    const rt = peek(runtime);
    rt.closed = true;
    auth.stop();
    if (rt.onBeforeUnload) {
      window.removeEventListener("beforeunload", rt.onBeforeUnload);
      rt.onBeforeUnload = null;
    }
    if (!rt.started) return Promise.resolve();
    return socket.terminate();
  }, `${name}.close`);

  /** The paginated query for `options`, created on first use. */
  const paginatedQueryEntry = (
    udfPath: string,
    args: Record<string, Value>,
    paginatedOptions: PaginatedOptions,
  ): ReatomPaginatedQuery => {
    const canonical = canonicalizeUdfPath(udfPath);
    const token = serializePaginatedPathAndArgs(
      canonical,
      args,
      paginatedOptions,
    );
    const { paginated } = peek(runtime);
    const existing = paginated.get(token);
    if (existing !== undefined) return existing;
    const created = reatomPaginatedQuery(
      (pageUdfPath, pageArgs, paginationOpts) =>
        queryEntry(pageUdfPath, { ...pageArgs, paginationOpts }),
      canonical,
      args,
      paginatedOptions,
      `${name}.paginatedQuery#${canonical}`,
    );
    paginated.set(created);
    return created;
  };

  /** A result held locally, with optimistic updates applied. */
  const rawQueryResultByToken = (
    queryToken: QueryToken,
  ): FunctionResult | undefined => {
    const entry = lookup(queryToken);
    return entry !== undefined
      ? entry.visible
      : peek(runtime).overlay.get(queryToken)?.result;
  };

  /** Like {@link rawQueryResultByToken}, but throws a failed query's error. */
  const localQueryResultByToken = (
    queryToken: QueryToken,
  ): Value | undefined => {
    const result = rawQueryResultByToken(queryToken);
    if (result === undefined) return undefined;
    if (result.success) return result.value;
    const udfPath =
      lookup(queryToken)?.udfPath ??
      peek(runtime).overlay.get(queryToken)!.udfPath;
    throw functionError("query", udfPath, result);
  };

  const client: ReatomConvexClient = {
    name,
    url: address,
    connectionState,
    isAuthenticated,
    isRefreshingAuth,
    authClaims,
    socketState: socket.state,
    query: (query, args, queryOptions) =>
      queryEntry(
        functionName(query),
        parseArgs(args as Record<string, Value> | undefined),
        queryOptions,
      ) as ReatomQuery<any>,
    paginatedQuery: (query, args, paginatedOptions) =>
      paginatedQueryEntry(
        functionName(query),
        parseArgs(args as Record<string, Value>),
        {
          initialNumItems: paginatedOptions.initialNumItems,
          id: paginatedOptions.id ?? -1,
        },
      ) as ReatomPaginatedQuery<any>,
    mutation: (mutation, args, mutationOptions) =>
      runMutation(
        functionName(mutation),
        args as Record<string, Value> | undefined,
        mutationOptions,
      ),
    action: (action, args) =>
      runAction(
        functionName(action),
        args as Record<string, Value> | undefined,
      ),
    setAuth,
    clearAuth,
    close,
  };
  INTERNALS.set(client, {
    setAdminAuth,
    localQueryResultByToken,
    rawQueryResultByToken,
    hasLocalQueryResultByToken: (queryToken) =>
      rawQueryResultByToken(queryToken) !== undefined,
    queryJournal: (queryToken) => {
      const entry = lookup(queryToken);
      return entry?.id === null ? undefined : entry?.journal;
    },
    queryEntry,
    retainQuery: (entry) => {
      const queryEntry = entry as QueryEntry;
      if (queryEntry.retained++ === 0 && !queryEntry.connected) {
        connectEntry(queryEntry);
      }
      let released = false;
      return () => {
        if (released) return;
        released = true;
        if (--queryEntry.retained === 0 && !queryEntry.connected) {
          disconnectEntry(queryEntry);
        }
      };
    },
    paginatedQueryEntry,
    enqueueMutation,
    actionInternal,
    addOnTransitionHandler: (handler) => {
      const { transitionHandlers } = peek(runtime);
      transitionHandlers.add(handler);
      return () => transitionHandlers.delete(handler);
    },
    start,
    sessionId: () => peek(runtime).sessionId,
    nextRequestId: () => peek(runtime).nextRequestId,
    hasAuth: () => authToken() !== undefined,
    maxObservedTimestamp: () => maxObservedTimestamp(),
    authState: () => auth.state().state,
    optimisticUpdates: () => optimisticUpdates().length,
  });
  return client;
}

/**
 * A Convex client whose state lives in Reatom atoms, created with
 * {@link reatomConvexClient}. Use it inside a Reatom frame.
 *
 * @public
 */
export interface ReatomConvexClient {
  /** The prefix of the client's atom names. */
  readonly name: string;
  /** The deployment address. */
  readonly url: string;
  /** The connection with the backend; subscribing connects. */
  readonly connectionState: Computed<ConnectionState>;
  /**
   * Whether the server accepted the current token: `null` while a token waits
   * for the server, `false` without auth.
   */
  readonly isAuthenticated: AtomLike<boolean | null, []>;
  /** The server rejected a confirmed token and a replacement is being fetched. */
  readonly isRefreshingAuth: AtomLike<boolean, []>;
  /** The current user JWT and its decoded claims. */
  readonly authClaims: Computed<
    { token: string; decoded: Record<string, any> } | undefined
  >;
  /** The socket state machine. */
  readonly socketState: AtomLike<SocketState, []>;
  /**
   * A query of this client. Equal queries return the same entry.
   *
   * @param query - A query reference, or a function path.
   * @param args - The query arguments, `{}` by default.
   */
  query<Query extends QueryRef>(
    query: Query,
    args?: ArgsOf<Query>,
    options?: ReatomQueryOptions,
  ): ReatomQuery<ReturnOf<Query>>;
  /**
   * A paginated query of this client. Equal queries with the same `id` share
   * their pages; `id` is `-1` by default.
   *
   * @param query - A paginated query reference, or a function path.
   * @param args - The query arguments without `paginationOpts`.
   */
  paginatedQuery<Query extends QueryRef>(
    query: Query,
    args: Omit<ArgsOf<Query>, "paginationOpts">,
    options: { initialNumItems: number; id?: number },
  ): ReatomPaginatedQuery<PaginatedItemOf<Query>>;
  /** Run a mutation; `optimisticUpdate` edits local results until it is reflected. */
  mutation<Mutation extends MutationRef>(
    mutation: Mutation,
    args?: ArgsOf<Mutation>,
    options?: MutationOptions,
  ): Promise<ReturnOf<Mutation>>;
  action<Action extends ActionRef>(
    action: Action,
    args?: ArgsOf<Action>,
  ): Promise<ReturnOf<Action>>;
  /**
   * Fetch a token, send it, and refetch it before it expires or when the
   * server rejects it. `fetchToken` should return `null` when no token can be
   * had, for example after the user's rights were revoked.
   */
  readonly setAuth: Action<
    [
      fetchToken: AuthTokenFetcher,
      onChange?: ((isAuthenticated: boolean) => void) | undefined,
      onRefreshChange?: ((isRefreshing: boolean) => void) | undefined,
    ],
    void
  >;
  /** Stop the auth flow and continue without an identity. */
  readonly clearAuth: Action<[], void>;
  /** Close the connection for good. */
  readonly close: Action<[], Promise<void>>;
}

/**
 * What `BaseConvexClient` and `PaginatedQueryClient` need beyond the public API.
 *
 * @internal
 */
export type ReatomConvexClientInternals = {
  setAdminAuth: (
    value: string,
    fakeUserIdentity?: UserIdentityAttributes,
  ) => void;
  localQueryResultByToken: (queryToken: QueryToken) => Value | undefined;
  rawQueryResultByToken: (queryToken: QueryToken) => FunctionResult | undefined;
  hasLocalQueryResultByToken: (queryToken: QueryToken) => boolean;
  queryJournal: (queryToken: QueryToken) => QueryJournal | undefined;
  queryEntry: (
    udfPath: string,
    args: Record<string, Value>,
    options?: ReatomQueryOptions,
  ) => ReatomQuery & { result: Atom<FunctionResult | undefined> };
  /** Keeps the query subscribed until the returned function runs. */
  retainQuery: (entry: ReatomQuery) => () => void;
  paginatedQueryEntry: (
    udfPath: string,
    args: Record<string, Value>,
    options: PaginatedOptions,
  ) => ReatomPaginatedQuery;
  enqueueMutation: (
    udfPath: string,
    args?: Record<string, Value>,
    options?: MutationOptions,
    componentPath?: string,
  ) => { requestId: RequestId; mutationPromise: Promise<FunctionResult> };
  actionInternal: (
    udfPath: string,
    args?: Record<string, Value>,
    componentPath?: string,
  ) => Promise<FunctionResult>;
  /** Calls `handler` synchronously with each change to local query results. */
  addOnTransitionHandler: (
    handler: (transition: Transition) => void,
  ) => () => boolean;
  /** Opens the socket in this context. */
  start: () => void;
  sessionId: () => string;
  nextRequestId: () => RequestId;
  hasAuth: () => boolean;
  maxObservedTimestamp: () => TS | undefined;
  authState: () => string;
  /** Optimistic updates still pending. */
  optimisticUpdates: () => number;
};

const INTERNALS = new WeakMap<
  ReatomConvexClient,
  ReatomConvexClientInternals
>();

/** @internal */
export function internals(
  client: ReatomConvexClient,
): ReatomConvexClientInternals {
  const found = INTERNALS.get(client);
  if (!found) throw new Error("Not a client created by reatomConvexClient");
  return found;
}
