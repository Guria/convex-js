/**
 * Upstream's `BaseConvexClient` API over a {@link ReatomConvexClient}, so the
 * parity tests can run upstream's scenarios against the Reatom client. Test
 * code only: the package doesn't export it.
 *
 * It runs in the Reatom context that was current when it was constructed.
 */
import { context, top, type Frame } from "@reatom/core";
import { Value } from "convex/values";
import { parseArgs } from "../../common/index.js";
import {
  BaseConvexClientOptions,
  ConnectionState,
  internals,
  MutationOptions,
  reatomConvexClient,
  ReatomConvexClient,
  ReatomConvexClientInternals,
  SubscribeOptions,
  Transition,
} from "./client.js";
import { functionError } from "./errors.js";
import { FunctionResult } from "../sync/function_result.js";
import {
  QueryJournal,
  RequestId,
  TS,
  UserIdentityAttributes,
} from "../sync/protocol.js";
import { QueryToken, serializePathAndArgs } from "../sync/udf_path_utils.js";

export type { AuthTokenFetcher } from "./auth.js";
import type { AuthTokenFetcher } from "./auth.js";
export type {
  BaseConvexClientOptions,
  ConnectionState,
  MutationOptions,
  QueryModification,
  SubscribeOptions,
  Transition,
} from "./client.js";

let defaultFrame: Frame | undefined;

/**
 * The root frame of the current Reatom context. Outside any frame, e.g.
 * after `clearStack()`, a context of its own.
 */
function currentRootFrame(): Frame {
  try {
    return top().root.frame;
  } catch {
    return (defaultFrame ??= context.start());
  }
}

export class BaseConvexClient {
  private readonly client: ReatomConvexClient;
  private readonly internal: ReatomConvexClientInternals;
  private readonly frame: Frame;
  private readonly connectionStateSubscribers = new Map<
    number,
    (connectionState: ConnectionState) => void
  >();
  private nextConnectionStateSubscriberId = 0;
  private lastPublishedConnectionState: string | undefined;

  /**
   * @param address - The url of your Convex deployment, often provided
   * by an environment variable. E.g. `https://small-mouse-123.convex.cloud`.
   * @param onTransition - A callback receiving an array of query tokens
   * corresponding to query results that have changed -- additional handlers
   * can be added via `addOnTransitionHandler`.
   * @param options - See {@link BaseConvexClientOptions} for a full description.
   */
  constructor(
    address: string,
    onTransition: (updatedQueries: QueryToken[]) => void,
    options?: BaseConvexClientOptions,
  ) {
    this.frame = currentRootFrame();
    this.client = reatomConvexClient(address, options, "convexClient");
    this.internal = internals(this.client);
    this.frame.run(() => {
      this.internal.addOnTransitionHandler((transition) => {
        onTransition(transition.queries.map((q) => q.token));
      });
      this.internal.start();
      // Published from the start, so a later subscriber waits for a change.
      this.client.connectionState.subscribe((connectionState) =>
        this.publishConnectionState(connectionState),
      );
    });
  }

  /**
   * The Reatom client holding this client's state.
   *
   * Read its atoms inside {@link BaseConvexClient.reatomFrame}.
   */
  get reatom(): ReatomConvexClient {
    return this.client;
  }

  /** The root frame of the Reatom context this client runs in. */
  get reatomFrame(): Frame {
    return this.frame;
  }

  private run<T>(fn: () => T): T {
    return this.frame.run(fn);
  }

  // Like the dirty flag of the connection state: publishes in a microtask,
  // only when something changed.
  private publishConnectionState(connectionState: ConnectionState) {
    void Promise.resolve().then(() => {
      const serialized = JSON.stringify(connectionState);
      if (serialized === this.lastPublishedConnectionState) return;
      this.lastPublishedConnectionState = serialized;
      for (const cb of this.connectionStateSubscribers.values()) {
        // A throwing callback stops the others but leaves the client intact.
        cb(connectionState);
      }
    });
  }

  getMaxObservedTimestamp(): TS | undefined {
    return this.run(() => this.internal.maxObservedTimestamp());
  }

  /**
   * Add a handler that will be called on a transition.
   *
   * Any external side effects (e.g. setting React state) should be handled here.
   *
   * @param fn
   *
   * @returns
   */
  addOnTransitionHandler(fn: (transition: Transition) => void) {
    return this.run(() => this.internal.addOnTransitionHandler(fn));
  }

  /**
   * Get the current JWT auth token and decoded claims.
   */
  getCurrentAuthClaims():
    | { token: string; decoded: Record<string, any> }
    | undefined {
    return this.run(() => this.client.authClaims());
  }

  /**
   * Set the authentication token to be used for subsequent queries and mutations.
   * `fetchToken` will be called automatically again if a token expires.
   * `fetchToken` should return `null` if the token cannot be retrieved, for example
   * when the user's rights were permanently revoked.
   * @param fetchToken - an async function returning the JWT-encoded OpenID Connect Identity Token
   * @param onChange - a callback that will be called when the authentication status changes
   * @param onRefreshChange - a callback called with `true` when the socket is paused to fetch a replacement token after a server rejection, and `false` when refresh completes
   */
  setAuth(
    fetchToken: AuthTokenFetcher,
    onChange: (isAuthenticated: boolean) => void,
    onRefreshChange?: (isRefreshing: boolean) => void,
  ) {
    this.run(() => this.client.setAuth(fetchToken, onChange, onRefreshChange));
  }

  hasAuth() {
    return this.run(() => this.internal.hasAuth());
  }

  /** @internal */
  setAdminAuth(value: string, fakeUserIdentity?: UserIdentityAttributes) {
    this.run(() => this.internal.setAdminAuth(value, fakeUserIdentity));
  }

  clearAuth() {
    this.run(() => this.client.clearAuth());
  }

  /**
   * Subscribe to a query function.
   *
   * Whenever this query's result changes, the `onTransition` callback
   * passed into the constructor will be called.
   *
   * @param name - The name of the query.
   * @param args - An arguments object for the query. If this is omitted, the
   * arguments will be `{}`.
   * @param options - A {@link SubscribeOptions} options object for this query.

   * @returns An object containing a {@link QueryToken} corresponding to this
   * query and an `unsubscribe` callback.
   */
  subscribe(
    name: string,
    args?: Record<string, Value>,
    options?: SubscribeOptions,
  ): { queryToken: QueryToken; unsubscribe: () => void } {
    const argsObject = parseArgs(args);
    return this.run(() => {
      const entry = this.internal.queryEntry(name, argsObject, options);
      const release = this.internal.retainQuery(entry);
      return {
        queryToken: entry.token,
        unsubscribe: () => this.run(release),
      };
    });
  }

  /**
   * A query result based only on the current, local state.
   *
   * The only way this will return a value is if we're already subscribed to the
   * query or its value has been set optimistically.
   */
  localQueryResult(
    udfPath: string,
    args?: Record<string, Value>,
  ): Value | undefined {
    return this.localQueryResultByToken(
      serializePathAndArgs(udfPath, parseArgs(args)),
    );
  }

  /**
   * Get query result by query token based on current, local state
   *
   * The only way this will return a value is if we're already subscribed to the
   * query or its value has been set optimistically.
   *
   * @internal
   */
  localQueryResultByToken(queryToken: QueryToken): Value | undefined {
    return this.run(() => this.internal.localQueryResultByToken(queryToken));
  }

  /**
   * Whether local query result is available for a token.
   *
   * This method does not throw if the result is an error.
   *
   * @internal
   */
  hasLocalQueryResultByToken(queryToken: QueryToken): boolean {
    return this.run(() => this.internal.hasLocalQueryResultByToken(queryToken));
  }

  /**
   * @internal
   */
  localQueryLogs(
    udfPath: string,
    args?: Record<string, Value>,
  ): string[] | undefined {
    const queryToken = serializePathAndArgs(udfPath, parseArgs(args));
    return this.run(
      () => this.internal.rawQueryResultByToken(queryToken)?.logLines,
    );
  }

  /**
   * Retrieve the current {@link QueryJournal} for this query function.
   *
   * If we have not yet received a result for this query, this will be `undefined`.
   *
   * @param name - The name of the query.
   * @param args - The arguments object for this query.
   * @returns The query's {@link QueryJournal} or `undefined`.
   */
  queryJournal(
    name: string,
    args?: Record<string, Value>,
  ): QueryJournal | undefined {
    const queryToken = serializePathAndArgs(name, parseArgs(args));
    return this.run(() => this.internal.queryJournal(queryToken));
  }

  /**
   * Get the current {@link ConnectionState} between the client and the Convex
   * backend.
   *
   * @returns The {@link ConnectionState} with the Convex backend.
   */
  connectionState(): ConnectionState {
    return this.run(() => this.client.connectionState());
  }

  /**
   * Subscribe to the {@link ConnectionState} between the client and the Convex
   * backend, calling a callback each time it changes.
   *
   * Subscribed callbacks will be called when any part of ConnectionState changes.
   * ConnectionState may grow in future versions (e.g. to provide a array of
   * inflight requests) in which case callbacks would be called more frequently.
   *
   * @returns An unsubscribe function to stop listening.
   */
  subscribeToConnectionState(
    cb: (connectionState: ConnectionState) => void,
  ): () => void {
    const id = this.nextConnectionStateSubscriberId++;
    this.connectionStateSubscribers.set(id, cb);
    return () => {
      this.connectionStateSubscribers.delete(id);
    };
  }

  /**
   * Execute a mutation function.
   *
   * @param name - The name of the mutation.
   * @param args - An arguments object for the mutation. If this is omitted,
   * the arguments will be `{}`.
   * @param options - A {@link MutationOptions} options object for this mutation.

   * @returns - A promise of the mutation's result.
   */
  async mutation(
    name: string,
    args?: Record<string, Value>,
    options?: MutationOptions,
  ): Promise<any> {
    const result = await this.mutationInternal(name, args, options);
    if (!result.success) throw functionError("mutation", name, result);
    return result.value;
  }

  /**
   * @internal
   */
  async mutationInternal(
    udfPath: string,
    args?: Record<string, Value>,
    options?: MutationOptions,
    componentPath?: string,
  ): Promise<FunctionResult> {
    const { mutationPromise } = this.enqueueMutation(
      udfPath,
      args,
      options,
      componentPath,
    );
    return mutationPromise;
  }

  /**
   * @internal
   */
  enqueueMutation(
    udfPath: string,
    args?: Record<string, Value>,
    options?: MutationOptions,
    componentPath?: string,
  ): { requestId: RequestId; mutationPromise: Promise<FunctionResult> } {
    return this.run(() =>
      this.internal.enqueueMutation(udfPath, args, options, componentPath),
    );
  }

  /**
   * Execute an action function.
   *
   * @param name - The name of the action.
   * @param args - An arguments object for the action. If this is omitted,
   * the arguments will be `{}`.
   * @returns A promise of the action's result.
   */
  async action(name: string, args?: Record<string, Value>): Promise<any> {
    const result = await this.actionInternal(name, args);
    if (!result.success) throw functionError("action", name, result);
    return result.value;
  }

  /**
   * @internal
   */
  async actionInternal(
    udfPath: string,
    args?: Record<string, Value>,
    componentPath?: string,
  ): Promise<FunctionResult> {
    return this.run(() =>
      this.internal.actionInternal(udfPath, args, componentPath),
    );
  }

  /**
   * Close any network handles associated with this client and stop all subscriptions.
   *
   * Call this method when you're done with an {@link BaseConvexClient} to
   * dispose of its sockets and resources.
   *
   * @returns A `Promise` fulfilled when the connection has been completely closed.
   */
  async close(): Promise<void> {
    return this.run(() => this.client.close());
  }

  /**
   * Return the address for this client, useful for creating a new client.
   *
   * Not guaranteed to match the address with which this client was constructed:
   * it may be canonicalized.
   */
  get url() {
    return this.client.url;
  }

  /**
   * @internal
   */
  get nextRequestId() {
    return this.run(() => this.internal.nextRequestId());
  }

  /**
   * @internal
   */
  get sessionId() {
    return this.run(() => this.internal.sessionId());
  }
}

/**
 * The public API of {@link BaseConvexClient}.
 *
 * @internal
 */
export interface BaseConvexClientInterface {
  addOnTransitionHandler(fn: (transition: Transition) => void): () => void;

  setAuth(
    fetchToken: AuthTokenFetcher,
    onChange: (isAuthenticated: boolean) => void,
    onRefreshChange?: (isRefreshing: boolean) => void,
  ): void;

  /** @internal */
  setAdminAuth(value: string, fakeUserIdentity?: UserIdentityAttributes): void;

  clearAuth(): void;

  subscribe(
    name: string,
    args?: Record<string, Value>,
    options?: SubscribeOptions,
  ): { queryToken: QueryToken; unsubscribe: () => void };

  localQueryResult(
    udfPath: string,
    args?: Record<string, Value>,
  ): Value | undefined;

  localQueryResultByToken(queryToken: QueryToken): Value | undefined;

  hasLocalQueryResultByToken(queryToken: QueryToken): boolean;

  localQueryLogs(
    udfPath: string,
    args?: Record<string, Value>,
  ): string[] | undefined;

  queryJournal(
    name: string,
    args?: Record<string, Value>,
  ): QueryJournal | undefined;

  connectionState(): ConnectionState;

  subscribeToConnectionState(
    cb: (connectionState: ConnectionState) => void,
  ): () => void;

  mutation(
    name: string,
    args?: Record<string, Value>,
    options?: MutationOptions,
  ): Promise<any>;

  action(name: string, args?: Record<string, Value>): Promise<any>;

  close(): Promise<void>;

  readonly url: string;
}
