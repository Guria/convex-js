import {
  Base64,
  ConvexError,
  convexToJson,
  jsonToConvex,
  Value,
} from "../../values/index.js";
import { Long } from "../../vendor/long.js";

// --experimental-vm-modules which we use for jest doesn't support named exports
import WebSocket, { WebSocketServer } from "ws";
// Node's own, also under DOM test environments.
import { MessageChannel } from "node:worker_threads";

// Let's pretend this ws WebSocket is a browser WebSocket (it's very close)
export const nodeWebSocket = WebSocket as unknown as typeof window.WebSocket;

import { ClientMessage, WireServerMessage } from "./protocol.js";
import {
  PaginatedQueryToken,
  QueryToken,
  serializedQueryTokenIsPaginated,
} from "./udf_path_utils.js";
import { BaseConvexClient } from "./client.js";
import { PaginatedQueryClient } from "./paginated_query_client.js";

export type InMemoryWebSocketTest = (args: {
  address: string;
  socket: () => WebSocket;
  receive: () => Promise<ClientMessage>;
  send: (message: WireServerMessage) => void;
  close: () => void;
}) => Promise<void>;

function listeningSocketServer(): Promise<WebSocketServer> {
  return new Promise((resolve) => {
    const wss = new WebSocketServer({ port: 0 });
    wss.on("listening", () => resolve(wss));
  });
}

// Run a test with a real node WebSocket instance connected
export async function withInMemoryWebSocket(
  cb: InMemoryWebSocketTest,
  debug = false,
) {
  // These state variables are consistent over multiple sockets.
  let received: (msg: string) => void;
  // prettier-ignore
  const messages: Promise<string>[] = [ new Promise((r) => { received = r; }) ];
  let socket: WebSocket | null = null;

  const wss = await listeningSocketServer();

  const setUpSocket = () => {
    wss.once("connection", function connection(ws: WebSocket) {
      socket = ws;
      ws.on("message", function message(data: string) {
        received(data);
        // eslint-disable-next-line no-console
        if (debug) console.debug(`client --${JSON.parse(data).type}--> `);
        messages.push(
          new Promise((r) => {
            received = r;
          }),
        );
      });
    });
  };
  setUpSocket();

  // receive and send are stable across multiple socket connections
  async function receive(): Promise<ClientMessage> {
    const msgP = messages.shift();
    if (!msgP) {
      throw new Error("Receive() called twice? No message promise found.");
    }
    const text = await msgP;
    const structured = JSON.parse(text);
    return structured;
  }
  function send(message: WireServerMessage) {
    // eslint-disable-next-line no-console
    if (debug) console.debug(`      <--${message.type}-- server`);
    socket!.send(encodeServerMessage(message));
  }

  const s: any = wss.address();
  const address = typeof s === "string" ? s : `http://127.0.0.1:${s.port}`;

  try {
    await cb({
      address,
      socket: () => socket as unknown as WebSocket,
      receive,
      send,
      close: () => {
        // eslint-disable-next-line no-console
        if (debug) console.debug(`           --CLOSE-->8-- server`);
        socket!.close();
        setUpSocket();
      },
    });
  } finally {
    const s = socket!;
    if (s!) {
      socket!.close();
    }
    wss.close();
  }
}

export function encodeServerMessage(message: WireServerMessage): string {
  function replacer(_key: string, value: any) {
    if (Long.isLong(value)) {
      return encodeLong(value);
    }
    return value;
  }
  return JSON.stringify(message, replacer);
}

function encodeLong(n: Long) {
  const integerBytes = Uint8Array.from(n.toBytesLE());
  return Base64.fromByteArray(integerBytes);
}

/**
 * const q = new UpdateQueue();
 * const client = new BaseConvexClient(address, queryTokens => { q.onTransition(client)(queryTokens) });
 *
 * await q.updatePromises[3];
 *
 */
export class UpdateQueue {
  updateResolves: ((v: Record<QueryToken, any>) => void)[];
  updatePromises: Promise<Record<QueryToken, any>>[];
  updates: Record<QueryToken, any>[];
  allResults: Record<QueryToken, any>;
  nextIndex: number;

  constructor(maxLength = 10) {
    this.updateResolves = [];
    this.updatePromises = [];
    this.allResults = {};
    this.updates = [];
    this.nextIndex = 0;

    let nextResolve: (v: Record<QueryToken, any>) => void;
    let nextPromise: Promise<Record<QueryToken, any>>;

    for (let i = 0; i < maxLength; i++) {
      nextPromise = new Promise((r) => {
        nextResolve = r;
      });
      this.updateResolves.push(nextResolve!);
      this.updatePromises.push(nextPromise);
    }
  }

  /**
   * Useful to use instead of directly awaiting so that the timeout has a line number
   * unlike the default Vite test timeout.
   */
  async awaitPromiseAtIndexWithTimeout(
    i: number,
  ): Promise<Record<QueryToken | PaginatedQueryToken, any>> {
    if (!this.updatePromises[i]) {
      throw new Error("That promise doesn't exist yet");
    }
    const inBandSignal = "UpdateQueue await timed out";
    const result = await Promise.race([
      new Promise((r) => setTimeout(() => r(inBandSignal), 1000)),
      this.updatePromises[i],
    ]);
    if (result === inBandSignal) {
      throw new Error("Awaiting promise in UpdateQueue");
    }
    // cast from the updatePromises where this was any, but know it's unknown
    return result as any;
  }

  onTransition =
    (client: BaseConvexClient, paginatedClient?: PaginatedQueryClient) =>
    (updatedQueryTokens: (QueryToken | PaginatedQueryToken)[]) => {
      const update: Record<QueryToken, any> = {};
      for (const queryToken of updatedQueryTokens) {
        if (serializedQueryTokenIsPaginated(queryToken)) {
          if (!paginatedClient) {
            throw new Error(
              "No PaginatedQueryClient provided to look up value for token " +
                queryToken,
            );
          }
          const value = paginatedClient?.localQueryResultByToken(queryToken);
          update[queryToken] = value;
          this.allResults[queryToken] = value;
        } else {
          const value = client.localQueryResultByToken(queryToken);
          update[queryToken] = value;
          this.allResults[queryToken] = value;
        }
      }
      this.updateResolves[this.nextIndex](update);
      this.updates.push(update);
      this.nextIndex++;
    };
}

export type FakeIdentity = { subject: string } & Record<string, unknown>;

export type FakeFunctionContext = {
  identity: FakeIdentity | null;
  /** Per-subscription scratch space, e.g. a page's pinned end cursor. */
  memo: Record<string, unknown>;
};

type FakeFunction = (
  ctx: FakeFunctionContext,
  args: Record<string, Value>,
) => unknown;

type FakeSubscription = {
  udfPath: string;
  args: Record<string, Value>;
  memo: Record<string, unknown>;
  /** Last result sent, so only changes are sent. */
  sent?: string;
};

type FakeConnection = {
  socket: FakeSocketEnd;
  queries: Map<number, FakeSubscription>;
  identity: FakeIdentity | null;
  querySetVersion: number;
  identityVersion: number;
  sentVersion: { querySet: number; identity: number; ts: number };
};

type FakeSocketEnd = {
  readyState: number;
  deliver(data: string): void;
  closed(code: number, reason: string): void;
};

const failure = (error: unknown) => ({
  message: error instanceof Error ? error.message : String(error),
  errorData:
    error instanceof ConvexError
      ? convexToJson(error.data as Value)
      : undefined,
});

const base64url = (value: unknown) =>
  btoa(JSON.stringify(value))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");

let jwtCount = 0;
/** An unsigned JWT; the client reads `iat` and `exp` to schedule refetches. */
export function fakeJwt(
  claims: Record<string, unknown>,
  lifetimeSeconds = 3600,
) {
  const iat = Math.floor(Date.now() / 1000);
  const payload = {
    iat,
    exp: iat + lifetimeSeconds,
    jti: String(++jwtCount),
    ...claims,
  };
  return `${base64url({ alg: "none" })}.${base64url(payload)}.fake`;
}

export function readFakeJwt(token: string): Record<string, unknown> {
  const payload = token.split(".")[1] ?? "";
  return JSON.parse(atob(payload.replace(/-/g, "+").replace(/_/g, "/")));
}

/**
 * Cursor pagination over items with ids. Like the real backend, a page pins
 * its end once loaded, so items inserted later grow the page.
 */
export function fakePaginate<T extends { _id: string }>(
  ctx: FakeFunctionContext,
  items: T[],
  options: Value,
) {
  const { numItems, cursor, endCursor } = options as {
    numItems: number;
    cursor: string | null;
    endCursor?: string | null;
  };
  const start =
    cursor === null ? 0 : items.findIndex((item) => item._id === cursor) + 1;
  const pinned = endCursor ?? ctx.memo["endCursor"];
  const pinnedEnd =
    typeof pinned === "string"
      ? items.findIndex((item) => item._id === pinned)
      : -1;
  const end = pinnedEnd >= 0 ? pinnedEnd + 1 : start + numItems;
  const page = items.slice(start, end);
  const isDone = end >= items.length;
  const continueCursor = page.at(-1)?._id ?? cursor ?? "";
  if (!isDone) ctx.memo["endCursor"] = continueCursor;
  return { page, isDone, continueCursor };
}

/**
 * An in-process Convex deployment speaking the sync protocol. Pass
 * `server.WebSocket` as `webSocketConstructor`. Every delivery, both ways,
 * lands on a later macrotask in order, like frames on a socket.
 */
export function createFakeConvexServer(options: {
  queries?: Record<string, FakeFunction>;
  mutations?: Record<string, FakeFunction>;
  actions?: Record<string, FakeFunction>;
  /** Identity for a user token, or the reason the server rejects it. */
  authenticate?: (token: string) => FakeIdentity | string;
}) {
  const { queries = {}, mutations = {}, actions = {}, authenticate } = options;
  const connections = new Set<FakeConnection>();
  /** Every client message received, in order. */
  const received: ClientMessage[] = [];
  let ts = 1;
  let online = true;
  const pendingOpens = new Set<() => void>();
  /** Mutations received while held, run on release. */
  let heldMutations: (() => void)[] | null = null;

  const tasks: Array<() => void> = [];
  const channel = new MessageChannel();
  channel.port1.on("message", () => tasks.shift()?.());
  channel.port1.unref();
  const later = (task: () => void) => {
    tasks.push(task);
    channel.port2.postMessage(null);
  };

  const send = (connection: FakeConnection, message: WireServerMessage) => {
    const data = encodeServerMessage(message);
    later(() => connection.socket.deliver(data));
  };

  const version = (v: FakeConnection["sentVersion"]) => ({
    querySet: v.querySet,
    identity: v.identity,
    ts: Long.fromNumber(v.ts),
  });

  const evaluate = (
    connection: FakeConnection,
    subscription: FakeSubscription,
  ) => {
    const handler = queries[subscription.udfPath];
    const ctx = { identity: connection.identity, memo: subscription.memo };
    try {
      if (!handler) {
        throw new Error(`Fake server has no query ${subscription.udfPath}`);
      }
      const value = convexToJson(
        (handler(ctx, subscription.args) ?? null) as Value,
      );
      return {
        sent: JSON.stringify(value),
        modification: { type: "QueryUpdated" as const, value },
      };
    } catch (error) {
      const { message, errorData } = failure(error);
      return {
        sent: `error:${message}`,
        modification: {
          type: "QueryFailed" as const,
          errorMessage: message,
          errorData,
        },
      };
    }
  };

  /** Re-runs subscribed queries and sends the changes in one transition. */
  const transition = (
    connection: FakeConnection,
    {
      force = false,
      only,
      removed = [],
    }: { force?: boolean; only?: Set<number>; removed?: number[] } = {},
  ) => {
    const modifications: any[] = removed.map((queryId) => ({
      type: "QueryRemoved",
      queryId,
    }));
    for (const [queryId, subscription] of connection.queries) {
      if (only && !only.has(queryId)) continue;
      const { sent, modification } = evaluate(connection, subscription);
      if (sent === subscription.sent) continue;
      subscription.sent = sent;
      modifications.push({
        ...modification,
        queryId,
        logLines: [],
        journal: null,
      });
    }
    const next = {
      querySet: connection.querySetVersion,
      identity: connection.identityVersion,
      ts,
    };
    const prev = connection.sentVersion;
    const versionChanged =
      next.querySet !== prev.querySet ||
      next.identity !== prev.identity ||
      next.ts !== prev.ts;
    if (!force && !versionChanged && modifications.length === 0) return;
    send(connection, {
      type: "Transition",
      startVersion: version(prev),
      endVersion: version(next),
      modifications,
    });
    connection.sentVersion = next;
  };

  const broadcast = () =>
    connections.forEach((connection) => transition(connection));

  const receive = (connection: FakeConnection, message: ClientMessage) => {
    received.push(message);
    handle(connection, message);
  };

  const handle = (connection: FakeConnection, message: ClientMessage) => {
    switch (message.type) {
      case "ModifyQuerySet": {
        if (message.baseVersion !== connection.querySetVersion) {
          throw new Error(
            `Fake server expected query set version ${connection.querySetVersion}, got ${message.baseVersion}`,
          );
        }
        const added = new Set<number>();
        const removed: number[] = [];
        for (const modification of message.modifications) {
          if (modification.type === "Add") {
            connection.queries.set(modification.queryId, {
              udfPath: modification.udfPath,
              args: jsonToConvex(modification.args[0] ?? {}) as Record<
                string,
                Value
              >,
              memo: {},
            });
            added.add(modification.queryId);
          } else {
            connection.queries.delete(modification.queryId);
            removed.push(modification.queryId);
          }
        }
        connection.querySetVersion = message.newVersion;
        transition(connection, { force: true, only: added, removed });
        return;
      }
      case "Authenticate": {
        if (message.tokenType === "User") {
          const result =
            authenticate?.(message.value) ?? "Authentication is not set up";
          if (typeof result === "string") {
            send(connection, {
              type: "AuthError",
              error: result,
              baseVersion: message.baseVersion,
              authUpdateAttempted: true,
            });
            return;
          }
          connection.identity = result;
        } else {
          connection.identity = null;
        }
        connection.identityVersion = message.baseVersion + 1;
        // Identity changes rerun every query.
        for (const subscription of connection.queries.values()) {
          delete subscription.sent;
        }
        transition(connection, { force: true });
        return;
      }
      case "Mutation": {
        if (heldMutations !== null) {
          heldMutations.push(() => {
            if (connections.has(connection)) handle(connection, message);
          });
          return;
        }
        const handler = mutations[message.udfPath];
        try {
          if (!handler) {
            throw new Error(`Fake server has no mutation ${message.udfPath}`);
          }
          const result = handler(
            { identity: connection.identity, memo: {} },
            jsonToConvex(message.args[0] ?? {}) as Record<string, Value>,
          );
          ts += 1;
          send(connection, {
            type: "MutationResponse",
            requestId: message.requestId,
            success: true,
            result: convexToJson((result ?? null) as Value),
            ts: Long.fromNumber(ts),
            logLines: [],
          });
          broadcast();
        } catch (error) {
          const { message: text, errorData } = failure(error);
          send(connection, {
            type: "MutationResponse",
            requestId: message.requestId,
            success: false,
            result: text,
            ...(errorData === undefined ? {} : { errorData }),
            logLines: [],
          });
        }
        return;
      }
      case "Action": {
        const handler = actions[message.udfPath];
        const respond = (outcome: { result: unknown } | { error: unknown }) => {
          if ("result" in outcome) {
            send(connection, {
              type: "ActionResponse",
              requestId: message.requestId,
              success: true,
              result: convexToJson((outcome.result ?? null) as Value),
              logLines: [],
            });
          } else {
            const { message: text, errorData } = failure(outcome.error);
            send(connection, {
              type: "ActionResponse",
              requestId: message.requestId,
              success: false,
              result: text,
              ...(errorData === undefined ? {} : { errorData }),
              logLines: [],
            });
          }
        };
        if (!handler) {
          respond({
            error: new Error(`Fake server has no action ${message.udfPath}`),
          });
          return;
        }
        Promise.resolve()
          .then(() =>
            handler(
              { identity: connection.identity, memo: {} },
              jsonToConvex(message.args[0] ?? {}) as Record<string, Value>,
            ),
          )
          .then(
            (result) => respond({ result }),
            (error) => respond({ error }),
          );
        return;
      }
      default:
        // Connect and Event need no answer.
        return;
    }
  };

  class FakeSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    readyState = FakeSocket.CONNECTING;
    onopen: ((event: unknown) => void) | null = null;
    onmessage: ((event: { data: string }) => void) | null = null;
    onclose: ((event: unknown) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    readonly connection: FakeConnection;

    constructor(_url: string) {
      this.connection = {
        socket: this,
        queries: new Map(),
        identity: null,
        querySetVersion: 0,
        identityVersion: 0,
        sentVersion: { querySet: 0, identity: 0, ts: 0 },
      };
      const open = () => {
        if (this.readyState !== FakeSocket.CONNECTING) return;
        if (!online) {
          pendingOpens.add(open);
          return;
        }
        this.readyState = FakeSocket.OPEN;
        connections.add(this.connection);
        this.onopen?.({});
      };
      later(open);
    }

    send(data: string) {
      if (this.readyState !== FakeSocket.OPEN) return;
      const message = JSON.parse(data) as ClientMessage;
      later(() => {
        if (this.readyState === FakeSocket.OPEN) {
          receive(this.connection, message);
        }
      });
    }

    close(code = 1000, reason = "") {
      if (this.readyState >= FakeSocket.CLOSING) return;
      this.readyState = FakeSocket.CLOSING;
      connections.delete(this.connection);
      later(() => this.closed(code, reason));
    }

    deliver(data: string) {
      if (this.readyState === FakeSocket.OPEN) this.onmessage?.({ data });
    }

    closed(code: number, reason: string) {
      this.readyState = FakeSocket.CLOSED;
      connections.delete(this.connection);
      this.onclose?.({ code, reason, wasClean: code === 1000 });
    }
  }

  return {
    /** Pass as `webSocketConstructor`. */
    WebSocket: FakeSocket as unknown as typeof globalThis.WebSocket,
    received,
    /** Client messages of `type` received so far. */
    receivedOfType<T extends ClientMessage["type"]>(type: T) {
      return received.filter(
        (message): message is Extract<ClientMessage, { type: T }> =>
          message.type === type,
      );
    },
    /** Queues mutations until the returned function is called. */
    holdMutations() {
      heldMutations = [];
      return () => {
        const held = heldMutations ?? [];
        heldMutations = null;
        held.forEach((run) => run());
      };
    },
    /** Pushes results of queries whose data changed outside a mutation. */
    invalidate() {
      ts += 1;
      broadcast();
    },
    /** Sends a raw message to every open connection. */
    sendToClients(message: WireServerMessage) {
      connections.forEach((connection) => send(connection, message));
    },
    /** Server-side token expiry. */
    expireAuth() {
      connections.forEach((connection) => {
        if (!connection.identity) return;
        send(connection, {
          type: "AuthError",
          error: "Token expired",
          baseVersion: connection.identityVersion - 1,
          authUpdateAttempted: false,
        });
      });
    },
    /** Closes every socket from the server side. */
    dropConnections(code = 1011, reason = "InternalServerError") {
      connections.forEach((connection) => {
        connection.socket.readyState = FakeSocket.CLOSING;
        later(() => connection.socket.closed(code, reason));
      });
      connections.clear();
    },
    /** Offline drops every socket; new ones connect once back online. */
    setOnline(next: boolean) {
      online = next;
      if (next) {
        pendingOpens.forEach(later);
        pendingOpens.clear();
        return;
      }
      this.dropConnections(1006, "");
    },
    connectionCount: () => connections.size,
    /** Server-side subscriptions to `udfPath` across connections. */
    subscriptionCount: (udfPath: string) =>
      [...connections].reduce(
        (count, connection) =>
          count +
          [...connection.queries.values()].filter(
            (query) => query.udfPath === udfPath,
          ).length,
        0,
      ),
    identities: () => [...connections].map((connection) => connection.identity),
    /** Resolves once every delivery queued so far, both ways, has run. */
    async settled(rounds = 8) {
      for (let i = 0; i < rounds; i++) {
        await new Promise<void>((resolve) => later(resolve));
      }
    },
    close() {
      connections.clear();
      channel.port1.close();
    },
  };
}

export type FakeConvexServer = ReturnType<typeof createFakeConvexServer>;
