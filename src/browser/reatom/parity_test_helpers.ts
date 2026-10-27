/**
 * Runs one scenario against convex@1.48.0 from npm and against this package,
 * over the same scripted in-memory server, and compares what each client
 * shows the outside world.
 *
 * The server answers by query identity, not query id, and keeps each
 * client's versions, so both clients get consistent transitions even though
 * this package batches query set changes. Query set messages are not traced
 * one by one: scenarios trace the server's query set at checkpoints instead.
 */
import { writeFileSync } from "fs";
import { expect, vi } from "vitest";
import * as upstreamBrowser from "convex-upstream/browser";
import * as upstreamReact from "convex-upstream/react";
import * as browser from "../index.js";
import * as react from "../../react/index.js";
import { Long } from "../../vendor/long.js";
import { longToU64 } from "../sync/protocol.js";

export type Impl = {
  name: "upstream" | "reatom";
  BaseConvexClient: typeof browser.BaseConvexClient;
  ConvexClient: typeof browser.ConvexClient;
  ConvexReactClient: typeof react.ConvexReactClient;
};

export const implementations: Impl[] = [
  {
    name: "upstream",
    BaseConvexClient: upstreamBrowser.BaseConvexClient as any,
    ConvexClient: upstreamBrowser.ConvexClient as any,
    ConvexReactClient: upstreamReact.ConvexReactClient as any,
  },
  {
    name: "reatom",
    BaseConvexClient: browser.BaseConvexClient,
    ConvexClient: browser.ConvexClient,
    ConvexReactClient: react.ConvexReactClient,
  },
];

type ServerQuery = {
  udfPath: string;
  args: unknown;
  journal?: string | null | undefined;
  componentPath?: string | undefined;
};

type Model = {
  querySet: Map<number, ServerQuery>;
  querySetVersion: number;
  identityVersion: number;
  /** The version of the last transition sent on this connection. */
  sent: { querySet: number; identity: number; ts: number };
};

const freshModel = (): Model => ({
  querySet: new Map(),
  querySetVersion: 0,
  identityVersion: 0,
  sent: { querySet: 0, identity: 0, ts: 0 },
});

const encodeTs = (ts: number) => longToU64(Long.fromNumber(ts));

export type Answer =
  | { value: unknown; journal?: string | null; logLines?: string[] }
  | {
      error: string;
      errorData?: unknown;
      journal?: string | null;
      logLines?: string[];
    }
  | "removed";

/** JSON with sorted keys, as `convexToJson` sorts them. */
const stable = (value: unknown): string =>
  JSON.stringify(value, (_key, inner) =>
    inner && typeof inner === "object" && !Array.isArray(inner)
      ? Object.fromEntries(
          Object.entries(inner).sort(([a], [b]) => (a < b ? -1 : 1)),
        )
      : inner,
  );

const canonical = (udfPath: string) => {
  const name = udfPath.includes(":") ? udfPath : `${udfPath}:default`;
  return name.replace(/\.js:/, ":");
};

export class FakeSocket {
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  closedByClient = false;

  constructor(
    private readonly server: FakeServer,
    readonly url: string,
  ) {
    server.sockets.push(this);
    server.trace.push({ socket: "constructed", count: server.sockets.length });
  }

  send(data: string) {
    if (this.closedByClient) throw new Error("socket closed");
    if (this !== this.server.socket) throw new Error("stale socket");
    this.server.receive(JSON.parse(data));
  }

  close() {
    this.closedByClient = true;
    queueMicrotask(() => this.onclose?.({ code: 1000, reason: "" }));
  }
}

export class FakeServer {
  readonly sockets: FakeSocket[] = [];
  readonly WebSocket: typeof WebSocket;
  /** Requests received on any connection, by request id. */
  readonly requests = new Map<number, any>();
  private model = freshModel();
  private ts = 0;

  constructor(readonly trace: unknown[]) {
    const server = this;
    this.WebSocket = class extends FakeSocket {
      constructor(url: string) {
        super(server, url);
      }
    } as unknown as typeof WebSocket;
  }

  get socket(): FakeSocket {
    const socket = this.sockets[this.sockets.length - 1];
    if (!socket) throw new Error("no socket");
    return socket;
  }

  /** Completes the handshake of the latest socket. */
  open() {
    this.socket.onopen?.({});
  }

  /** Closes the latest socket from the server side. */
  drop(code = 1006, reason = "") {
    this.socket.onclose?.({ code, reason });
  }

  push(message: Record<string, unknown>) {
    this.socket.onmessage?.({ data: JSON.stringify(message) });
  }

  receive(message: any) {
    switch (message.type) {
      case "Connect": {
        this.model = freshModel();
        this.trace.push({
          received: "Connect",
          connectionCount: message.connectionCount,
          lastCloseReason: message.lastCloseReason,
          maxObservedTimestamp:
            message.maxObservedTimestamp === undefined
              ? undefined
              : Number(
                  Long.fromBytesLE(
                    Array.from(
                      Buffer.from(message.maxObservedTimestamp, "base64"),
                    ),
                  ).toString(),
                ),
        });
        return;
      }
      case "ModifyQuerySet": {
        if (message.baseVersion !== this.model.querySetVersion) {
          this.trace.push({
            error: "query set version",
            expected: this.model.querySetVersion,
            got: message.baseVersion,
          });
        }
        for (const modification of message.modifications) {
          if (modification.type === "Add") {
            this.model.querySet.set(modification.queryId, {
              udfPath: modification.udfPath,
              args: modification.args[0],
              journal: modification.journal,
              componentPath: modification.componentPath,
            });
          } else {
            this.model.querySet.delete(modification.queryId);
          }
        }
        this.model.querySetVersion = message.newVersion;
        return;
      }
      case "Authenticate": {
        if (message.baseVersion !== this.model.identityVersion) {
          this.trace.push({
            error: "identity version",
            expected: this.model.identityVersion,
            got: message.baseVersion,
          });
        }
        this.model.identityVersion = message.baseVersion + 1;
        this.trace.push({
          received: "Authenticate",
          tokenType: message.tokenType,
          value: message.value,
          impersonating: message.impersonating,
        });
        return;
      }
      case "Mutation":
      case "Action": {
        this.requests.set(message.requestId, message);
        this.trace.push({
          received: message.type,
          requestId: message.requestId,
          udfPath: message.udfPath,
          args: message.args,
          componentPath: message.componentPath,
        });
        return;
      }
      case "Event": {
        this.trace.push({ received: "Event", eventType: message.eventType });
        return;
      }
      default:
        this.trace.push({ error: "unknown message", message });
    }
  }

  /** The queries the server holds for the current connection, sorted. */
  querySet() {
    return Array.from(this.model.querySet.values(), (query) =>
      JSON.stringify(query),
    ).sort();
  }

  private queryId(udfPath: string, args: unknown) {
    const path = canonical(udfPath);
    const json = stable(args);
    for (const [queryId, query] of this.model.querySet) {
      if (query.udfPath === path && stable(query.args) === json) {
        return queryId;
      }
    }
    throw new Error(`not subscribed: ${path} ${json}`);
  }

  /** A transition answering the given queries, advancing the timestamp. */
  transition(
    answers: Array<[udfPath: string, args: unknown, answer: Answer]>,
    options: { clientClockSkew?: number; tsStep?: number } = {},
  ) {
    this.ts += options.tsStep ?? 1;
    const end = {
      querySet: this.model.querySetVersion,
      identity: this.model.identityVersion,
      ts: this.ts,
    };
    const start = this.model.sent;
    this.model.sent = end;
    if (process.env.PARITY_VERBOSE)
      this.trace.push({ sending: { start, end } });
    this.push({
      type: "Transition",
      startVersion: { ...start, ts: encodeTs(start.ts) },
      endVersion: { ...end, ts: encodeTs(end.ts) },
      modifications: answers.map(([udfPath, args, answer]) => {
        const queryId = this.queryId(udfPath, args);
        if (answer === "removed") return { type: "QueryRemoved", queryId };
        if ("error" in answer) {
          return {
            type: "QueryFailed",
            queryId,
            errorMessage: answer.error,
            errorData: answer.errorData,
            logLines: answer.logLines ?? [],
            journal: answer.journal ?? null,
          };
        }
        return {
          type: "QueryUpdated",
          queryId,
          value: answer.value,
          logLines: answer.logLines ?? [],
          journal: answer.journal ?? null,
        };
      }),
      ...(options.clientClockSkew === undefined
        ? {}
        : { clientClockSkew: options.clientClockSkew }),
    });
  }

  /** Like `transition`, but sent as `parts` TransitionChunks. */
  chunkedTransition(
    answers: Array<[udfPath: string, args: unknown, answer: Answer]>,
    parts: number,
  ) {
    const socket = this.socket;
    const sent: string[] = [];
    const deliver = socket.onmessage;
    socket.onmessage = (event) => sent.push(event.data);
    try {
      this.transition(answers);
    } finally {
      socket.onmessage = deliver;
    }
    const json = sent[0];
    const size = Math.ceil(json.length / parts);
    for (let partNumber = 0; partNumber < parts; partNumber++) {
      this.push({
        type: "TransitionChunk",
        chunk: json.slice(partNumber * size, (partNumber + 1) * size),
        partNumber,
        totalParts: parts,
        transitionId: "t1",
      });
    }
  }

  /** Answers a mutation or action; a mutation commits at the next timestamp. */
  respond(
    requestId: number,
    answer:
      | { value: unknown; logLines?: string[] }
      | { error: string; errorData?: unknown; logLines?: string[] },
  ) {
    const request = this.requests.get(requestId);
    if (!request) throw new Error(`no request ${requestId}`);
    const type =
      request.type === "Mutation" ? "MutationResponse" : "ActionResponse";
    if ("error" in answer) {
      this.push({
        type,
        requestId,
        success: false,
        result: answer.error,
        errorData: answer.errorData,
        logLines: answer.logLines ?? [],
      });
    } else {
      this.push({
        type,
        requestId,
        success: true,
        result: answer.value,
        logLines: answer.logLines ?? [],
        ...(type === "MutationResponse" ? { ts: encodeTs(this.ts + 1) } : {}),
      });
    }
  }

  authError(
    error: string,
    options: { authUpdateAttempted: boolean; baseVersion?: number },
  ) {
    this.push({
      type: "AuthError",
      error,
      baseVersion: options.baseVersion ?? this.model.identityVersion - 1,
      authUpdateAttempted: options.authUpdateAttempted,
    });
  }
}

/** Lets every queued microtask run. */
export const settle = () =>
  new Promise<void>((resolve) => setImmediate(resolve));

export type ScenarioContext = {
  impl: Impl;
  server: FakeServer;
  log: (...entry: unknown[]) => void;
  /** Settles, then traces the server's query set. */
  checkpoint: (label: string) => Promise<void>;
  settle: () => Promise<void>;
};

export type Scenario = (ctx: ScenarioContext) => Promise<void>;

const describeError = (error: unknown) =>
  error instanceof Error
    ? {
        error: error.message,
        name: error.constructor.name,
        data: (error as any).data,
      }
    : { thrown: error };

export const caught = async (promise: Promise<unknown>) => {
  try {
    return { value: await promise };
  } catch (error) {
    return describeError(error);
  }
};

export const attempt = (fn: () => unknown) => {
  try {
    return { value: fn() };
  } catch (error) {
    return describeError(error);
  }
};

async function run(impl: Impl, scenario: Scenario) {
  const trace: unknown[] = [];
  const server = new FakeServer(trace);
  vi.useFakeTimers({
    toFake: [
      "setTimeout",
      "clearTimeout",
      "setInterval",
      "clearInterval",
      "Date",
    ],
  });
  vi.setSystemTime(0);
  const random = vi.spyOn(Math, "random").mockReturnValue(0.5);
  try {
    await scenario({
      impl,
      server,
      log: (...entry) => trace.push(entry.length === 1 ? entry[0] : entry),
      settle,
      checkpoint: async (label) => {
        await settle();
        trace.push({ checkpoint: label, querySet: server.querySet() });
      },
    });
    await settle();
  } catch (error) {
    trace.push({ scenarioError: String(error) });
    if (process.env.PARITY_VERBOSE)
      trace.push({ stack: (error as Error).stack });
  } finally {
    random.mockRestore();
    vi.clearAllTimers();
    vi.useRealTimers();
  }
  return trace;
}

/** Runs `scenario` on both clients, for scenarios where they differ on purpose. */
export async function runBoth(scenario: Scenario) {
  return {
    upstream: await run(implementations[0], scenario),
    reatom: await run(implementations[1], scenario),
  };
}

/** Runs `scenario` on both clients and expects the same trace. */
export async function expectParity(scenario: Scenario) {
  const [upstream, reatom] = [
    await run(implementations[0], scenario),
    await run(implementations[1], scenario),
  ];
  // PARITY_TRACE=file.json writes both traces for inspection.
  if (process.env.PARITY_TRACE) {
    writeFileSync(
      process.env.PARITY_TRACE,
      JSON.stringify({ upstream, reatom }, null, 1),
    );
  }
  expect(reatom).toEqual(upstream);
  // An error both clients hit would otherwise pass as parity.
  expect(upstream.filter((entry: any) => entry?.scenarioError)).toEqual([]);
  return upstream;
}

/** The options every scenario client is built with. */
export const clientOptions = (server: FakeServer) => ({
  webSocketConstructor: server.WebSocket,
  unsavedChangesWarning: false,
  logger: false as const,
});
