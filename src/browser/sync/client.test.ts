import { afterEach, describe, expect, test, vi } from "vitest";
import { makeFunctionReference } from "../../server/api.js";
import { ConvexError, Value } from "../../values/index.js";
import { BaseConvexClient, ConnectionState, Transition } from "./client.js";
import {
  createFakeConvexServer,
  FakeConvexServer,
  fakeJwt,
  readFakeJwt,
} from "./client_node_test_helpers.js";
import { ClientMessage } from "./protocol.js";

const listRef = makeFunctionReference<"query">("messages:list");
const otherRef = makeFunctionReference<"query">("messages:other");

const servers: FakeConvexServer[] = [];
const clients: BaseConvexClient[] = [];

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(clients.splice(0).map((client) => client.close()));
  servers.splice(0).forEach((server) => server.close());
});

function setup(
  options: Parameters<typeof createFakeConvexServer>[0] = {},
  clientOptions: ConstructorParameters<typeof BaseConvexClient>[2] = {},
) {
  const server = createFakeConvexServer(options);
  servers.push(server);
  const transitions: Transition[] = [];
  const client = new BaseConvexClient("http://127.0.0.1:8000", () => {}, {
    webSocketConstructor: server.WebSocket,
    unsavedChangesWarning: false,
    logger: false,
    ...clientOptions,
  });
  client.addOnTransitionHandler((transition) => transitions.push(transition));
  clients.push(client);
  const changedTokens = () =>
    transitions.flatMap((transition) =>
      transition.queries.map(({ token }) => token),
    );
  return { server, client, transitions, changedTokens };
}

/** The query set changes received after the first `skip` messages. */
const querySetChanges = (server: FakeConvexServer, skip = 0) =>
  server.receivedOfType("ModifyQuerySet").slice(skip);

function messagesStore() {
  let messages: string[] = [];
  return {
    queries: {
      "messages:list": () => messages,
      "messages:other": () => "other",
    },
    mutations: {
      "messages:send": (_ctx: unknown, { body }: Record<string, Value>) => {
        messages = [...messages, body as string];
        return messages.length;
      },
      "messages:fail": () => {
        throw new ConvexError({ code: "nope" });
      },
      "messages:crash": () => {
        throw new Error("boom");
      },
    },
    set(next: string[]) {
      messages = next;
    },
  };
}

describe("subscriptions", () => {
  test("a subscription receives its result", async () => {
    const { server, client } = setup({
      queries: { "messages:list": () => ["hello"] },
    });
    const { queryToken } = client.subscribe("messages:list", {});
    await server.settled();
    expect(client.localQueryResultByToken(queryToken)).toEqual(["hello"]);
    expect(client.localQueryResult("messages:list", {})).toEqual(["hello"]);
    expect(client.hasLocalQueryResultByToken(queryToken)).toBe(true);
  });

  test("subscriptions made in one tick go out as one ModifyQuerySet", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    const before = querySetChanges(server).length;
    client.subscribe("messages:list", {});
    client.subscribe("messages:other", {});
    client.subscribe("messages:list", { page: 2 });
    await server.settled();
    const changes = querySetChanges(server, before);
    expect(changes).toHaveLength(1);
    expect(changes[0]).toMatchObject({ baseVersion: 1, newVersion: 2 });
    expect(changes[0].modifications.map((m) => m.type)).toEqual([
      "Add",
      "Add",
      "Add",
    ]);
  });

  test("equal subscriptions share one server subscription", async () => {
    const { server, client } = setup(messagesStore());
    const first = client.subscribe("messages:list", {});
    const second = client.subscribe("messages:list.js:list", {});
    const third = client.subscribe("messages:list", {});
    expect(second.queryToken).not.toBe(first.queryToken);
    expect(third.queryToken).toBe(first.queryToken);
    await server.settled();
    expect(server.subscriptionCount("messages:list")).toBe(1);

    first.unsubscribe();
    await server.settled();
    expect(server.subscriptionCount("messages:list")).toBe(1);
    expect(client.localQueryResult("messages:list", {})).toEqual([]);

    third.unsubscribe();
    await server.settled();
    expect(server.subscriptionCount("messages:list")).toBe(0);
    const removes = querySetChanges(server).flatMap((change) =>
      change.modifications.filter((m) => m.type === "Remove"),
    );
    expect(removes).toHaveLength(1);
  });

  test("unsubscribing twice removes one subscriber", async () => {
    const { server, client } = setup(messagesStore());
    const first = client.subscribe("messages:list", {});
    client.subscribe("messages:list", {});
    first.unsubscribe();
    first.unsubscribe();
    await server.settled();
    expect(server.subscriptionCount("messages:list")).toBe(1);
  });

  test("a subscription dropped in the same tick sends nothing", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    const before = querySetChanges(server).length;
    const { unsubscribe } = client.subscribe("messages:list", {});
    unsubscribe();
    await server.settled();
    expect(querySetChanges(server, before)).toHaveLength(0);
  });

  test("resubscribing in the same tick keeps the result and the server subscription", async () => {
    const { server, client } = setup(messagesStore());
    const first = client.subscribe("messages:list", {});
    await server.settled();
    const before = querySetChanges(server).length;
    first.unsubscribe();
    const second = client.subscribe("messages:list", {});
    expect(client.localQueryResultByToken(second.queryToken)).toEqual([]);
    await server.settled();
    expect(querySetChanges(server, before)).toHaveLength(0);
    expect(client.localQueryResultByToken(second.queryToken)).toEqual([]);
  });

  test("a result stays readable after unsubscribing until the next transition", async () => {
    const store = messagesStore();
    const { server, client } = setup(store);
    const { queryToken, unsubscribe } = client.subscribe("messages:list", {});
    client.subscribe("messages:other", {});
    await server.settled();
    unsubscribe();
    expect(client.localQueryResultByToken(queryToken)).toEqual([]);
    await server.settled();
    expect(client.localQueryResultByToken(queryToken)).toBeUndefined();
    expect(client.hasLocalQueryResultByToken(queryToken)).toBe(false);
  });

  test("subscribe passes the journal and the query journal follows the server", async () => {
    const { server, client } = setup(messagesStore());
    client.subscribe("messages:list", {}, { journal: "j1" });
    await server.settled();
    const add = querySetChanges(server)
      .flatMap((change) => change.modifications)
      .find((m) => m.type === "Add");
    expect(add).toMatchObject({ journal: "j1", udfPath: "messages:list" });
    // The fake server answers with a null journal.
    expect(client.queryJournal("messages:list", {})).toBeNull();
  });
});

describe("transitions", () => {
  test("a transition reports only the queries whose result changed", async () => {
    let a = 1;
    const { server, client, transitions } = setup({
      queries: { "q:a": () => a, "q:b": () => "b" },
    });
    const tokenA = client.subscribe("q:a", {}).queryToken;
    const tokenB = client.subscribe("q:b", {}).queryToken;
    await server.settled();
    expect(
      transitions
        .at(-1)!
        .queries.map((q) => q.token)
        .sort(),
    ).toEqual([tokenA, tokenB].sort());

    a = 2;
    server.invalidate();
    await server.settled();
    const last = transitions.at(-1)!;
    expect(last.queries).toEqual([
      {
        token: tokenA,
        modification: {
          kind: "Updated",
          result: { success: true, value: 2, logLines: [] },
        },
      },
    ]);
    expect(client.localQueryResult("q:b", {})).toBe("b");
  });

  test("the constructor's onTransition gets the changed tokens", async () => {
    const server = createFakeConvexServer(messagesStore());
    servers.push(server);
    const updates: string[][] = [];
    const client = new BaseConvexClient(
      "http://127.0.0.1:8000",
      (tokens) => updates.push(tokens),
      {
        webSocketConstructor: server.WebSocket,
        unsavedChangesWarning: false,
        logger: false,
      },
    );
    clients.push(client);
    const { queryToken } = client.subscribe("messages:list", {});
    await server.settled();
    expect(updates).toContainEqual([queryToken]);
  });

  test("a failed query throws its error, with ConvexError data", async () => {
    const { server, client } = setup({
      queries: {
        "q:convexError": () => {
          throw new ConvexError({ reason: "denied" });
        },
        "q:error": () => {
          throw new Error("broken");
        },
      },
    });
    client.subscribe("q:convexError", {});
    client.subscribe("q:error", {});
    await server.settled();
    let caught: unknown;
    try {
      client.localQueryResult("q:convexError", {});
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ConvexError);
    expect((caught as ConvexError<any>).data).toEqual({ reason: "denied" });
    expect(() => client.localQueryResult("q:error", {})).toThrow(/broken/);
  });

  test("transition chunks are assembled", async () => {
    const { server, client } = setup(messagesStore());
    const { queryToken } = client.subscribe("messages:list", {});
    await server.settled();
    const connect = server.receivedOfType("ModifyQuerySet").at(-1)!;
    const queryId = (connect.modifications[0] as { queryId: number }).queryId;
    const transition = {
      type: "Transition",
      startVersion: { querySet: 1, identity: 0, ts: "AQAAAAAAAAA=" },
      endVersion: { querySet: 1, identity: 0, ts: "AgAAAAAAAAA=" },
      modifications: [
        {
          type: "QueryUpdated",
          queryId,
          value: ["chunked"],
          logLines: [],
          journal: null,
        },
      ],
    };
    const json = JSON.stringify(transition);
    const parts = [json.slice(0, 40), json.slice(40, 90), json.slice(90)];
    parts.forEach((chunk, partNumber) =>
      server.sendToClients({
        type: "TransitionChunk",
        chunk,
        partNumber,
        totalParts: parts.length,
        transitionId: "t1",
      }),
    );
    await server.settled();
    expect(client.localQueryResultByToken(queryToken)).toEqual(["chunked"]);
  });
});

describe("mutations and actions", () => {
  test("a mutation resolves once its write is visible", async () => {
    const { server, client } = setup(messagesStore());
    client.subscribe("messages:list", {});
    await server.settled();
    const result = await client.mutation("messages:send", { body: "hi" });
    expect(result).toBe(1);
    expect(client.localQueryResult("messages:list", {})).toEqual(["hi"]);
  });

  test("a failed mutation rejects with its error", async () => {
    const { client } = setup(messagesStore());
    await expect(client.mutation("messages:fail", {})).rejects.toMatchObject({
      data: { code: "nope" },
    });
    await expect(client.mutation("messages:crash", {})).rejects.toThrow(/boom/);
  });

  test("actions resolve with their result and reject with their error", async () => {
    const { client } = setup({
      actions: {
        "work:double": (_ctx, { n }) => (n as number) * 2,
        "work:fail": () => {
          throw new ConvexError("bad input");
        },
      },
    });
    await expect(client.action("work:double", { n: 21 })).resolves.toBe(42);
    await expect(client.action("work:fail", {})).rejects.toMatchObject({
      data: "bad input",
    });
  });

  test("requests made before the socket opens are sent once it does", async () => {
    const { server, client } = setup(messagesStore());
    const sent = client.mutation("messages:send", { body: "early" });
    expect(server.received).toHaveLength(0);
    await expect(sent).resolves.toBe(1);
    expect(server.receivedOfType("Mutation")).toHaveLength(1);
  });

  test("query set changes go out before a request made after them", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    const before = server.received.length;
    client.subscribe("messages:list", {});
    void client.mutation("messages:send", { body: "x" });
    await server.settled();
    expect(server.received.slice(before).map((m) => m.type)).toEqual([
      "ModifyQuerySet",
      "Mutation",
    ]);
  });

  test("connection state counts requests in flight", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    const release = server.holdMutations();
    const sent = client.mutation("messages:send", { body: "x" });
    expect(client.connectionState()).toMatchObject({
      hasInflightRequests: true,
      inflightMutations: 1,
      inflightActions: 0,
      isWebSocketConnected: true,
    });
    expect(client.connectionState().timeOfOldestInflightRequest).toBeInstanceOf(
      Date,
    );
    release();
    await sent;
    expect(client.connectionState()).toMatchObject({
      hasInflightRequests: false,
      inflightMutations: 0,
      timeOfOldestInflightRequest: null,
    });
  });

  test("connection state subscribers hear about changes", async () => {
    const { server, client } = setup(messagesStore());
    const states: ConnectionState[] = [];
    client.subscribeToConnectionState((state) => states.push(state));
    await server.settled();
    expect(states.at(-1)).toMatchObject({
      isWebSocketConnected: true,
      hasEverConnected: true,
      connectionCount: 1,
    });
    const count = states.length;
    await client.mutation("messages:send", { body: "x" });
    await server.settled();
    expect(states.length).toBeGreaterThan(count);
    expect(states.at(-1)).toMatchObject({ inflightMutations: 0 });
  });
});

describe("optimistic updates", () => {
  const sendWithEcho = {
    optimisticUpdate: (store: any, args: Record<string, Value>) => {
      const current = store.getQuery(listRef, {});
      if (current !== undefined) {
        store.setQuery(listRef, {}, [...current, `${args.body as string}?`]);
      }
    },
  };

  test("an update shows at once and gives way to the server result", async () => {
    const { server, client, transitions } = setup(messagesStore());
    const { queryToken } = client.subscribe("messages:list", {});
    await server.settled();
    const release = server.holdMutations();
    const sent = client.mutation("messages:send", { body: "a" }, sendWithEcho);
    expect(client.localQueryResultByToken(queryToken)).toEqual(["a?"]);
    expect(transitions.at(-1)!.queries).toEqual([
      {
        token: queryToken,
        modification: {
          kind: "Updated",
          result: { success: true, value: ["a?"], logLines: [] },
        },
      },
    ]);
    release();
    await sent;
    expect(client.localQueryResultByToken(queryToken)).toEqual(["a"]);
    expect(transitions.at(-1)!.reflectedMutations).toHaveLength(1);
  });

  test("a failed mutation rolls its update back", async () => {
    const { server, client } = setup(messagesStore());
    client.subscribe("messages:list", {});
    await server.settled();
    const failed = client.mutation(
      "messages:fail",
      { body: "x" },
      sendWithEcho,
    );
    expect(client.localQueryResult("messages:list", {})).toEqual(["x?"]);
    await expect(failed).rejects.toBeInstanceOf(ConvexError);
    expect(client.localQueryResult("messages:list", {})).toEqual([]);
  });

  test("pending updates are replayed over new server results in order", async () => {
    const store = messagesStore();
    const { server, client } = setup(store);
    client.subscribe("messages:list", {});
    await server.settled();
    const release = server.holdMutations();
    const first = client.mutation("messages:send", { body: "a" }, sendWithEcho);
    const second = client.mutation(
      "messages:send",
      { body: "b" },
      sendWithEcho,
    );
    expect(client.localQueryResult("messages:list", {})).toEqual(["a?", "b?"]);
    store.set(["server"]);
    server.invalidate();
    await server.settled();
    expect(client.localQueryResult("messages:list", {})).toEqual([
      "server",
      "a?",
      "b?",
    ]);
    release();
    await Promise.all([first, second]);
    expect(client.localQueryResult("messages:list", {})).toEqual([
      "server",
      "a",
      "b",
    ]);
  });

  test("an update can set a query nobody subscribes to", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    const release = server.holdMutations();
    const sent = client.mutation(
      "messages:send",
      { body: "x" },
      {
        optimisticUpdate: (store) => {
          store.setQuery(otherRef, { id: 1 }, "optimistic");
          expect(store.getQuery(otherRef, { id: 1 })).toBe("optimistic");
          expect(store.getAllQueries(otherRef)).toEqual([
            { args: { id: 1 }, value: "optimistic" },
          ]);
        },
      },
    );
    expect(client.localQueryResult("messages:other", { id: 1 })).toBe(
      "optimistic",
    );
    release();
    await sent;
    expect(client.localQueryResult("messages:other", { id: 1 })).toBe(
      undefined,
    );
  });

  test("setting a query to undefined makes it loading", async () => {
    const { server, client } = setup(messagesStore());
    const { queryToken } = client.subscribe("messages:list", {});
    await server.settled();
    const release = server.holdMutations();
    const sent = client.mutation(
      "messages:send",
      { body: "x" },
      { optimisticUpdate: (store) => store.setQuery(listRef, {}, undefined) },
    );
    expect(client.localQueryResultByToken(queryToken)).toBeUndefined();
    expect(client.hasLocalQueryResultByToken(queryToken)).toBe(true);
    release();
    await sent;
    expect(client.localQueryResultByToken(queryToken)).toEqual(["x"]);
  });
});

describe("reconnects", () => {
  test("a new connection replays the query set and in-flight requests", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const store = messagesStore();
    const { server, client } = setup({
      ...store,
      actions: { "work:forever": () => new Promise(() => {}) },
    });
    client.subscribe("messages:list", {});
    client.subscribe("messages:other", {});
    await server.settled();
    const firstSet = querySetChanges(server).at(-1)!;

    const release = server.holdMutations();
    const mutation = client.mutation("messages:send", { body: "x" });
    const action = client.action("work:forever", {}).catch((error) => error);
    await server.settled();

    server.dropConnections(1011, "InternalServerError");
    await server.settled();
    expect(client.connectionState().isWebSocketConnected).toBe(false);

    await vi.advanceTimersByTimeAsync(2000);
    await server.settled();
    expect(await action).toBeInstanceOf(Error);
    expect((await action).message).toMatch(
      /Connection lost while action was in flight/,
    );

    const connects = server.receivedOfType("Connect");
    expect(connects).toHaveLength(2);
    expect(connects[1]).toMatchObject({
      connectionCount: 1,
      lastCloseReason: "InternalServerError",
    });
    const replayed = querySetChanges(server).at(-1)!;
    expect(replayed).toMatchObject({ baseVersion: 0, newVersion: 1 });
    // Re-added with the journals the server sent.
    expect(replayed.modifications).toEqual(
      firstSet.modifications.map((m) => ({ ...m, journal: null })),
    );
    expect(
      server.received.filter(
        (m: ClientMessage) => m.type === "Mutation" && m.requestId === 0,
      ),
    ).toHaveLength(2);

    release();
    await expect(mutation).resolves.toBe(1);
    expect(client.localQueryResult("messages:list", {})).toEqual(["x"]);
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: true,
      connectionCount: 2,
    });
  });

  test("an offline client keeps retrying until the server is back", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { server, client } = setup(messagesStore());
    client.subscribe("messages:list", {});
    await server.settled();
    server.setOnline(false);
    await server.settled();
    expect(client.connectionState().isWebSocketConnected).toBe(false);
    await vi.advanceTimersByTimeAsync(2000);
    await server.settled();
    expect(client.connectionState().connectionRetries).toBeGreaterThan(0);
    server.setOnline(true);
    await server.settled();
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: true,
      connectionCount: 2,
    });
    expect(client.localQueryResult("messages:list", {})).toEqual([]);
  });
});

describe("auth", () => {
  const authServer = () => ({
    queries: {
      "users:me": (ctx: { identity: { subject: string } | null }) =>
        ctx.identity?.subject ?? null,
    },
    authenticate: (token: string) => {
      const claims = readFakeJwt(token);
      return claims.valid === false
        ? "Invalid token"
        : { subject: claims.sub as string };
    },
  });

  test("fetches a cached then a fresh token and authenticates queries", async () => {
    // `onChange(true)` fires on the cached token's confirmation and again on
    // the fresh one's.
    const { server, client } = setup(authServer());
    const fetchToken = vi.fn(async (_args: { forceRefreshToken: boolean }) =>
      fakeJwt({ sub: "user1" }),
    );
    const onChange = vi.fn();
    client.setAuth(fetchToken, onChange);
    client.subscribe("users:me", {});
    await server.settled();
    expect(fetchToken.mock.calls.map(([args]) => args)).toEqual([
      { forceRefreshToken: false },
      { forceRefreshToken: true },
    ]);
    expect(onChange.mock.calls).toEqual([[true], [true]]);
    expect(client.localQueryResult("users:me", {})).toBe("user1");
    expect(client.hasAuth()).toBe(true);
    expect(client.getCurrentAuthClaims()?.decoded).toMatchObject({
      sub: "user1",
    });
  });

  test("queries and requests wait for the token", async () => {
    const { server, client } = setup(authServer());
    let resolveToken!: (token: string) => void;
    const fetchToken = vi.fn(({ forceRefreshToken }) =>
      forceRefreshToken
        ? Promise.resolve(fakeJwt({ sub: "user1" }))
        : new Promise<string>((resolve) => (resolveToken = resolve)),
    );
    client.setAuth(fetchToken, () => {});
    client.subscribe("users:me", {});
    await server.settled();
    expect(server.received).toEqual([]);

    resolveToken(fakeJwt({ sub: "user1" }));
    await server.settled();
    const types = server.received.map((m) => m.type);
    expect(types.slice(0, 3)).toEqual([
      "Connect",
      "Authenticate",
      "ModifyQuerySet",
    ]);
    expect(client.localQueryResult("users:me", {})).toBe("user1");
  });

  test("expectAuth holds everything until the first token", async () => {
    const { server, client } = setup(authServer(), { expectAuth: true });
    client.subscribe("users:me", {});
    let resolveToken!: (token: string) => void;
    client.setAuth(
      ({ forceRefreshToken }) =>
        forceRefreshToken
          ? Promise.resolve(fakeJwt({ sub: "user2" }))
          : new Promise<string>((resolve) => (resolveToken = resolve)),
      () => {},
    );
    await server.settled();
    expect(server.received).toEqual([]);
    resolveToken(fakeJwt({ sub: "user2" }));
    await server.settled();
    expect(server.received[0].type).toBe("Connect");
    expect(client.localQueryResult("users:me", {})).toBe("user2");
  });

  test("refetches the token before it expires", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { server, client } = setup(authServer());
    let count = 0;
    const fetchToken = vi.fn(async () =>
      fakeJwt({ sub: `user${++count}` }, 30),
    );
    client.setAuth(fetchToken, () => {});
    client.subscribe("users:me", {});
    await server.settled();
    expect(fetchToken).toHaveBeenCalledTimes(2);
    expect(client.localQueryResult("users:me", {})).toBe("user2");

    // 30s lifetime minus the default 10s leeway.
    await vi.advanceTimersByTimeAsync(19_000);
    expect(fetchToken).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_000);
    await server.settled();
    expect(fetchToken).toHaveBeenCalledTimes(3);
    expect(client.localQueryResult("users:me", {})).toBe("user3");
  });

  test("a server-side expiry refreshes the token with the socket paused", async () => {
    const { server, client } = setup(authServer());
    const fetchToken = vi.fn(async () => fakeJwt({ sub: "user1" }));
    const onChange = vi.fn();
    const onRefreshChange = vi.fn();
    client.setAuth(fetchToken, onChange, onRefreshChange);
    client.subscribe("users:me", {});
    await server.settled();
    server.expireAuth();
    await server.settled();
    expect(fetchToken).toHaveBeenCalledTimes(3);
    expect(onRefreshChange.mock.calls).toEqual([[true], [false]]);
    expect(onChange.mock.calls).toEqual([[true], [true], [true]]);
    expect(server.receivedOfType("Connect")).toHaveLength(2);
    expect(client.localQueryResult("users:me", {})).toBe("user1");
  });

  test("a rejected token ends unauthenticated after bounded retries", async () => {
    const { server, client } = setup(authServer());
    const fetchToken = vi.fn(async () => fakeJwt({ sub: "x", valid: false }));
    const onChange = vi.fn();
    client.setAuth(fetchToken, onChange);
    client.subscribe("users:me", {});
    await server.settled(40);
    expect(onChange.mock.calls).toEqual([[false]]);
    expect(fetchToken.mock.calls.length).toBeLessThanOrEqual(5);
    expect(client.hasAuth()).toBe(false);
    expect(client.localQueryResult("users:me", {})).toBeNull();
  });

  test("no token means unauthenticated", async () => {
    const { server, client } = setup(authServer());
    const fetchToken = vi.fn(
      async (_args: { forceRefreshToken: boolean }) => null,
    );
    const onChange = vi.fn();
    client.setAuth(fetchToken, onChange);
    client.subscribe("users:me", {});
    await server.settled();
    expect(fetchToken.mock.calls.map(([args]) => args)).toEqual([
      { forceRefreshToken: false },
      { forceRefreshToken: true },
    ]);
    expect(onChange.mock.calls).toEqual([[false]]);
    expect(client.localQueryResult("users:me", {})).toBeNull();
  });

  test("clearAuth drops the identity", async () => {
    const { server, client } = setup(authServer());
    client.setAuth(
      async () => fakeJwt({ sub: "user1" }),
      () => {},
    );
    client.subscribe("users:me", {});
    await server.settled();
    expect(client.localQueryResult("users:me", {})).toBe("user1");
    client.clearAuth();
    await server.settled();
    expect(client.hasAuth()).toBe(false);
    expect(client.localQueryResult("users:me", {})).toBeNull();
    expect(server.receivedOfType("Authenticate").at(-1)).toMatchObject({
      tokenType: "None",
    });
  });

  test("a reconnect re-sends the token", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { server, client } = setup(authServer());
    client.setAuth(
      async () => fakeJwt({ sub: "user1" }),
      () => {},
    );
    client.subscribe("users:me", {});
    await server.settled();
    server.dropConnections();
    await server.settled();
    await vi.advanceTimersByTimeAsync(2000);
    await server.settled();
    const afterReconnect = server.received.slice(
      server.received.lastIndexOf(server.receivedOfType("Connect")[1]),
    );
    expect(afterReconnect.map((m) => m.type).slice(0, 3)).toEqual([
      "Connect",
      "Authenticate",
      "ModifyQuerySet",
    ]);
    expect(afterReconnect[1]).toMatchObject({ baseVersion: 0 });
    expect(client.localQueryResult("users:me", {})).toBe("user1");
  });
});

describe("close", () => {
  test("close terminates the socket for good", async () => {
    const { server, client } = setup(messagesStore());
    await server.settled();
    await client.close();
    await server.settled();
    expect(server.connectionCount()).toBe(0);
    expect(client.connectionState().isWebSocketConnected).toBe(false);
  });
});
