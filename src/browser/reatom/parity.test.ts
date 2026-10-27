import { describe, expect, test, vi } from "vitest";
import { makeFunctionReference } from "../../server/api.js";
import type { BaseConvexClient } from "../sync/client.js";
import type { OptimisticLocalStore } from "../sync/optimistic_updates.js";
import {
  attempt,
  caught,
  clientOptions,
  expectParity,
  FakeServer,
  Impl,
  runBoth,
} from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";
const list = makeFunctionReference<"query">("notes:list");
const get = makeFunctionReference<"query">("notes:get");

describe("BaseConvexClient", () => {
  test("subscribe, results, dedupe and unsubscribe", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        (tokens) => log({ transition: tokens.length }),
        clientOptions(server),
      );
      server.open();
      const first = client.subscribe("notes:list", {});
      const second = client.subscribe("notes:list", {});
      const other = client.subscribe("notes:get", { id: 1 });
      log({ sameToken: first.queryToken === second.queryToken });
      await checkpoint("subscribed");
      log({ before: client.localQueryResult("notes:list", {}) });
      server.transition([
        ["notes:list", {}, { value: ["a"], journal: "j1" }],
        ["notes:get", { id: 1 }, { value: "a" }],
      ]);
      log({
        list: client.localQueryResult("notes:list", {}),
        get: client.localQueryResult("notes:get", { id: 1 }),
        journal: client.queryJournal("notes:list", {}),
        has: client.hasLocalQueryResultByToken(first.queryToken),
        ts: client.getMaxObservedTimestamp()?.toString(),
      });
      first.unsubscribe();
      await checkpoint("one of two unsubscribed");
      second.unsubscribe();
      other.unsubscribe();
      await checkpoint("all unsubscribed");
      await client.close();
    });
  });

  test("failed queries throw from localQueryResult", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      const { queryToken } = client.subscribe("notes:get", { id: 2 });
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.transition([
        ["notes:get", { id: 2 }, { error: "boom" }],
        ["notes:list", {}, { error: "denied", errorData: { code: 403 } }],
      ]);
      log(attempt(() => client.localQueryResult("notes:get", { id: 2 })));
      log(attempt(() => client.localQueryResult("notes:list", {})));
      log({ has: client.hasLocalQueryResultByToken(queryToken) });
      await client.close();
    });
  });

  test("mutations resolve once reflected, with optimistic updates", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client: BaseConvexClient = new impl.BaseConvexClient(
        URL,
        (tokens) =>
          log({
            transition: tokens.map((token) => JSON.parse(token).udfPath),
            list: attempt(() => client.localQueryResult("notes:list", {})),
          }),
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.transition([["notes:list", {}, { value: ["a"] }]]);
      const result = client
        .mutation(
          "notes:add",
          { text: "b" },
          {
            optimisticUpdate: (store: OptimisticLocalStore, args: any) => {
              const current = store.getQuery(list, {}) ?? [];
              store.setQuery(list, {}, [...current, args.text]);
              log({
                all: store.getAllQueries(list),
                missing: store.getQuery(get, { id: 9 }),
              });
            },
          },
        )
        .then((value: unknown) => log({ resolved: value }));
      log({
        optimistic: client.localQueryResult("notes:list", {}),
        state: client.connectionState(),
      });
      await settle();
      server.respond(0, { value: "id-b" });
      await settle();
      log({ afterResponse: client.localQueryResult("notes:list", {}) });
      server.transition([["notes:list", {}, { value: ["a", "b"] }]]);
      await result;
      log({
        reflected: client.localQueryResult("notes:list", {}),
        state: client.connectionState(),
      });
      await client.close();
    });
  });

  test("failed mutations roll back and throw ConvexError data", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        (tokens) => log({ transition: tokens.length }),
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.transition([["notes:list", {}, { value: ["a"] }]]);
      const result = caught(
        client.mutation(
          "notes:add",
          { text: "b" },
          {
            optimisticUpdate: (store) => store.setQuery(list, {}, ["x"]),
          },
        ),
      );
      const plain = caught(client.mutation("notes:add", { text: "c" }));
      log({ optimistic: client.localQueryResult("notes:list", {}) });
      server.respond(0, { error: "nope", errorData: { why: "full" } });
      server.respond(1, { error: "plain" });
      log(await result, await plain);
      log({ rolledBack: client.localQueryResult("notes:list", {}) });
      await client.close();
    });
  });

  test("actions resolve on response and fail on reconnect", async () => {
    await expectParity(async ({ impl, server, log, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      const ok = caught(client.action("notes:send", { to: "x" }));
      const failed = caught(client.action("notes:send", { to: "y" }));
      const lost = caught(client.action("notes:send", { to: "z" }));
      server.respond(0, { value: 42, logLines: ["[LOG] hi"] });
      server.respond(1, { error: "bad", errorData: [1, 2] });
      log(await ok, await failed);
      log({ state: client.connectionState() });
      server.drop(1011, "InternalServerError");
      await settle();
      await vi.advanceTimersByTimeAsync(2000);
      server.open();
      log(await lost);
      log({ state: client.connectionState() });
      await client.close();
    });
  });

  test("reconnect resends queries, auth and mutations", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      client.subscribe("notes:get", { id: 1 }, { journal: "start" });
      await checkpoint("subscribed");
      server.transition([["notes:list", {}, { value: [], journal: "j" }]]);
      const pending = client.mutation("notes:add", { text: "x" });
      server.respond(0, { value: null });
      server.drop(1006, "");
      await settle();
      log({ state: client.connectionState() });
      await vi.advanceTimersByTimeAsync(1500);
      server.open();
      await checkpoint("reconnected");
      log({ state: client.connectionState() });
      server.transition([
        ["notes:list", {}, { value: ["x"] }],
        ["notes:get", { id: 1 }, { value: 1 }],
      ]);
      log({ resolved: await pending });
      log({ state: client.connectionState() });
      await client.close();
    });
  });

  test("connection state subscribers see changes", async () => {
    await expectParity(async ({ impl, server, log, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      const unsubscribe = client.subscribeToConnectionState((state) =>
        log({ published: state }),
      );
      await settle();
      server.open();
      await settle();
      const failed = caught(client.mutation("notes:add", {}));
      await settle();
      server.respond(0, { error: "x" });
      log(await failed);
      await settle();
      unsubscribe();
      server.drop();
      await settle();
      await client.close();
    });
  });

  test("componentPath and admin auth", async () => {
    await expectParity(async ({ impl, server, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.setAdminAuth("admin-key", {
        subject: "user",
        issuer: "issuer",
      } as any);
      client.subscribe("notes:list", {}, { componentPath: "child" });
      void client.mutationInternal("notes:add", { a: 1 }, undefined, "child");
      void client.actionInternal("notes:send", {}, "child");
      await checkpoint("component queries");
      await client.close();
    });
  });
});

/** An unsigned JWT; the client only decodes `iat` and `exp`. */
const jwt = (name: string, iat: number, exp: number) =>
  [
    Buffer.from(JSON.stringify({ alg: "none" })).toString("base64url"),
    Buffer.from(JSON.stringify({ sub: name, iat, exp })).toString("base64url"),
    name,
  ].join(".");

/** A token fetcher handing out `tokens` in order and logging each call. */
const fetcher =
  (log: (...entry: unknown[]) => void, tokens: Array<string | null>) =>
  async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
    const token = tokens.shift() ?? null;
    log({ fetchToken: forceRefreshToken, token: token?.split(".")[2] });
    return token;
  };

describe("auth", () => {
  test("cached token, fresh token, scheduled refetch", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      client.setAuth(
        fetcher(log, [jwt("cached", 1, 3601), jwt("fresh", 1, 61)]),
        (isAuthenticated) => log({ onChange: isAuthenticated }),
        (isRefreshing) => log({ onRefreshChange: isRefreshing }),
      );
      await checkpoint("paused for token");
      log({ hasAuth: client.hasAuth() });
      server.transition([["notes:list", {}, { value: 1 }]]);
      await settle();
      log({ claims: client.getCurrentAuthClaims()?.decoded });
      server.transition([["notes:list", {}, { value: 2 }]]);
      await settle();
      log({ claims: client.getCurrentAuthClaims()?.decoded });
      await vi.advanceTimersByTimeAsync(49_000);
      log("before refetch");
      await vi.advanceTimersByTimeAsync(2_000);
      await checkpoint("refetched");
      await client.close();
    });
  });

  test("no cached token: force refetch, then confirmation", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.setAuth(
        fetcher(log, [null, jwt("fresh", 1, 601)]),
        (isAuthenticated) => log({ onChange: isAuthenticated }),
      );
      await checkpoint("fetched");
      server.transition([]);
      await settle();
      log({ hasAuth: client.hasAuth() });
      await client.close();
    });
  });

  test("no token at all reports unauthenticated", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      client.setAuth(fetcher(log, [null, null]), (isAuthenticated) =>
        log({ onChange: isAuthenticated }),
      );
      await checkpoint("gave up");
      log({ hasAuth: client.hasAuth() });
      await client.close();
    });
  });

  test("a rejected token is replaced with the socket stopped", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      client.setAuth(
        fetcher(log, [
          jwt("cached", 1, 3601),
          jwt("fresh", 1, 3601),
          jwt("replacement", 1, 3601),
        ]),
        (isAuthenticated) => log({ onChange: isAuthenticated }),
        (isRefreshing) => log({ onRefreshChange: isRefreshing }),
      );
      await settle();
      server.transition([["notes:list", {}, { value: 1 }]]);
      await settle();
      server.transition([]);
      await settle();
      // An expired token while running a function is not a token update.
      server.authError("expired", { authUpdateAttempted: false });
      await settle();
      log({ state: client.connectionState() });
      await settle();
      server.open();
      await checkpoint("reauthenticated");
      server.transition([["notes:list", {}, { value: 2 }]]);
      await settle();
      log({ claims: client.getCurrentAuthClaims()?.decoded });
      await client.close();
    });
  });

  test("a fresh token rejected too often gives up", async () => {
    await expectParity(async ({ impl, server, log, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      client.setAuth(
        fetcher(log, [
          null,
          jwt("a", 1, 3601),
          jwt("b", 1, 3601),
          jwt("c", 1, 3601),
          jwt("d", 1, 3601),
        ]),
        (isAuthenticated) => log({ onChange: isAuthenticated }),
        (isRefreshing) => log({ onRefreshChange: isRefreshing }),
      );
      await settle();
      for (let attempt = 0; attempt < 3; attempt++) {
        const sockets = server.sockets.length;
        server.authError("invalid", { authUpdateAttempted: true });
        await settle();
        if (server.sockets.length > sockets) server.open();
        await settle();
      }
      log({ hasAuth: client.hasAuth() });
      await client.close();
    });
  });

  test("initialAuthTokenReuse keeps the cached token", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      vi.setSystemTime(1_000_000);
      const client = new impl.BaseConvexClient(URL, () => {}, {
        ...clientOptions(server),
        initialAuthTokenReuse: true,
      });
      server.open();
      client.setAuth(
        fetcher(log, [jwt("cached", 900, 1100), jwt("fresh", 1080, 1200)]),
        (isAuthenticated) => log({ onChange: isAuthenticated }),
      );
      await settle();
      // The server's clock is 20s behind: 120s left on the cached token.
      server.transition([], { clientClockSkew: 20_000 });
      await settle();
      for (let second = 0; second < 109; second += 10) {
        server.push({ type: "Ping" });
        await vi.advanceTimersByTimeAsync(10_000);
      }
      log("before refetch");
      await vi.advanceTimersByTimeAsync(2_000);
      await checkpoint("refetched");
      await client.close();
    });
  });

  test("expectAuth holds requests until the first token", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(URL, () => {}, {
        ...clientOptions(server),
        expectAuth: true,
      });
      server.open();
      client.subscribe("notes:list", {});
      const mutation = caught(client.mutation("notes:add", {}));
      await checkpoint("held");
      log({ state: client.connectionState() });
      client.setAuth(fetcher(log, [jwt("cached", 1, 3601)]), (value) =>
        log({ onChange: value }),
      );
      await checkpoint("sent");
      server.respond(0, { value: "ok" });
      server.transition([["notes:list", {}, { value: [] }]]);
      log(await mutation);
      await settle();
      await client.close();
    });
  });

  test("setAuth during a token fetch wins", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      let release!: (token: string) => void;
      client.setAuth(
        () => new Promise<string>((resolve) => (release = resolve)),
        (value) => log({ first: value }),
      );
      await settle();
      client.setAuth(fetcher(log, [jwt("second", 1, 3601)]), (value) =>
        log({ second: value }),
      );
      release(jwt("first", 1, 3601));
      await checkpoint("raced");
      server.transition([]);
      await settle();
      await client.close();
    });
  });
});

/** A page result as a paginated query function returns it. */
const pageOf = (
  items: unknown[],
  continueCursor: string,
  extra: Record<string, unknown> = {},
) => ({
  value: { page: items, isDone: false, continueCursor, ...extra },
});

/** A paginated client over a base client, logging each paginated update. */
const paginatedClient = (
  impl: Impl,
  server: FakeServer,
  log: (...entry: unknown[]) => void,
) => {
  const base = new impl.BaseConvexClient(URL, () => {}, clientOptions(server));
  const paginated = new impl.PaginatedQueryClient(base, (transition) => {
    for (const { modification } of transition.paginatedQueries) {
      log(
        modification.kind === "Updated"
          ? {
              results: modification.result?.results,
              status: modification.result?.status,
            }
          : { paginated: modification.kind },
      );
    }
  });
  return { base, paginated };
};

describe("pagination", () => {
  test("first page, load more, exhaust", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const { base, paginated } = paginatedClient(impl, server, log);
      server.open();
      const options = { initialNumItems: 2, id: 1 };
      const { unsubscribe } = paginated.subscribe(
        "notes:page",
        { channel: "a" },
        options,
      );
      const current = (): any =>
        paginated.localQueryResult("notes:page", { channel: "a" }, options);
      log({ loading: current()?.status });
      await checkpoint("first page");
      server.transition([
        [
          "notes:page",
          {
            channel: "a",
            paginationOpts: { cursor: null, numItems: 2, id: 1 },
          },
          pageOf([1, 2], "c1"),
        ],
      ]);
      log({ current: current()?.status });
      log({ loadMore: current().loadMore(3) });
      log({ again: current().loadMore(3) });
      await checkpoint("second page");
      server.transition([
        [
          "notes:page",
          {
            channel: "a",
            paginationOpts: { cursor: "c1", numItems: 3, id: 1 },
          },
          { value: { page: [3, 4], isDone: true, continueCursor: "end" } },
        ],
      ]);
      log({ exhausted: current().loadMore(1) });
      unsubscribe();
      await checkpoint("unsubscribed");
      await base.close();
    });
  });

  test("page splits replace a page once both halves load", async () => {
    await expectParity(async ({ impl, server, checkpoint, log }) => {
      const { base, paginated } = paginatedClient(impl, server, log);
      const { unsubscribe } = paginated.subscribe(
        "notes:page",
        {},
        { initialNumItems: 2, id: 7 },
      );
      server.open();
      await checkpoint("first page");
      server.transition([
        [
          "notes:page",
          { paginationOpts: { cursor: null, numItems: 2, id: 7 } },
          pageOf([1, 2, 3, 4, 5], "c5", {
            splitCursor: "c2",
            pageStatus: "SplitRequired",
          }),
        ],
      ]);
      await checkpoint("splitting");
      server.transition([
        [
          "notes:page",
          {
            paginationOpts: {
              numItems: 2,
              id: 7,
              cursor: null,
              endCursor: "c2",
            },
          },
          pageOf([1, 2], "c2"),
        ],
      ]);
      await checkpoint("one half loaded");
      server.transition([
        [
          "notes:page",
          {
            paginationOpts: {
              numItems: 2,
              id: 7,
              cursor: "c2",
              endCursor: "c5",
            },
          },
          pageOf([3, 4, 5], "c5"),
        ],
      ]);
      await checkpoint("split done");
      unsubscribe();
      await checkpoint("unsubscribed");
      await base.close();
    });
  });
});

describe("protocol", () => {
  test("chunked transitions, pings and inactivity", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(
        URL,
        (tokens) => log({ transition: tokens.length }),
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.chunkedTransition(
        [["notes:list", {}, { value: "x".repeat(50) }]],
        3,
      );
      log({ value: client.localQueryResult("notes:list", {}) });
      server.push({ type: "Ping" });
      await vi.advanceTimersByTimeAsync(59_000);
      server.push({ type: "Ping" });
      await vi.advanceTimersByTimeAsync(59_000);
      log({ state: client.connectionState() });
      await vi.advanceTimersByTimeAsync(2_000);
      await settle();
      log({ state: client.connectionState() });
      await vi.advanceTimersByTimeAsync(200);
      server.open();
      await checkpoint("reconnected after inactivity");
      await client.close();
    });
  });

  test("bad chunks, fatal errors and invalid versions throw", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const client = new impl.BaseConvexClient(
        URL,
        () => {},
        clientOptions(server),
      );
      server.open();
      log(
        attempt(() =>
          server.push({
            type: "TransitionChunk",
            chunk: "{",
            partNumber: 1,
            totalParts: 2,
            transitionId: "x",
          }),
        ),
      );
      log(
        attempt(() =>
          server.push({
            type: "Transition",
            startVersion: { querySet: 5, identity: 0, ts: "AAAAAAAAAAA=" },
            endVersion: { querySet: 5, identity: 0, ts: "AQAAAAAAAAA=" },
            modifications: [],
          }),
        ),
      );
      log(attempt(() => server.push({ type: "FatalError", error: "bye" })));
      log({ state: client.connectionState() });
      await client.close();
    });
  });

  test("backoff grows until the client syncs past a reconnect", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const errors: string[] = [];
      const client = new impl.BaseConvexClient(URL, () => {}, {
        ...clientOptions(server),
        onServerDisconnectError: (message) => errors.push(message),
      });
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      for (const reason of ["TooManyConcurrentRequests", "", "Unknown"]) {
        server.drop(1011, reason);
        await settle();
        log({ retries: client.connectionState().connectionRetries });
        await vi.advanceTimersByTimeAsync(20_000);
        server.open();
        await settle();
      }
      log({ errors });
      server.transition([["notes:list", {}, { value: 1 }]]);
      log({ retries: client.connectionState().connectionRetries });
      await client.close();
    });
  });

  test("removed queries, unknown responses and query logs", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.BaseConvexClient(
        URL,
        (tokens) => log({ transition: tokens.length }),
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.transition([
        ["notes:list", {}, { value: 1, logLines: ["[LOG] 'hello'"] }],
      ]);
      log({ logs: client.localQueryLogs("notes:list", {}) });
      server.push({
        type: "MutationResponse",
        requestId: 99,
        success: false,
        result: "unknown",
        logLines: [],
      });
      server.transition([["notes:list", {}, "removed"]]);
      log({ afterRemoved: client.localQueryResult("notes:list", {}) });
      await client.close();
    });
  });
});

describe("optimistic updates", () => {
  test("compose in order and drop as mutations land", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client: BaseConvexClient = new impl.BaseConvexClient(
        URL,
        () => log({ list: client.localQueryResult("notes:list", {}) }),
        clientOptions(server),
      );
      server.open();
      client.subscribe("notes:list", {});
      client.subscribe("notes:list", { tag: "x" });
      await checkpoint("subscribed");
      server.transition([
        ["notes:list", {}, { value: ["a"] }],
        ["notes:list", { tag: "x" }, { value: ["x"] }],
      ]);
      const append = (text: string) => (store: OptimisticLocalStore) => {
        for (const { args, value } of store.getAllQueries(list)) {
          store.setQuery(list, args, [...((value as string[]) ?? []), text]);
        }
        store.setQuery(get, { id: text }, text);
        store.setQuery(get, { id: "loading" }, undefined);
      };
      const first = client.mutation(
        "notes:add",
        {},
        { optimisticUpdate: append("1") },
      );
      const second = client.mutation(
        "notes:add",
        {},
        { optimisticUpdate: append("2") },
      );
      log({
        tagged: client.localQueryResult("notes:list", { tag: "x" }),
        optimisticOnly: client.localQueryResult("notes:get", { id: "2" }),
      });
      await settle();
      server.respond(1, { value: "second" });
      server.transition([["notes:list", {}, { value: ["a", "2"] }]]);
      log({ second: await second });
      server.respond(0, { value: "first" });
      server.transition([
        ["notes:list", {}, { value: ["a", "1", "2"] }],
        ["notes:list", { tag: "x" }, { value: ["x", "1", "2"] }],
      ]);
      log({ first: await first });
      log({
        optimisticOnly: client.localQueryResult("notes:get", { id: "2" }),
      });
      await client.close();
    });
  });
});

// Where this package differs from convex@1.48.0 on purpose.
describe("deviations", () => {
  test("a failed page throws from localQueryResult, not from the socket handler", async () => {
    const { upstream, reatom } = await runBoth(
      async ({ impl, server, log, checkpoint }) => {
        const { base, paginated } = paginatedClient(impl, server, log);
        const options = { initialNumItems: 1, id: 1 };
        paginated.subscribe("notes:page", {}, options);
        server.open();
        await checkpoint("subscribed");
        log(
          attempt(() =>
            server.transition([
              [
                "notes:page",
                { paginationOpts: { cursor: null, numItems: 1, id: 1 } },
                { error: "page failed" },
              ],
            ]),
          ),
        );
        log(
          attempt(() => paginated.localQueryResult("notes:page", {}, options)),
        );
        await base.close();
      },
    );
    const afterSubscribe = (trace: any[]) =>
      trace.slice(
        trace.findIndex((entry) => entry?.checkpoint === "subscribed") + 1,
      );
    const failed = {
      error: expect.stringContaining("page failed"),
      name: "Error",
      data: undefined,
    };
    expect(afterSubscribe(upstream)).toEqual([failed, failed]);
    expect(afterSubscribe(reatom)).toEqual([
      { results: undefined, status: undefined },
      { value: undefined },
      failed,
    ]);
  });

  test("clearAuth stops the auth flow, so a scheduled refetch can't sign in again", async () => {
    const { upstream, reatom } = await runBoth(
      async ({ impl, server, log, settle }) => {
        const client = new impl.BaseConvexClient(
          URL,
          () => {},
          clientOptions(server),
        );
        server.open();
        client.setAuth(
          fetcher(log, [
            jwt("cached", 1, 3601),
            jwt("fresh", 1, 61),
            jwt("stale", 1, 61),
          ]),
          (value) => log({ onChange: value }),
        );
        await settle();
        server.transition([]);
        await settle();
        server.transition([]);
        await settle();
        client.clearAuth();
        await settle();
        log("cleared");
        await vi.advanceTimersByTimeAsync(60_000);
        log({ hasAuth: client.hasAuth() });
        await client.close();
      },
    );
    const afterClear = (trace: unknown[]) =>
      trace.slice(trace.indexOf("cleared"));
    expect(afterClear(upstream)).toContainEqual({
      received: "Authenticate",
      tokenType: "User",
      value: jwt("stale", 1, 61),
      impersonating: undefined,
    });
    expect(afterClear(reatom)).toEqual(["cleared", { hasAuth: false }]);
  });

  test("results stay visible across a reconnect until the server answers", async () => {
    const { upstream, reatom } = await runBoth(
      async ({ impl, server, log, checkpoint, settle }) => {
        const client = new impl.BaseConvexClient(
          URL,
          () => {},
          clientOptions(server),
        );
        server.open();
        client.subscribe("notes:list", {});
        client.subscribe("notes:get", { id: 1 });
        await checkpoint("subscribed");
        server.transition([
          ["notes:list", {}, { value: ["a"] }],
          ["notes:get", { id: 1 }, { value: "one" }],
        ]);
        server.drop();
        await settle();
        await vi.advanceTimersByTimeAsync(1500);
        server.open();
        await settle();
        server.transition([["notes:list", {}, { value: ["b"] }]]);
        log({
          list: client.localQueryResult("notes:list", {}),
          get: client.localQueryResult("notes:get", { id: 1 }),
        });
        await client.close();
      },
    );
    expect(upstream.at(-1)).toEqual({ list: ["b"], get: undefined });
    expect(reatom.at(-1)).toEqual({ list: ["b"], get: "one" });
  });
});
