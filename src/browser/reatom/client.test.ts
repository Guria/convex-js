import { atom, computed, context, noop } from "@reatom/core";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from "vitest";
import { makeFunctionReference } from "convex/server";
import { ConvexError } from "convex/values";
import {
  internals,
  reatomConvexClient,
  type ReatomConvexClient,
} from "./client.js";
import { clientOptions, FakeServer, settle } from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";
const list = makeFunctionReference<"query", { tag?: string }, string[]>(
  "notes:list",
);
const get = makeFunctionReference<"query", { id: number }, string>("notes:get");
const add = makeFunctionReference<"mutation", { text: string }, null>(
  "notes:add",
);
const page = makeFunctionReference<
  "query",
  { paginationOpts: unknown },
  { page: number[]; isDone: boolean; continueCursor: string }
>("notes:page");

let trace: unknown[];
let server: FakeServer;

beforeEach(() => {
  trace = [];
  server = new FakeServer(trace);
});

afterEach(() => {
  vi.useRealTimers();
});

/** Messages the server received, other than query set changes. */
const received = () =>
  trace
    .filter((entry: any) => entry.received)
    .map((entry: any) => entry.received);

describe("reatomConvexClient", () => {
  test("connects on first use and subscribes what is read", async () => {
    const frame = context.start();
    const convex = reatomConvexClient(URL, clientOptions(server));
    expect(server.sockets).toHaveLength(0);

    const notes = frame.run(() => convex.query(list, {}));
    // Reading without a subscriber neither connects nor subscribes.
    expect(frame.run(() => notes.data())).toBeUndefined();
    expect(server.sockets).toHaveLength(0);

    const seen: unknown[] = [];
    const unsubscribe = frame.run(() =>
      notes.data.subscribe((value) => seen.push(value)),
    );
    await settle();
    expect(server.sockets).toHaveLength(1);
    server.open();
    expect(server.querySet()).toEqual([
      JSON.stringify({ udfPath: "notes:list", args: {} }),
    ]);
    frame.run(() => expect(notes.status()).toBe("loading"));

    server.transition([["notes:list", {}, { value: ["a"] }]]);
    await settle();
    expect(seen).toEqual([undefined, ["a"]]);
    frame.run(() => {
      expect(notes.status()).toBe("success");
      expect(convex.query(list, {})).toBe(notes);
    });

    unsubscribe();
    await settle();
    expect(server.querySet()).toEqual([]);
    await frame.run(() => convex.close());
  });

  test("changing args swaps the subscription in one message", async () => {
    const frame = context.start();
    const convex = reatomConvexClient(URL, clientOptions(server));
    const id = atom(1, "id");
    const note = computed(() => convex.query(get, { id: id() }).data(), "note");
    const unsubscribe = frame.run(() => note.subscribe(noop));
    await settle();
    server.open();
    server.transition([["notes:get", { id: 1 }, { value: "one" }]]);
    frame.run(() => expect(note()).toBe("one"));

    const sent = vi.spyOn(server.socket, "send");
    frame.run(() => id.set(2));
    await settle();
    expect(sent).toHaveBeenCalledTimes(1);
    expect(JSON.parse(sent.mock.calls[0][0]).modifications).toMatchObject([
      { type: "Add", args: [{ id: 2 }] },
      { type: "Remove" },
    ]);
    server.transition([["notes:get", { id: 2 }, { value: "two" }]]);
    frame.run(() => expect(note()).toBe("two"));
    unsubscribe();
    await frame.run(() => convex.close());
  });

  test("errors, mutations with optimistic updates and actions", async () => {
    const frame = context.start();
    const convex = reatomConvexClient(URL, clientOptions(server));
    const tagged = frame.run(() => convex.query(list, { tag: "x" }));
    const notes = frame.run(() => convex.query(list, {}));
    frame.run(() => {
      tagged.error.subscribe(noop);
      notes.data.subscribe(noop);
    });
    await settle();
    server.open();
    server.transition([
      ["notes:list", { tag: "x" }, { error: "nope", errorData: { code: 1 } }],
      ["notes:list", {}, { value: ["a"] }],
    ]);
    frame.run(() => {
      const error = tagged.error();
      expect(error).toBeInstanceOf(ConvexError);
      expect((error as ConvexError<any>).data).toEqual({ code: 1 });
      expect(tagged.status()).toBe("error");
    });

    const result = frame.run(() =>
      convex.mutation(
        add,
        { text: "b" },
        {
          optimisticUpdate: (store, args) =>
            store.setQuery(list, {}, [
              ...(store.getQuery(list, {}) ?? []),
              args.text,
            ]),
        },
      ),
    );
    frame.run(() => {
      expect(notes.data()).toEqual(["a", "b"]);
      expect(internals(convex).optimisticUpdates()).toBe(1);
      expect(convex.connectionState().inflightMutations).toBe(1);
    });
    server.respond(0, { value: null });
    server.transition([["notes:list", {}, { value: ["a", "B"] }]]);
    await expect(result).resolves.toBeNull();
    frame.run(() => {
      expect(notes.data()).toEqual(["a", "B"]);
      expect(internals(convex).optimisticUpdates()).toBe(0);
    });

    const failed = frame.run(() => convex.action("notes:send", {}));
    server.respond(1, { error: "bad", errorData: "why" });
    await expect(failed).rejects.toMatchObject({ data: "why" });
    await frame.run(() => convex.close());
  });

  test("paginated queries are atoms with a loadMore action", async () => {
    const frame = context.start();
    const convex = reatomConvexClient(URL, clientOptions(server));
    const pages = frame.run(() =>
      convex.paginatedQuery(page, {}, { initialNumItems: 2 }),
    );
    const unsubscribe = frame.run(() => pages.results.subscribe(noop));
    await settle();
    server.open();
    frame.run(() => expect(pages.status()).toBe("LoadingFirstPage"));
    server.transition([
      [
        "notes:page",
        { paginationOpts: { cursor: null, numItems: 2, id: -1 } },
        { value: { page: [1, 2], isDone: false, continueCursor: "c" } },
      ],
    ]);
    frame.run(() => {
      expect(pages.results()).toEqual([1, 2]);
      expect(pages.loadMore(5)).toBe(true);
      expect(pages.status()).toBe("LoadingMore");
      expect(pages.loadMore(5)).toBe(false);
    });
    await settle();
    server.transition([
      [
        "notes:page",
        { paginationOpts: { cursor: "c", numItems: 5, id: -1 } },
        { value: { page: [3], isDone: true, continueCursor: "e" } },
      ],
    ]);
    frame.run(() => {
      expect(pages.results()).toEqual([1, 2, 3]);
      expect(pages.status()).toBe("Exhausted");
    });
    unsubscribe();
    await settle();
    expect(server.querySet()).toEqual([]);
    await frame.run(() => convex.close());
  });

  test("auth state is atoms", async () => {
    const frame = context.start();
    const convex = reatomConvexClient(URL, clientOptions(server));
    const token = [
      Buffer.from('{"alg":"none"}').toString("base64url"),
      Buffer.from('{"sub":"me","iat":1,"exp":3601}').toString("base64url"),
      "sig",
    ].join(".");
    frame.run(() => {
      expect(convex.isAuthenticated()).toBe(false);
      convex.setAuth(async () => token);
      expect(convex.isAuthenticated()).toBeNull();
    });
    server.open();
    await settle();
    server.transition([]);
    await settle();
    frame.run(() => {
      expect(convex.isAuthenticated()).toBe(true);
      expect(convex.authClaims()?.decoded.sub).toBe("me");
      convex.clearAuth();
      expect(convex.isAuthenticated()).toBe(false);
      expect(convex.authClaims()).toBeUndefined();
    });
    expect(received().slice(-1)).toEqual(["Authenticate"]);
    await frame.run(() => convex.close());
  });

  test("each context gets its own connection", async () => {
    const servers = [new FakeServer([]), new FakeServer([])];
    let next = 0;
    // Each socket goes to the next server.
    const webSocketConstructor = function (url: string) {
      return new servers[next++].WebSocket(url);
    } as unknown as typeof WebSocket;
    const convex = reatomConvexClient(URL, {
      ...clientOptions(server),
      webSocketConstructor,
    });
    const [first, second] = [context.start(), context.start()];
    for (const frame of [first, second]) {
      frame.run(() => convex.query(list, {}).data.subscribe(noop));
    }
    await settle();
    for (const each of servers) each.open();
    servers[1].transition([["notes:list", {}, { value: ["second"] }]]);
    expect(second.run(() => convex.query(list, {}).data())).toEqual(["second"]);
    expect(first.run(() => convex.query(list, {}).data())).toBeUndefined();
    await first.run(() => convex.close());
    await second.run(() => convex.close());
  });
});

test("function references type arguments and results", () => {
  const typed = (convex: ReatomConvexClient) => {
    expectTypeOf(convex.query(get, { id: 1 }).data()).toEqualTypeOf<
      string | undefined
    >();
    // @ts-expect-error `id` is a number
    convex.query(get, { id: "1" });
    expectTypeOf(
      convex.paginatedQuery(page, {}, { initialNumItems: 1 }).results(),
    ).toEqualTypeOf<number[]>();
    expectTypeOf(convex.mutation(add, { text: "a" })).toEqualTypeOf<
      Promise<null>
    >();
  };
  expect(typed).toBeTypeOf("function");
});
