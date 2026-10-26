import { afterEach, describe, expect, test, vi } from "vitest";
import { makeFunctionReference } from "../server/api.js";
import { PaginationResult } from "../server/index.js";
import { Value } from "../values/index.js";
import { ConvexClient } from "./simple_client.js";
import {
  createFakeConvexServer,
  FakeConvexServer,
  FakeFunctionContext,
  fakePaginate,
} from "./sync/client_node_test_helpers.js";
import { PaginatedQueryResult } from "./sync/pagination.js";

const listRef = makeFunctionReference<"query">("messages:list");
const sendRef = makeFunctionReference<"mutation">("messages:send");
const pagesRef = makeFunctionReference<"query">("items:list");

const servers: FakeConvexServer[] = [];
const clients: ConvexClient[] = [];

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  servers.splice(0).forEach((server) => server.close());
});

type Item = { _id: string; n: number };

function setup(options: Parameters<typeof createFakeConvexServer>[0]) {
  const server = createFakeConvexServer(options);
  servers.push(server);
  const client = new ConvexClient("http://127.0.0.1:8000", {
    webSocketConstructor: server.WebSocket,
    logger: false,
  });
  clients.push(client);
  return { server, client };
}

function messages() {
  let list: string[] = [];
  return {
    queries: { "messages:list": () => list },
    mutations: {
      "messages:send": (_ctx: unknown, { body }: Record<string, Value>) => {
        list = [...list, body as string];
        return null;
      },
    },
  };
}

describe("ConvexClient", () => {
  test("onUpdate calls back with each result", async () => {
    const { server, client } = setup(messages());
    const results: unknown[] = [];
    const unsubscribe = client.onUpdate(listRef, {}, (value) =>
      results.push(value),
    );
    await server.settled();
    expect(results).toEqual([[]]);
    await client.mutation(sendRef, { body: "a" });
    expect(results).toEqual([[], ["a"]]);
    expect(unsubscribe.getCurrentValue()).toEqual(["a"]);
    unsubscribe();
    await client.mutation(sendRef, { body: "b" });
    expect(results).toEqual([[], ["a"]]);
  });

  test("a second listener gets the current value and shares the subscription", async () => {
    const { server, client } = setup(messages());
    client.onUpdate(listRef, {}, () => {});
    await server.settled();
    const late = vi.fn();
    client.onUpdate(listRef, {}, late);
    // Delivered from a timer, not by a server message.
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(late).toHaveBeenCalledWith(
      [],
      "Second argument to onUpdate callback is reserved for later use",
    );
    expect(server.subscriptionCount("messages:list")).toBe(1);
  });

  test("query resolves once and unsubscribes", async () => {
    const { server, client } = setup(messages());
    await expect(client.query(listRef, {})).resolves.toEqual([]);
    await server.settled();
    expect(server.subscriptionCount("messages:list")).toBe(0);
  });

  test("errors go to onError", async () => {
    const { server, client } = setup({
      queries: {
        "messages:list": () => {
          throw new Error("denied");
        },
      },
    });
    const onError = vi.fn();
    client.onUpdate(listRef, {}, () => {}, onError);
    await server.settled();
    expect(onError.mock.calls[0][0].message).toMatch(/denied/);
  });

  test("close stops callbacks", async () => {
    const { server, client } = setup(messages());
    const callback = vi.fn();
    client.onUpdate(listRef, {}, callback);
    await server.settled();
    await client.close();
    expect(client.closed).toBe(true);
    server.invalidate();
    await server.settled();
    expect(callback).toHaveBeenCalledTimes(1);
  });

  test("a disabled client does nothing", async () => {
    const client = new ConvexClient("http://127.0.0.1:8000", {
      disabled: true,
    });
    expect(client.onUpdate(listRef, {}, () => {}).getCurrentValue()).toBe(
      undefined,
    );
    await expect(client.mutation(sendRef, {})).rejects.toThrow(/disabled/);
    await client.close();
  });
});

describe("paginated queries", () => {
  const itemsServer = (count: number) => {
    const items: Item[] = Array.from({ length: count }, (_, n) => ({
      _id: `id${n}`,
      n,
    }));
    return {
      items,
      options: {
        queries: {
          "items:list": (
            ctx: FakeFunctionContext,
            { paginationOpts }: Record<string, Value>,
          ) => fakePaginate(ctx, items, paginationOpts),
        },
      },
    };
  };

  const watch = (client: ConvexClient) => {
    const updates: PaginationResult<Item>[] = [];
    const unsubscribe = client.onPaginatedUpdate_experimental(
      pagesRef,
      {},
      { initialNumItems: 2 },
      (result) => updates.push(result as PaginationResult<Item>),
    );
    const current = () =>
      unsubscribe.getCurrentValue() as unknown as PaginatedQueryResult<Item>;
    return { updates, unsubscribe, current };
  };

  test("loads pages until exhausted", async () => {
    const { options } = itemsServer(5);
    const { server, client } = setup(options);
    const { current } = watch(client);
    expect(current().status).toBe("LoadingFirstPage");
    await server.settled();
    expect(current().status).toBe("CanLoadMore");
    expect(current().results.map((item) => item.n)).toEqual([0, 1]);

    expect(current().loadMore(2)).toBe(true);
    expect(current().status).toBe("LoadingMore");
    expect(current().loadMore(2)).toBe(false);
    await server.settled();
    expect(current().results.map((item) => item.n)).toEqual([0, 1, 2, 3]);

    current().loadMore(5);
    await server.settled();
    expect(current().status).toBe("Exhausted");
    expect(current().results.map((item) => item.n)).toEqual([0, 1, 2, 3, 4]);
    expect(current().loadMore(1)).toBe(false);
    expect(server.subscriptionCount("items:list")).toBe(3);
  });

  test("loaded pages stay live", async () => {
    const { options, items } = itemsServer(3);
    const { server, client } = setup(options);
    const { current } = watch(client);
    await server.settled();
    items[1].n = 100;
    server.invalidate();
    await server.settled();
    expect(current().results.map((item) => item.n)).toEqual([0, 100]);
  });

  test("equal paginated subscriptions share pages and unsubscribe together", async () => {
    const { options } = itemsServer(5);
    const { server, client } = setup(options);
    const first = watch(client);
    const second = watch(client);
    await server.settled();
    first.current().loadMore(2);
    await server.settled();
    expect(second.current().results).toHaveLength(4);
    expect(server.subscriptionCount("items:list")).toBe(2);
    first.unsubscribe();
    await server.settled();
    expect(server.subscriptionCount("items:list")).toBe(2);
    second.unsubscribe();
    await server.settled();
    expect(server.subscriptionCount("items:list")).toBe(0);
  });

  test("a page the server asks to split is replaced by two", async () => {
    const items: Item[] = Array.from({ length: 6 }, (_, n) => ({
      _id: `id${n}`,
      n,
    }));
    const { server, client } = setup({
      queries: {
        "items:list": (ctx, { paginationOpts }) => {
          const opts = paginationOpts as { endCursor?: string | null };
          const page = fakePaginate(ctx, items, paginationOpts);
          // The first, unsplit page grew too big.
          return opts.endCursor === undefined && page.page.length >= 2
            ? {
                ...fakePaginate(ctx, items, {
                  ...(paginationOpts as Record<string, Value>),
                  endCursor: "id5",
                }),
                splitCursor: "id2",
                pageStatus: "SplitRequired",
              }
            : page;
        },
      },
    });
    const { current } = watch(client);
    await server.settled();
    expect(current().results.map((item) => item.n)).toEqual([0, 1, 2, 3, 4, 5]);
    expect(server.subscriptionCount("items:list")).toBe(2);
    const adds = server
      .receivedOfType("ModifyQuerySet")
      .flatMap((change) => change.modifications)
      .filter((m) => m.type === "Add")
      .map((m) => (m as { args: any[] }).args[0].paginationOpts);
    expect(adds.slice(1)).toEqual([
      { numItems: 2, id: -1, cursor: null, endCursor: "id2" },
      { numItems: 2, id: -1, cursor: "id2", endCursor: "id5" },
    ]);
  });
});
