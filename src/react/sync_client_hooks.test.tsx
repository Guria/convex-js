/**
 * @vitest-environment happy-dom
 */
import React, { act, useState } from "react";
import { createRoot, Root } from "react-dom/client";
import { afterEach, describe, expect, test } from "vitest";
import {
  createFakeConvexServer,
  FakeConvexServer,
  FakeFunctionContext,
  fakePaginate,
} from "../browser/sync/client_node_test_helpers.js";
import { makeFunctionReference } from "../server/api.js";
import { Value } from "../values/index.js";
import {
  ConvexProvider,
  ConvexReactClient,
  useMutation,
  useQuery,
} from "./client.js";
import { usePaginatedQuery } from "./use_paginated_query.js";
import { usePaginatedQuery_experimental } from "./use_paginated_query2.js";

const listRef = makeFunctionReference<"query", { tag?: string }, string[]>(
  "messages:list",
);
const sendRef = makeFunctionReference<"mutation", { body: string }, null>(
  "messages:send",
);
const itemsRef = makeFunctionReference<"query", any, any>("items:list");

(
  globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }
).IS_REACT_ACT_ENVIRONMENT = true;

const servers: FakeConvexServer[] = [];
const clients: ConvexReactClient[] = [];
const roots: Root[] = [];

const text = () => document.body.textContent ?? "";

/** Waits, letting the fake server deliver, until the page shows `expected`. */
async function waitForText(expected: string) {
  for (let i = 0; i < 50; i++) {
    if (text().includes(expected)) return;
    await act(() => new Promise((resolve) => setTimeout(resolve, 2)));
  }
  expect(text()).toContain(expected);
}

afterEach(async () => {
  act(() => roots.splice(0).forEach((root) => root.unmount()));
  document.body.innerHTML = "";
  await Promise.all(clients.splice(0).map((client) => client.close()));
  servers.splice(0).forEach((server) => server.close());
});

function setup(options: Parameters<typeof createFakeConvexServer>[0]) {
  const server = createFakeConvexServer(options);
  servers.push(server);
  const client = new ConvexReactClient("http://127.0.0.1:8000", {
    webSocketConstructor: server.WebSocket,
    unsavedChangesWarning: false,
    logger: false,
  });
  clients.push(client);
  const renderWith = (element: React.ReactElement) => {
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    roots.push(root);
    act(() =>
      root.render(<ConvexProvider client={client}>{element}</ConvexProvider>),
    );
  };
  return { server, client, renderWith };
}

function messages() {
  let list: string[] = [];
  return {
    queries: {
      "messages:list": (_ctx: unknown, { tag }: Record<string, Value>) =>
        tag === undefined
          ? list
          : list.map((body) => `${tag as string}${body}`),
    },
    mutations: {
      "messages:send": (_ctx: unknown, { body }: Record<string, Value>) => {
        list = [...list, body as string];
        return null;
      },
    },
    set(next: string[]) {
      list = next;
    },
  };
}

describe("convex/react on the sync client", () => {
  test("useQuery shows loading, then data, then updates", async () => {
    const store = messages();
    store.set(["first"]);
    const { server, renderWith } = setup(store);
    function List() {
      const list = useQuery(listRef, {});
      return <div>{list === undefined ? "loading" : list.join(",")}</div>;
    }
    renderWith(<List />);
    expect(text()).toContain("loading");
    await waitForText("first");

    store.set(["first", "second"]);
    await act(async () => {
      server.invalidate();
      await server.settled();
    });
    expect(text()).toContain("first,second");
  });

  test("useQuery follows args and skip", async () => {
    const store = messages();
    store.set(["x"]);
    const { server, renderWith } = setup(store);
    let setTag!: (tag: string | null) => void;
    function List() {
      const [tag, set] = useState<string | null>("a:");
      setTag = set;
      const list = useQuery(listRef, tag === null ? "skip" : { tag });
      return <div>{list === undefined ? "none" : list.join(",")}</div>;
    }
    renderWith(<List />);
    await waitForText("a:x");
    act(() => setTag("b:"));
    await waitForText("b:x");
    act(() => setTag(null));
    await waitForText("none");
    await act(() => server.settled());
    expect(server.subscriptionCount("messages:list")).toBe(0);
  });

  test("equal useQuery calls share one subscription", async () => {
    const { server, renderWith } = setup(messages());
    function List({ id }: { id: number }) {
      const list = useQuery(listRef, {});
      return <div>{list === undefined ? "loading" : `list${id}`}</div>;
    }
    renderWith(
      <>
        <List id={1} />
        <List id={2} />
        <List id={3} />
      </>,
    );
    await waitForText("list3");
    expect(server.subscriptionCount("messages:list")).toBe(1);
    const adds = server
      .receivedOfType("ModifyQuerySet")
      .flatMap((change) => change.modifications)
      .filter((m) => m.type === "Add");
    expect(adds).toHaveLength(1);
  });

  test("useMutation applies an optimistic update, then the server result", async () => {
    const store = messages();
    const { server, renderWith } = setup(store);
    let send!: (body: string) => Promise<null>;
    function List() {
      const list = useQuery(listRef, {});
      const mutate = useMutation(sendRef).withOptimisticUpdate(
        (localStore, { body }) => {
          const current = localStore.getQuery(listRef, {});
          if (current !== undefined) {
            localStore.setQuery(listRef, {}, [...current, `${body}...`]);
          }
        },
      );
      send = (body) => mutate({ body });
      return (
        <div>{list === undefined ? "loading" : list.join(",") || "-"}</div>
      );
    }
    renderWith(<List />);
    await waitForText("-");

    const release = server.holdMutations();
    let sent!: Promise<null>;
    act(() => {
      sent = send("hi");
    });
    expect(text()).toContain("hi...");
    await act(async () => {
      release();
      await sent;
    });
    expect(text()).toContain("hi");
  });

  const itemsServer = () => {
    const items = Array.from({ length: 5 }, (_, n) => ({ _id: `id${n}`, n }));
    return {
      queries: {
        "items:list": (
          ctx: FakeFunctionContext,
          { paginationOpts }: Record<string, Value>,
        ) => fakePaginate(ctx, items, paginationOpts),
      },
    };
  };

  test("usePaginatedQuery loads more pages", async () => {
    const { renderWith } = setup(itemsServer());
    let loadMore!: (n: number) => void;
    function Items() {
      const {
        results,
        status,
        loadMore: more,
      } = usePaginatedQuery(itemsRef, {}, { initialNumItems: 2 });
      loadMore = more;
      return (
        <div>
          {status}:{results.map((item: { n: number }) => item.n).join(",")}
        </div>
      );
    }
    renderWith(<Items />);
    await waitForText("CanLoadMore:0,1");
    act(() => loadMore(2));
    await waitForText("CanLoadMore:0,1,2,3");
    act(() => loadMore(2));
    await waitForText("Exhausted:0,1,2,3,4");
  });

  test("usePaginatedQuery_experimental loads more pages", async () => {
    const { renderWith } = setup(itemsServer());
    let loadMore!: (n: number) => void;
    function Items() {
      const result = usePaginatedQuery_experimental(
        itemsRef,
        {},
        { initialNumItems: 3 },
      );
      loadMore = result.loadMore;
      return (
        <div>
          {result.status}:
          {result.results.map((item: { n: number }) => item.n).join(",")}
        </div>
      );
    }
    renderWith(<Items />);
    await waitForText("CanLoadMore:0,1,2");
    act(() => loadMore(5));
    await waitForText("Exhausted:0,1,2,3,4");
  });
});
