// @vitest-environment happy-dom
import React, { act } from "react";
import { createRoot } from "react-dom/client";
import { describe, test } from "vitest";
import { makeFunctionReference } from "../server/api.js";
import {
  clientOptions,
  expectParity,
  Impl,
} from "../browser/reatom/parity_test_helpers.js";

(globalThis as any).IS_REACT_ACT_ENVIRONMENT = true;

const URL = "https://happy-otter-123.convex.cloud";
const list = makeFunctionReference<"query", { tag?: string }, string[]>(
  "notes:list",
);
const add = makeFunctionReference<"mutation", { text: string }>("notes:add");
const page = makeFunctionReference<"query">("notes:page");

class Boundary extends React.Component<
  { children: React.ReactNode },
  { error: string | null }
> {
  state = { error: null as string | null };
  static getDerivedStateFromError(error: Error) {
    return { error: error.message.split("\n")[0] };
  }
  render() {
    return this.state.error
      ? `caught: ${this.state.error}`
      : this.props.children;
  }
}

/** Renders `element` into a fresh container. */
const render = (element: React.ReactElement) => {
  const container = document.createElement("div");
  const root = createRoot(container);
  act(() => root.render(element));
  return {
    container,
    rerender: (next: React.ReactElement) => root.render(next),
    unmount: () => act(() => root.unmount()),
  };
};

/** Runs `step` in act and lets microtasks and React work settle. */
const step = async (fn: () => unknown = () => {}) => {
  await act(async () => {
    fn();
    await new Promise<void>((resolve) => setImmediate(resolve));
  });
};

describe("convex/react hooks", () => {
  test("useQuery and useMutation with an optimistic update", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const r = impl.react;
      const client = new impl.ConvexReactClient(URL, clientOptions(server));
      let addNote!: (args: { text: string }) => Promise<unknown>;
      function Notes() {
        const notes = r.useQuery(list, {});
        addNote = r.useMutation(add).withOptimisticUpdate((store, args) => {
          store.setQuery(list, {}, [
            ...(store.getQuery(list, {}) ?? []),
            args.text,
          ]);
        });
        return <>{notes === undefined ? "loading" : notes.join(",")}</>;
      }
      const { container, unmount } = render(
        <r.ConvexProvider client={client}>
          <Notes />
        </r.ConvexProvider>,
      );
      log({ text: container.textContent });
      await step(() => server.open());
      log({ set: server.querySet() });
      await step(() =>
        server.transition([["notes:list", {}, { value: ["a"] }]]),
      );
      log({ text: container.textContent });
      let result: Promise<unknown> | undefined;
      await step(() => (result = addNote({ text: "b" })));
      log({ optimistic: container.textContent });
      await step(() => server.respond(0, { value: null }));
      await step(() =>
        server.transition([["notes:list", {}, { value: ["a", "B"] }]]),
      );
      await result;
      log({ reflected: container.textContent });
      unmount();
      await step();
      log({ set: server.querySet() });
      await client.close();
    });
  });

  test("a failed query throws into the error boundary", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const r = impl.react;
      const client = new impl.ConvexReactClient(URL, clientOptions(server));
      function Notes() {
        const notes = r.useQuery(list, { tag: "x" });
        return <>{notes?.join(",") ?? "loading"}</>;
      }
      const { container } = render(
        <r.ConvexProvider client={client}>
          <Boundary>
            <Notes />
          </Boundary>
        </r.ConvexProvider>,
      );
      await step(() => server.open());
      await step(() =>
        server.transition([["notes:list", { tag: "x" }, { error: "denied" }]]),
      );
      log({ text: container.textContent });
      await client.close();
    });
  });

  test("usePaginatedQuery loads more", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const r = impl.react;
      const client = new impl.ConvexReactClient(URL, clientOptions(server));
      let loadMore!: (n: number) => void;
      function Pages() {
        const result = r.usePaginatedQuery(page, {}, { initialNumItems: 2 });
        loadMore = result.loadMore;
        return <>{`${result.status}:${result.results.join(",")}`}</>;
      }
      const { container } = render(
        <r.ConvexProvider client={client}>
          <Pages />
        </r.ConvexProvider>,
      );
      await step(() => server.open());
      log({ text: container.textContent, set: server.querySet() });
      const [first] = server.querySet().map((query) => JSON.parse(query));
      await step(() =>
        server.transition([
          [
            "notes:page",
            first.args,
            { value: { page: [1, 2], isDone: false, continueCursor: "c" } },
          ],
        ]),
      );
      log({ text: container.textContent });
      await step(() => loadMore(1));
      log({ text: container.textContent, set: server.querySet() });
      const second = server
        .querySet()
        .map((query) => JSON.parse(query))
        .find((query) => query.args.paginationOpts.cursor === "c");
      await step(() =>
        server.transition([
          [
            "notes:page",
            second.args,
            { value: { page: [3], isDone: true, continueCursor: "e" } },
          ],
        ]),
      );
      log({ text: container.textContent });
      await client.close();
    });
  });

  test("ConvexProviderWithAuth and useConvexAuth", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const r = impl.react;
      const client = new impl.ConvexReactClient(URL, clientOptions(server));
      const token = [
        Buffer.from('{"alg":"none"}').toString("base64url"),
        Buffer.from('{"iat":1,"exp":3601}').toString("base64url"),
        "sig",
      ].join(".");
      let signedIn = true;
      const useAuth = () => ({
        isLoading: false,
        isAuthenticated: signedIn,
        fetchAccessToken: React.useCallback(async () => token, []),
      });
      function Status() {
        const { isLoading, isAuthenticated } = r.useConvexAuth();
        return <>{isLoading ? "loading" : String(isAuthenticated)}</>;
      }
      const app = () => (
        <r.ConvexProviderWithAuth client={client} useAuth={useAuth}>
          <Status />
        </r.ConvexProviderWithAuth>
      );
      const { container, rerender } = render(app());
      log({ text: container.textContent });
      await step(() => server.open());
      await step(() => server.transition([]));
      log({ text: container.textContent });
      await step(() => server.transition([]));
      log({ text: container.textContent });
      signedIn = false;
      await step(() => rerender(app()));
      log({ text: container.textContent });
      await client.close();
    });
  });
});

export type { Impl };
