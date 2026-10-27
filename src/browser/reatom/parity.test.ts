import { describe, test, vi } from "vitest";
import { makeFunctionReference } from "../../server/api.js";
import type { BaseConvexClient, OptimisticLocalStore } from "../index.js";
import {
  attempt,
  caught,
  clientOptions,
  expectParity,
} from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";
const list = makeFunctionReference<"query">("notes:list");
const get = makeFunctionReference<"query">("notes:get");
const add = makeFunctionReference<"mutation">("notes:add");
const send = makeFunctionReference<"action">("notes:send");

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

describe("ConvexClient", () => {
  test("onUpdate, query and getCurrentValue", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const client = new impl.ConvexClient(URL, clientOptions(server));
      server.open();
      const unsubscribe = client.onUpdate(
        list,
        {},
        (value) => log({ update: value }),
        (error) => log({ onError: error.message }),
      );
      await checkpoint("subscribed");
      server.transition([["notes:list", {}, { value: ["a"] }]]);
      log({ current: unsubscribe.getCurrentValue() });
      // A second listener gets the cached value on the next tick.
      const late = client.onUpdate(
        list,
        {},
        (value) => log({ late: value }),
        (error) => log({ lateError: error.message }),
      );
      await vi.advanceTimersByTimeAsync(0);
      const once = client.query(get, { id: 3 });
      await checkpoint("one-shot query");
      server.transition([
        ["notes:get", { id: 3 }, { value: "three" }],
        ["notes:list", {}, { error: "gone" }],
      ]);
      log({ once: await once });
      unsubscribe();
      late();
      await checkpoint("unsubscribed");
      await client.close();
    });
  });

  test("mutation and action helpers", async () => {
    await expectParity(async ({ impl, server, log }) => {
      const client = new impl.ConvexClient(URL, clientOptions(server));
      server.open();
      const mutation = caught(client.mutation(add, { text: "x" }));
      const action = caught(client.action(send, {}));
      server.respond(0, { value: 1 });
      server.respond(1, { value: 2 });
      server.transition([]);
      log(await mutation, await action);
      await client.close();
    });
  });
});
