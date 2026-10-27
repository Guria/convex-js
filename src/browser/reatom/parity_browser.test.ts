// @vitest-environment happy-dom
import { describe, test, vi } from "vitest";
import {
  caught,
  clientOptions,
  expectParity,
  FakeServer,
} from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";

const options = (server: FakeServer) => {
  const { unsavedChangesWarning: _, ...rest } = clientOptions(server);
  return rest;
};

const beforeUnload = () => {
  const event = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(event);
  return { prevented: event.defaultPrevented };
};

describe("in a browser", () => {
  test("warns on unload while a mutation is unconfirmed", async () => {
    await expectParity(async ({ impl, server, log, settle }) => {
      const client = new impl.BaseConvexClient(URL, () => {}, options(server));
      server.open();
      log({ idle: beforeUnload() });
      const mutation = caught(client.mutation("notes:add", {}));
      log({ inFlight: beforeUnload() });
      server.respond(0, { value: 1 });
      log({ completedNotReflected: beforeUnload() });
      server.transition([]);
      log(await mutation);
      await settle();
      log({ reflected: beforeUnload() });
      await client.close();
    });
  });

  test("reconnects at once when the network comes back", async () => {
    await expectParity(async ({ impl, server, log, checkpoint, settle }) => {
      const client = new impl.BaseConvexClient(URL, () => {}, {
        ...options(server),
        unsavedChangesWarning: false,
      });
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.drop(1006);
      await settle();
      await vi.advanceTimersByTimeAsync(400);
      window.dispatchEvent(new Event("online"));
      log({ state: client.connectionState() });
      server.open();
      await checkpoint("recovered");
      // A second online event while connected does nothing.
      window.dispatchEvent(new Event("online"));
      await settle();
      log({ sockets: server.sockets.length });
      await client.close();
      window.dispatchEvent(new Event("online"));
      log({ afterClose: server.sockets.length });
    });
  });

  test("reports debug events when asked to", async () => {
    await expectParity(async ({ impl, server, log, checkpoint }) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response("{}"));
      const client = new impl.BaseConvexClient(URL, () => {}, {
        ...options(server),
        unsavedChangesWarning: false,
        reportDebugInfoToConvex: true,
      });
      server.open();
      client.subscribe("notes:list", {});
      await checkpoint("subscribed");
      server.transition([["notes:list", {}, { value: 1 }]], {
        clientClockSkew: 0,
        serverTs: 0,
      });
      void client.mutation("notes:add", {}).catch(() => {});
      for (let second = 0; second < 61; second += 10) {
        server.push({ type: "Ping" });
        await vi.advanceTimersByTimeAsync(10_000);
      }
      void client.mutation("notes:add", {}).catch(() => {});
      log({ debugEvents: fetch.mock.calls.map(([url]) => String(url)) });
      fetch.mockRestore();
      await client.close();
    });
  });
});
