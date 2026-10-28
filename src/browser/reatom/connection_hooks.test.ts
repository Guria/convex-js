import { atom, context, noop, withConnectHook } from "@reatom/core";
import { expect, test, vi } from "vitest";
import { makeFunctionReference } from "convex/server";
import { reatomConvexClient } from "./client.js";
import { withConnectionHooks } from "./connection_hooks.js";
import { clientOptions, settle, FakeServer } from "./parity_test_helpers.js";

test.each([false, true])(
  "connection hooks compose with existing hook=%s",
  async (existing) => {
    const frame = context.start();
    const connect = vi.fn();
    const disconnect = vi.fn();
    const otherConnect = vi.fn();
    const otherDisconnect = vi.fn();
    const target = atom(0, "connectionHooksTest");
    const other = withConnectHook(() => {
      otherConnect();
      return otherDisconnect;
    });
    if (existing) target.extend(other);
    target.extend(withConnectionHooks(connect, disconnect));
    if (!existing) target.extend(other);
    for (let cycle = 1; cycle <= 2; cycle++) {
      const stop = frame.run(() => target.subscribe(noop));
      await settle();
      expect(connect).toHaveBeenCalledTimes(cycle);
      expect(otherConnect).toHaveBeenCalledTimes(cycle);
      stop();
      await settle();
      expect(disconnect).toHaveBeenCalledTimes(cycle);
      expect(otherDisconnect).toHaveBeenCalledTimes(cycle);
    }
  },
);

test("public connection and query atoms accept connect hooks", () => {
  const frame = context.start();
  const convex = reatomConvexClient(
    "https://happy-otter-123.convex.cloud",
    clientOptions(new FakeServer([])),
  );
  const ref = makeFunctionReference<"query">("notes:list");
  frame.run(() => {
    expect(() =>
      convex.connectionState.extend(withConnectHook(noop)),
    ).not.toThrow();
    expect(() =>
      convex.query(ref, {}).result.extend(withConnectHook(noop)),
    ).not.toThrow();
  });
});
