import { atom, context, noop } from "@reatom/core";
import { beforeEach, expect, test } from "vitest";
import { reatomAuthState, type AuthProvider } from "./auth_state.js";
import { reatomConvexClient } from "./client.js";
import {
  clientOptions,
  FakeServer,
  jwt,
  settle,
} from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";

let trace: unknown[];
let server: FakeServer;

beforeEach(() => {
  trace = [];
  server = new FakeServer(trace);
});

/** `Authenticate` messages the server received: the token's `sub`, or none. */
const authenticated = () =>
  trace
    .filter((entry: any) => entry.received === "Authenticate")
    .map((entry: any) =>
      entry.value
        ? JSON.parse(
            Buffer.from(entry.value.split(".")[1], "base64url").toString(),
          ).sub
        : entry.tokenType,
    );

const fakeProvider = (name: string) => {
  const isLoading = atom(false, `${name}.isLoading`);
  const session = atom<string | null>(null, `${name}.session`);
  const provider: AuthProvider = {
    isLoading,
    session,
    fetchToken: async () => {
      const sub = session();
      return sub === null ? null : jwt({ sub });
    },
  };
  return { provider, isLoading, session };
};

test("follows the provider's session and the server's answer", async () => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  const { provider, isLoading, session } = fakeProvider("lib");
  frame.run(() => isLoading.set(true));
  const auth = reatomAuthState(convex, provider);
  const statuses: string[] = [];
  const unsubscribe = frame.run(() =>
    auth.status.subscribe((status) => statuses.push(status)),
  );

  frame.run(() => isLoading.set(false));
  await settle();
  // Signed out from the start: nothing to clear, so nothing connects.
  expect(server.sockets).toHaveLength(0);

  frame.run(() => session.set("alice"));
  await settle();
  server.open();
  await settle();
  frame.run(() => expect(auth.isLoading()).toBe(true));
  server.transition([]);
  await settle();
  frame.run(() => expect(auth.isAuthenticated()).toBe(true));

  frame.run(() => session.set("bob"));
  await settle();
  frame.run(() => expect(auth.status()).toBe("loading"));
  server.transition([]);
  await settle();

  frame.run(() => session.set(null));
  await settle();
  expect(statuses).toEqual([
    "loading",
    "unauthenticated",
    "loading",
    "authenticated",
    "loading",
    "authenticated",
    "unauthenticated",
  ]);
  expect(authenticated()).toEqual(["alice", "bob", "None"]);
  unsubscribe();
  await frame.run(() => convex.close());
});

test("a token the server refuses leaves the session rejected", async () => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  const session = atom<string | null>("alice", "session");
  let fetches = 0;
  const auth = reatomAuthState(convex, {
    isLoading: atom(false, "isLoading"),
    session,
    // The library has nothing better than the token the server refused.
    fetchToken: async () => (fetches++ === 0 ? jwt({ sub: "alice" }) : null),
  });
  const unsubscribe = frame.run(() => auth.status.subscribe(noop));
  await settle();
  server.open();
  await settle();
  server.authError("issuer not configured", { authUpdateAttempted: true });
  await settle();
  frame.run(() => {
    expect(auth.status()).toBe("rejected");
    expect(auth.isLoading()).toBe(false);
    expect(auth.isAuthenticated()).toBe(false);
  });
  // The socket restarted without auth.
  server.open();
  unsubscribe();
  await frame.run(() => convex.close());
});

test("an atom of providers switches libraries", async () => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  const first = fakeProvider("first");
  const second = fakeProvider("second");
  frame.run(() => {
    first.session.set("alice");
    second.session.set("alice");
  });
  const active = atom(first.provider, "active");
  const auth = reatomAuthState(convex, active);
  const unsubscribe = frame.run(() => auth.status.subscribe(noop));
  await settle();
  server.open();
  server.transition([]);
  await settle();
  frame.run(() => expect(auth.status()).toBe("authenticated"));

  // The same session id from another library is another session.
  frame.run(() => active.set(second.provider));
  frame.run(() => expect(auth.status()).toBe("loading"));
  await settle();
  server.transition([]);
  await settle();
  frame.run(() => expect(auth.status()).toBe("authenticated"));
  expect(authenticated()).toEqual(["alice", "alice"]);
  unsubscribe();
  await frame.run(() => convex.close());
});

test("isRefreshing is set while a refused token is replaced", async () => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  let release!: (token: string) => void;
  let fetches = 0;
  const auth = reatomAuthState(convex, {
    isLoading: atom(false, "isLoading"),
    session: atom<string | null>("alice", "session"),
    fetchToken: ({ forceRefreshToken }) =>
      forceRefreshToken && fetches++ > 0
        ? new Promise<string>((resolve) => (release = resolve))
        : Promise.resolve(jwt({ sub: "alice", n: fetches })),
  });
  const unsubscribe = frame.run(() => auth.isRefreshing.subscribe(noop));
  await settle();
  server.open();
  await settle();
  server.transition([]);
  await settle();
  // The fresh token from the initial refetch.
  server.transition([]);
  await settle();
  frame.run(() => expect(auth.status()).toBe("authenticated"));

  server.authError("expired", { authUpdateAttempted: false });
  await settle();
  frame.run(() => expect(auth.isRefreshing()).toBe(true));
  release(jwt({ sub: "alice", n: 2 }));
  await settle();
  server.sockets.length > 1 && server.open();
  await settle();
  server.transition([]);
  await settle();
  frame.run(() => {
    expect(auth.isRefreshing()).toBe(false);
    expect(auth.status()).toBe("authenticated");
  });
  unsubscribe();
  await frame.run(() => convex.close());
});
