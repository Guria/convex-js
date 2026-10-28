// @vitest-environment happy-dom
// @vitest-environment-options {"url": "https://app.example/notes"}
import { context, noop } from "@reatom/core";
import { beforeEach, expect, test, vi } from "vitest";
import { reatomAuthState } from "./auth_state.js";
import {
  reatomBetterAuthProvider,
  type BetterAuthClient,
} from "./better_auth.js";
import { reatomConvexClient } from "./client.js";
import {
  clientOptions,
  FakeServer,
  jwt,
  settle,
} from "./parity_test_helpers.js";

const URL = "https://happy-otter-123.convex.cloud";

type SessionState = ReturnType<BetterAuthClient["useSession"]["get"]>;

/** A better-auth client whose session and requests the test drives. */
const fakeClient = () => {
  let value: SessionState = { data: null, isPending: true };
  const listeners = new Set<(value: SessionState) => void>();
  const tokenRequests: Array<(token: string | null) => void> = [];
  const client = {
    useSession: {
      get: () => value,
      subscribe(listener: (value: SessionState) => void) {
        listeners.add(listener);
        listener(value);
        return () => listeners.delete(listener);
      },
    },
    convex: {
      token: () =>
        new Promise<{ data: { token: string | null } }>((resolve) =>
          tokenRequests.push((token) => resolve({ data: { token } })),
        ),
    },
    getSession: vi.fn(async () => null),
    crossDomain: {
      oneTimeToken: {
        verify: vi.fn(async ({ token }: { token: string }) => ({
          data: { session: { token: `session-for-${token}` } },
        })),
      },
    },
    updateSession: vi.fn(),
  } satisfies BetterAuthClient;
  return {
    client,
    tokenRequests,
    setSession(next: SessionState) {
      value = next;
      for (const listener of listeners) listener(next);
    },
  };
};

let trace: any[];
let server: FakeServer;

beforeEach(() => {
  trace = [];
  server = new FakeServer(trace);
  history.replaceState(null, "", "/notes");
});

test("follows the session store and caches the Convex token", async () => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  const fake = fakeClient();
  const provider = reatomBetterAuthProvider(fake.client);
  const auth = reatomAuthState(convex, provider);
  const unsubscribe = frame.run(() => auth.status.subscribe(noop));
  frame.run(() => expect(auth.status()).toBe("loading"));
  // Connect hooks run after the subscription.
  await settle();

  fake.setSession({ data: null, isPending: false });
  frame.run(() => expect(auth.status()).toBe("unauthenticated"));

  fake.setSession({ data: { session: { id: "s1" } }, isPending: false });
  await settle();
  const token = jwt({ sub: "user1" });
  fake.tokenRequests.shift()!(token);
  await settle();
  server.open();
  await settle();
  server.transition([]);
  await settle();
  // Once the server accepts the first token, the client fetches a fresh one.
  fake.tokenRequests.shift()!(jwt({ sub: "user1", n: 2 }));
  await settle();
  server.transition([]);
  await settle();
  frame.run(() => {
    expect(auth.status()).toBe("authenticated");
    expect(provider.token()).toBe(jwt({ sub: "user1", n: 2 }));
  });
  expect(
    await frame.run(() => provider.fetchToken({ forceRefreshToken: false })),
  ).toBe(jwt({ sub: "user1", n: 2 }));
  expect(fake.tokenRequests).toHaveLength(0);

  fake.setSession({ data: null, isPending: false });
  await settle();
  frame.run(() => {
    expect(auth.status()).toBe("unauthenticated");
    expect(provider.token()).toBeNull();
  });
  unsubscribe();
  await frame.run(() => convex.close());
});

test("shares one request among unforced fetches", async () => {
  const frame = context.start();
  const fake = fakeClient();
  const provider = reatomBetterAuthProvider(fake.client);
  const tokens = frame.run(() => [
    provider.fetchToken({ forceRefreshToken: false }),
    provider.fetchToken({ forceRefreshToken: false }),
  ]);
  expect(fake.tokenRequests).toHaveLength(1);
  fake.tokenRequests.shift()!("t1");
  expect(await Promise.all(tokens)).toEqual(["t1", "t1"]);
});

test("another session drops the cached token", async () => {
  const frame = context.start();
  const fake = fakeClient();
  const provider = reatomBetterAuthProvider(fake.client);
  frame.run(() => provider.session.subscribe(noop));
  await settle();
  fake.setSession({ data: { session: { id: "s1" } }, isPending: false });
  const first = frame.run(() =>
    provider.fetchToken({ forceRefreshToken: false }),
  );
  fake.tokenRequests.shift()!("alice");
  expect(await first).toBe("alice");

  fake.setSession({ data: { session: { id: "s2" } }, isPending: false });
  const second = frame.run(() =>
    provider.fetchToken({ forceRefreshToken: false }),
  );
  expect(fake.tokenRequests).toHaveLength(1);
  fake.tokenRequests.shift()!("bob");
  expect(await second).toBe("bob");
});

test("an initial token stands in until the session loads", async () => {
  const frame = context.start();
  const fake = fakeClient();
  const provider = reatomBetterAuthProvider(fake.client, {
    initialToken: "ssr",
  });
  frame.run(() => {
    provider.session.subscribe(noop);
    expect(provider.isLoading()).toBe(false);
    expect(provider.session()).toBe("initialToken");
  });
  expect(
    await frame.run(() => provider.fetchToken({ forceRefreshToken: false })),
  ).toBe("ssr");
  fake.setSession({ data: { session: { id: "s1" } }, isPending: false });
  frame.run(() => expect(provider.session()).toBe("s1"));
});

test("trades a one-time token from the URL for the session", async () => {
  history.replaceState(null, "", "/notes?ott=abc&tab=1");
  const frame = context.start();
  const fake = fakeClient();
  const replaceURL = vi.fn();
  const provider = reatomBetterAuthProvider(fake.client, { replaceURL });
  frame.run(() => provider.isLoading.subscribe(noop));
  frame.run(() => expect(provider.isLoading()).toBe(true));
  await settle();
  expect(replaceURL).toHaveBeenCalledWith(
    new globalThis.URL("https://app.example/notes?tab=1"),
  );
  expect(fake.client.crossDomain.oneTimeToken.verify).toHaveBeenCalledWith({
    token: "abc",
  });
  expect(fake.client.getSession).toHaveBeenCalledWith({
    fetchOptions: { headers: { Authorization: "Bearer session-for-abc" } },
  });
  expect(fake.client.updateSession).toHaveBeenCalled();
  fake.setSession({ data: { session: { id: "s1" } }, isPending: false });
  frame.run(() => expect(provider.isLoading()).toBe(false));
});

test("a refused one-time token lands in completeSignIn.error", async () => {
  const frame = context.start();
  const fake = fakeClient();
  fake.client.crossDomain.oneTimeToken.verify.mockResolvedValueOnce({
    data: null,
    error: { message: "Invalid token" },
  } as never);
  const provider = reatomBetterAuthProvider(fake.client);
  await expect(
    frame.run(() => provider.completeSignIn("used")),
  ).rejects.toThrow("Invalid token");
  frame.run(() =>
    expect(provider.completeSignIn.error()?.message).toBe("Invalid token"),
  );
});

test("data follows the session store", () => {
  const frame = context.start();
  const fake = fakeClient();
  const provider = reatomBetterAuthProvider(fake.client);
  fake.setSession({
    data: { session: { id: "s1" }, user: { email: "a@b.c" } } as never,
    isPending: false,
  });
  frame.run(() => {
    provider.data.subscribe(noop);
    expect(provider.data()).toMatchObject({ user: { email: "a@b.c" } });
  });
});
