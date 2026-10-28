// @vitest-environment happy-dom
// @vitest-environment-options {"url": "https://app.example/notes"}
import { context, noop } from "@reatom/core";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { reatomAuthState } from "./auth_state.js";
import { reatomConvexClient } from "./client.js";
import { reatomConvexAuthProvider } from "./convex_auth.js";
import {
  clientOptions,
  FakeServer,
  jwt,
  settle,
} from "./parity_test_helpers.js";

// Refresh token exchanges, over HTTP.
const http = vi.hoisted(() => ({
  calls: [] as unknown[],
  answer: (_args: any): Promise<unknown> => Promise.resolve({ tokens: null }),
}));
let issued = 0;
/** A fresh pair for `user1|session1`, as `auth:signIn` issues for a refresh token. */
const freshTokens = async () => {
  issued++;
  return {
    tokens: {
      token: jwt({ sub: "user1|session1", n: issued }),
      refreshToken: `r${issued + 1}`,
    },
  };
};
vi.mock("convex/browser", () => ({
  ConvexHttpClient: class {
    action(_ref: unknown, args: unknown) {
      http.calls.push(args);
      return http.answer(args);
    }
  },
}));

const URL = "https://happy-otter-123.convex.cloud";
const JWT_KEY = "__convexAuthJWT_httpshappyotter123convexcloud";
const REFRESH_KEY = "__convexAuthRefreshToken_httpshappyotter123convexcloud";
const VERIFIER_KEY = "__convexAuthOAuthVerifier_httpshappyotter123convexcloud";

let trace: any[];
let server: FakeServer;
let opened: WeakSet<object>;

const open = () => {
  server.open();
  opened.add(server.socket);
};

beforeEach(() => {
  trace = [];
  server = new FakeServer(trace);
  opened = new WeakSet();
  http.calls = [];
  http.answer = freshTokens;
  issued = 0;
  localStorage.clear();
  history.replaceState(null, "", "/notes");
});

afterEach(() => {
  vi.useRealTimers();
});

/** The last action the server received. */
const lastAction = () =>
  trace.filter((entry) => entry.received === "Action").at(-1);

const setup = (
  options: Parameters<typeof reatomConvexAuthProvider>[1] = {},
) => {
  const frame = context.start();
  const convex = reatomConvexClient(URL, clientOptions(server));
  const provider = reatomConvexAuthProvider(convex, options);
  const auth = reatomAuthState(convex, provider);
  const unsubscribe = frame.run(() => auth.status.subscribe(noop));
  return {
    frame,
    convex,
    provider,
    auth,
    close: async () => {
      // Closing waits for a connecting socket to open.
      if (server.sockets.length > 0 && !opened.has(server.socket))
        server.open();
      unsubscribe();
      await frame.run(() => convex.close());
    },
  };
};

test("restores the stored session under @convex-dev/auth's keys", async () => {
  localStorage.setItem(JWT_KEY, jwt({ sub: "user1|session1" }));
  localStorage.setItem(REFRESH_KEY, "r1");
  const { frame, auth, provider, close } = setup();
  await settle();
  frame.run(() => expect(provider.session()).toBe("user1|session1"));
  open();
  await settle();
  server.transition([]);
  await settle();
  // Once the server accepts the stored token, the client trades it for a fresh one.
  expect(http.calls).toEqual([{ refreshToken: "r1" }]);
  server.transition([]);
  await settle();
  frame.run(() => {
    expect(auth.status()).toBe("authenticated");
    expect(provider.token()).toBe(jwt({ sub: "user1|session1", n: 1 }));
  });
  await close();
});

test("signs in with a password and stores the tokens", async () => {
  const { frame, provider, auth, close } = setup();
  await settle();
  frame.run(() => expect(auth.status()).toBe("unauthenticated"));
  const signIn = frame.run(() =>
    provider.signIn("password", { email: "a@b.c", flow: "signIn" }),
  );
  await settle();
  open();
  await settle();
  expect(lastAction()).toMatchObject({
    udfPath: "auth:signIn",
    args: [
      { provider: "password", params: { email: "a@b.c", flow: "signIn" } },
    ],
  });
  const token = jwt({ sub: "user1|session1" });
  server.respond(lastAction().requestId, {
    value: { tokens: { token, refreshToken: "r1" } },
  });
  expect(await signIn).toEqual({ signingIn: true });
  expect(localStorage.getItem(JWT_KEY)).toBe(token);
  expect(localStorage.getItem(REFRESH_KEY)).toBe("r1");
  await settle();
  server.transition([]);
  await settle();
  server.transition([]);
  await settle();
  frame.run(() => expect(auth.status()).toBe("authenticated"));
  await close();
});

test("OAuth: redirects with a verifier, then signs in with the code", async () => {
  const navigate = vi.fn();
  const first = setup({ navigate });
  const started = first.frame.run(() => first.provider.signIn("github"));
  await settle();
  open();
  await settle();
  server.respond(lastAction().requestId, {
    value: { redirect: "https://github.com/login", verifier: "v1" },
  });
  expect(await started).toEqual({
    signingIn: false,
    redirect: new globalThis.URL("https://github.com/login"),
  });
  expect(navigate).toHaveBeenCalledWith(
    new globalThis.URL("https://github.com/login"),
  );
  expect(localStorage.getItem(VERIFIER_KEY)).toBe("v1");
  await first.close();

  // The backend sends the browser back with a code.
  server = new FakeServer((trace = []));
  history.replaceState(null, "", "/notes?code=c1#top");
  const replaceURL = vi.fn();
  const second = setup({ replaceURL });
  await settle();
  expect(replaceURL).toHaveBeenCalledWith("/notes#top");
  second.frame.run(() => expect(second.auth.status()).toBe("loading"));
  open();
  await settle();
  expect(lastAction().args).toEqual([
    { params: { code: "c1" }, verifier: "v1" },
  ]);
  expect(localStorage.getItem(VERIFIER_KEY)).toBeNull();
  const token = jwt({ sub: "user1|session2" });
  server.respond(lastAction().requestId, {
    value: { tokens: { token, refreshToken: "r2" } },
  });
  await settle();
  second.frame.run(() => expect(second.provider.token()).toBe(token));
  await second.close();
});

test("refreshes once for concurrent forced fetches", async () => {
  localStorage.setItem(JWT_KEY, jwt({ sub: "user1|session1" }));
  localStorage.setItem(REFRESH_KEY, "r1");
  const { frame, provider, close } = setup();
  await settle();
  const fresh = jwt({ sub: "user1|session1", n: 1 });
  const tokens = await Promise.all(
    frame.run(() => [
      provider.fetchToken({ forceRefreshToken: true }),
      provider.fetchToken({ forceRefreshToken: true }),
    ]),
  );
  expect(tokens).toEqual([fresh, fresh]);
  expect(http.calls).toEqual([{ refreshToken: "r1" }]);
  expect(localStorage.getItem(REFRESH_KEY)).toBe("r2");
  await close();
});

test("retries an exchange that hits a network error", async () => {
  localStorage.setItem(JWT_KEY, jwt({ sub: "user1|session1" }));
  localStorage.setItem(REFRESH_KEY, "r1");
  const { frame, provider, close } = setup();
  await settle();
  vi.useFakeTimers({ toFake: ["setTimeout"] });
  const fresh = jwt({ sub: "user1|session1", n: 2 });
  let attempts = 0;
  http.answer = async () => {
    if (attempts++ === 0) throw new TypeError("Failed to fetch");
    return { tokens: { token: fresh, refreshToken: "r2" } };
  };
  const token = frame.run(() =>
    provider.fetchToken({ forceRefreshToken: true }),
  );
  await vi.advanceTimersByTimeAsync(1000);
  expect(await token).toBe(fresh);
  expect(attempts).toBe(2);
  vi.useRealTimers();
  await close();
});

test("follows another tab's token and sign-out", async () => {
  localStorage.setItem(JWT_KEY, jwt({ sub: "user1|session1" }));
  const { frame, provider, auth, close } = setup();
  await settle();
  const other = jwt({ sub: "user2|session3" });
  window.dispatchEvent(
    new StorageEvent("storage", {
      key: JWT_KEY,
      newValue: other,
      storageArea: localStorage,
    }),
  );
  frame.run(() => expect(provider.session()).toBe("user2|session3"));
  window.dispatchEvent(
    new StorageEvent("storage", {
      key: JWT_KEY,
      newValue: null,
      storageArea: localStorage,
    }),
  );
  await settle();
  frame.run(() => expect(auth.status()).toBe("unauthenticated"));
  await close();
});

test("signs out locally when auth:signOut fails", async () => {
  localStorage.setItem(JWT_KEY, jwt({ sub: "user1|session1" }));
  localStorage.setItem(REFRESH_KEY, "r1");
  const { frame, provider, auth, close } = setup();
  await settle();
  open();
  await settle();
  const signOut = frame.run(() => provider.signOut());
  await settle();
  expect(lastAction().udfPath).toBe("auth:signOut");
  server.respond(lastAction().requestId, { error: "no session" });
  await signOut;
  expect(localStorage.getItem(JWT_KEY)).toBeNull();
  expect(localStorage.getItem(REFRESH_KEY)).toBeNull();
  await settle();
  frame.run(() => expect(auth.status()).toBe("unauthenticated"));
  await close();
});

test("a failed code exchange lands in completeSignIn.error", async () => {
  history.replaceState(null, "", "/notes?code=stale");
  const { frame, provider, auth, close } = setup({ replaceURL: vi.fn() });
  await settle();
  open();
  await settle();
  frame.run(() => expect(auth.status()).toBe("loading"));
  server.respond(lastAction().requestId, {
    error: "Invalid verification code",
  });
  await settle();
  frame.run(() => {
    expect(provider.completeSignIn.error()?.message).toContain(
      "Invalid verification code",
    );
    expect(auth.status()).toBe("unauthenticated");
  });
  await close();
});
