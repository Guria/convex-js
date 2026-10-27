import type { AbortExt, Action, AtomLike, Ext } from "@reatom/core";

/**
 * Calls `connect` when `target` gains its first subscriber and `disconnect`
 * when it loses the last one. `target` must have no other connect hook.
 *
 * Sets Reatom's internal `onConnect` slot to plain functions: Reatom's queue
 * calls them in the context's root frame. Unlike `withConnectHook` this makes
 * no actions, AbortController or AbortError per connection, which matters
 * with thousands of queries.
 */
export const withConnectionHooks =
  <Target extends AtomLike>(
    connect: () => void,
    disconnect: () => void,
  ): Ext<Target> =>
  (target) => {
    target.__reatom.onConnect = Object.assign(() => connect(), {
      abort: () => disconnect(),
    }) as unknown as Action & AbortExt;
    return target;
  };
