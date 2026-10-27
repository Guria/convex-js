import {
  action,
  type AbortExt,
  type Action,
  type AtomLike,
  type Ext,
} from "@reatom/core";

/**
 * Calls `connect` when `target` gains its first subscriber and `disconnect`
 * when it loses the last one. Unlike `withConnectHook`, a connect makes no
 * AbortController and a disconnect throws no AbortError, which matters with
 * thousands of queries. `target` must have no other connect hook.
 *
 * Sets Reatom's internal `onConnect` slot.
 */
export const withConnectionHooks =
  <Target extends AtomLike>(
    connect: () => void,
    disconnect: () => void,
  ): Ext<Target> =>
  (target) => {
    const onConnect: Action & Partial<AbortExt> = action(
      connect,
      `${target.name}._connect`,
    );
    onConnect.abort = action(
      disconnect,
      `${target.name}._disconnect`,
    ) as unknown as AbortExt["abort"];
    target.__reatom.onConnect = onConnect as Action & AbortExt;
    return target;
  };
