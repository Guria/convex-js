import {
  abortVar,
  withConnectHook,
  type AtomLike,
  type Ext,
} from "@reatom/core";

/**
 * Composes with other public Reatom connection hooks. Client work belongs to
 * the context, not to a subscriber's abort scope; losing a subscription must
 * not cancel a socket operation or a scheduled query-set flush.
 */
export const withConnectionHooks = <Target extends AtomLike>(
  connect: () => void,
  disconnect: () => void,
): Ext<Target> =>
  withConnectHook(() => {
    abortVar.spawn(connect);
    return disconnect;
  });
