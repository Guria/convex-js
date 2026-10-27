// The sync client's only entry to @reatom/core. Module order matters: the
// guard must run before @reatom/core loads.
import { restoreBroadcastChannel } from "./reatom_node_guard.js";
import { context, STACK, type Frame } from "@reatom/core";
export { action, atom, bind, computed, peek, wrap } from "@reatom/core";
export type { Action, Atom, Computed, Frame } from "@reatom/core";

restoreBroadcastChannel();

// The context @reatom/core starts with. An app may empty the stack with
// `clearStack()`; clients built outside any frame then fall back to this one.
const defaultRoot: Frame = STACK.length > 0 ? context() : context.start();

/** The root frame of the caller's context, or the default one. */
export function clientRoot(): Frame {
  return STACK.length > 0 ? context() : defaultRoot;
}
