// The sync client's only entry to @reatom/core. Module order matters: the
// guard must run before @reatom/core loads.
import { restoreBroadcastChannel } from "./reatom_node_guard.js";
export {
  action,
  atom,
  bind,
  computed,
  context,
  peek,
  wrap,
} from "@reatom/core";
export type { Action, Atom, Computed, Frame } from "@reatom/core";

restoreBroadcastChannel();
