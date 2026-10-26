import { jsonToConvex } from "../../values/index.js";
import { Long } from "../../vendor/long.js";
import { logForFunction, Logger } from "../logging.js";
import { LocalSyncState, QueryEntry } from "./local_state.js";
import { StateVersion, Transition } from "./protocol.js";
import { atom } from "./reatom.js";

const initialVersion = (): StateVersion => ({
  querySet: 0,
  ts: Long.fromNumber(0),
  identity: 0,
});

/**
 * The query results received on the current WebSocket connection, kept in
 * each entry's `serverResult`.
 *
 * Results for queries already unsubscribed locally are dropped.
 */
export function reatomRemoteQuerySet({
  local,
  logger,
  name,
}: {
  local: LocalSyncState;
  logger: Logger;
  name: string;
}) {
  const version = atom<StateVersion>(initialVersion(), `${name}.version`);

  return {
    version,

    /** Applies a transition; returns the subscribed entries it named. */
    transition(transition: Transition): QueryEntry[] {
      const start = transition.startVersion;
      const current = version();
      if (
        current.querySet !== start.querySet ||
        current.ts.notEquals(start.ts) ||
        current.identity !== start.identity
      ) {
        throw new Error(
          `Invalid start version: ${start.ts.toString()}:${start.querySet}:${start.identity}, transitioning from ${current.ts.toString()}:${current.querySet}:${current.identity}`,
        );
      }
      const changed: QueryEntry[] = [];
      for (const modification of transition.modifications) {
        const entry = local.byId.get(modification.queryId);
        switch (modification.type) {
          case "QueryUpdated": {
            if (entry === undefined) break;
            for (const line of modification.logLines) {
              logForFunction(logger, "info", "query", entry.udfPath, line);
            }
            entry.serverResult = {
              success: true,
              value: jsonToConvex(modification.value ?? null),
              logLines: modification.logLines,
            };
            changed.push(entry);
            break;
          }
          case "QueryFailed": {
            if (entry === undefined) break;
            for (const line of modification.logLines) {
              logForFunction(logger, "info", "query", entry.udfPath, line);
            }
            const { errorData } = modification;
            entry.serverResult = {
              success: false,
              errorMessage: modification.errorMessage,
              errorData:
                errorData !== undefined ? jsonToConvex(errorData) : undefined,
              logLines: modification.logLines,
            };
            changed.push(entry);
            break;
          }
          case "QueryRemoved": {
            if (entry === undefined) break;
            entry.serverResult = undefined;
            changed.push(entry);
            break;
          }
          default: {
            // Enforce that the switch-case is exhaustive.
            modification satisfies never;
            throw new Error(
              `Invalid modification ${(modification as any).type}`,
            );
          }
        }
      }
      version.set(transition.endVersion);
      return changed;
    },

    /** A new connection starts with no results. */
    reset() {
      version.set(initialVersion());
      for (const entry of local.byId.values()) {
        entry.serverResult = undefined;
      }
    },

    timestamp(): Long {
      return version().ts;
    },
  };
}

export type RemoteQuerySet = ReturnType<typeof reatomRemoteQuerySet>;
