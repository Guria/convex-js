import {
  FunctionArgs,
  FunctionReturnType,
  OptionalRestArgs,
  getFunctionName,
  FunctionReference,
  FunctionReference_future,
} from "../../server/api.js";
import { parseArgs } from "../../common/index.js";
import { Value } from "../../values/index.js";
import { createHybridErrorStacktrace, forwardData } from "../logging.js";
import { FunctionResult } from "./function_result.js";
import { LocalSyncState, QueryEntry } from "./local_state.js";
import { OptimisticLocalStore } from "./optimistic_updates.js";
import { RequestId } from "./protocol.js";
import { atom } from "./reatom.js";
import {
  canonicalizeUdfPath,
  QueryToken,
  serializePathAndArgs,
} from "./udf_path_utils.js";
import { ConvexError } from "../../values/errors.js";

/**
 * An optimistic update function that has been curried over its arguments.
 */
type WrappedOptimisticUpdate = (locaQueryStore: OptimisticLocalStore) => void;

type OptimisticUpdateAndId = {
  update: WrappedOptimisticUpdate;
  mutationId: RequestId;
};

/** Results set by optimistic updates, by query. */
type Overlay = Map<QueryToken, FunctionResult | undefined>;

// If the query is an error state, just return `undefined` as though
// it's loading. Optimistic updates should already handle `undefined` well
// and there isn't a need to break the whole update because it tried
// to load a single query that errored.
function queryValue(result: FunctionResult | undefined): Value | undefined {
  return result?.success ? result.value : undefined;
}

/**
 * The implementation of `OptimisticLocalStore`: reads `overlay` over `base`,
 * writes into `overlay` and records the queries it sets.
 */
class OptimisticLocalStoreImpl implements OptimisticLocalStore {
  readonly modifiedQueries: QueryToken[] = [];

  constructor(
    private readonly local: LocalSyncState,
    private readonly overlay: Overlay,
    /** The result of `entry` without the overlay, or `null` if it has none. */
    private readonly base: (
      entry: QueryEntry,
    ) => { result: FunctionResult | undefined } | null,
  ) {}

  private read(entry: QueryEntry) {
    return this.overlay.has(entry.token)
      ? { result: this.overlay.get(entry.token) }
      : this.base(entry);
  }

  getQuery<
    Query extends
      | FunctionReference<"query">
      | FunctionReference_future<"query">,
  >(
    query: Query,
    ...args: OptionalRestArgs<Query>
  ): undefined | FunctionReturnType<Query> {
    const queryArgs = parseArgs(args[0]);
    const entry = this.local.entries.get(
      serializePathAndArgs(getFunctionName(query), queryArgs),
    );
    const found = entry === undefined ? null : this.read(entry);
    return found === null ? undefined : queryValue(found.result);
  }

  getAllQueries<
    Query extends
      | FunctionReference<"query">
      | FunctionReference_future<"query">,
  >(
    query: Query,
  ): {
    args: FunctionArgs<Query>;
    value: undefined | FunctionReturnType<Query>;
  }[] {
    const queriesWithName: {
      args: FunctionArgs<Query>;
      value: undefined | FunctionReturnType<Query>;
    }[] = [];
    const udfPath = canonicalizeUdfPath(getFunctionName(query));
    for (const entry of this.local.entries.values()) {
      if (entry.udfPath !== udfPath) continue;
      const found = this.read(entry);
      if (found === null) continue;
      queriesWithName.push({
        args: entry.args as FunctionArgs<Query>,
        value: queryValue(found.result),
      });
    }
    return queriesWithName;
  }

  setQuery<
    QueryReference extends
      | FunctionReference<"query">
      | FunctionReference_future<"query">,
  >(
    queryReference: QueryReference,
    args: FunctionArgs<QueryReference>,
    value: undefined | FunctionReturnType<QueryReference>,
  ): void {
    const queryArgs = parseArgs(args);
    const entry = this.local.entry(getFunctionName(queryReference), queryArgs);
    this.overlay.set(
      entry.token,
      value === undefined
        ? undefined
        : // It's an optimistic update, so there are no function logs to show.
          { success: true, value, logLines: [] },
    );
    this.modifiedQueries.push(entry.token);
  }
}

/**
 * A view of all of our query results with optimistic updates applied on top.
 *
 * Without optimistic updates to replay, a server transition changes only the
 * queries it names, plus the ones that left the query set since the last
 * one. With optimistic updates pending, or on the first transition after a
 * reconnect, every result is recomputed and the updates are replayed.
 */
export function reatomOptimisticQueryResults({
  local,
  name,
}: {
  local: LocalSyncState;
  name: string;
}) {
  const optimisticUpdates = atom<readonly OptimisticUpdateAndId[]>(
    [],
    `${name}.optimisticUpdates`,
  );
  const overlay = atom<ReadonlyMap<QueryToken, FunctionResult | undefined>>(
    new Map(),
    `${name}.overlay`,
  );

  /** Shows `result`; records the token if it is part of a transition. */
  const settle = (
    entry: QueryEntry,
    result: FunctionResult | undefined,
    present: boolean,
    changed: QueryToken[],
  ) => {
    const old = entry.result();
    if (old !== result) entry.result.set(result);
    if (present && (!entry.present || old !== result))
      changed.push(entry.token);
    entry.present = present;
  };

  const serverOf = (entry: QueryEntry) =>
    entry.subscribers > 0 ? entry.serverResult : undefined;

  /** Drops entries nothing refers to anymore. */
  const collect = (
    entry: QueryEntry,
    current: ReadonlyMap<QueryToken, any>,
  ) => {
    if (
      entry.subscribers === 0 &&
      entry.id === null &&
      !current.has(entry.token)
    ) {
      local.entries.delete(entry.token);
    }
  };

  return {
    optimisticUpdates,
    overlay,

    /**
     * Shows the server results of `named` (or of every query when `full`)
     * under the optimistic updates still pending, and drops the updates of
     * `optimisticUpdatesToDrop`.
     *
     * @returns The tokens of queries whose result changed.
     */
    ingestQueryResultsFromServer(
      named: QueryEntry[],
      optimisticUpdatesToDrop: Set<RequestId>,
      forceFull: boolean,
    ): QueryToken[] {
      const updates = optimisticUpdates();
      const changed: QueryToken[] = [];
      if (!forceFull && updates.length === 0 && overlay().size === 0) {
        for (const entry of named) {
          const result = serverOf(entry);
          settle(entry, result, result !== undefined, changed);
        }
        for (const entry of local.retired) {
          const result = serverOf(entry);
          settle(entry, result, result !== undefined, changed);
          collect(entry, overlay());
        }
        local.retired.clear();
        return changed;
      }

      const remaining =
        optimisticUpdatesToDrop.size === 0
          ? updates
          : optimisticUpdates.set(
              updates.filter(
                (update) => !optimisticUpdatesToDrop.has(update.mutationId),
              ),
            );
      const next: Overlay = new Map();
      const store = new OptimisticLocalStoreImpl(local, next, (entry) => {
        const result = serverOf(entry);
        return result === undefined ? null : { result };
      });
      for (const { update } of remaining) {
        update(store);
      }
      overlay.set(next);
      local.retired.clear();
      for (const entry of local.entries.values()) {
        if (next.has(entry.token)) {
          settle(entry, next.get(entry.token), true, changed);
        } else {
          const result = serverOf(entry);
          settle(entry, result, result !== undefined, changed);
          collect(entry, next);
        }
      }
      return changed;
    },

    /** @returns The tokens of the queries the update set. */
    applyOptimisticUpdate(
      update: WrappedOptimisticUpdate,
      mutationId: RequestId,
    ): QueryToken[] {
      optimisticUpdates.set((updates) => [...updates, { update, mutationId }]);
      const next: Overlay = new Map(overlay());
      const store = new OptimisticLocalStoreImpl(local, next, (entry) =>
        entry.present ? { result: entry.result() } : null,
      );
      update(store);
      overlay.set(next);
      for (const token of store.modifiedQueries) {
        const entry = local.entries.get(token)!;
        const result = next.get(token);
        if (entry.result() !== result) entry.result.set(result);
        entry.present = true;
      }
      return store.modifiedQueries;
    },

    /**
     * "Raw" with respect to errors vs values, but query results still have
     * optimistic updates applied.
     */
    rawQueryResult(queryToken: QueryToken): FunctionResult | undefined {
      return local.entries.get(queryToken)?.result();
    },

    queryResult(queryToken: QueryToken): Value | undefined {
      const entry = local.entries.get(queryToken);
      if (entry === undefined) {
        return undefined;
      }
      const result = entry.result();
      if (result === undefined) {
        return undefined;
      } else if (result.success) {
        return result.value;
      } else {
        if (result.errorData !== undefined) {
          throw forwardData(
            result,
            new ConvexError(
              createHybridErrorStacktrace("query", entry.udfPath, result),
            ),
          );
        }
        throw new Error(
          createHybridErrorStacktrace("query", entry.udfPath, result),
        );
      }
    },

    hasQueryResult(queryToken: QueryToken): boolean {
      return local.entries.get(queryToken)?.present ?? false;
    },

    queryLogs(queryToken: QueryToken): string[] | undefined {
      return local.entries.get(queryToken)?.result()?.logLines;
    },
  };
}

export type OptimisticQueryResults = ReturnType<
  typeof reatomOptimisticQueryResults
>;
