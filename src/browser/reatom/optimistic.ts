/**
 * Optimistic updates over server results. The overlay holds only the results
 * updates have set; everything else reads through to the server results of
 * subscribed queries.
 */
import {
  FunctionArgs,
  FunctionReference,
  FunctionReference_future,
  FunctionReturnType,
  getFunctionName,
  OptionalRestArgs,
} from "../../server/api.js";
import { parseArgs } from "../../common/index.js";
import { Value } from "../../values/index.js";
import { FunctionResult } from "../sync/function_result.js";
import { OptimisticLocalStore } from "../sync/optimistic_updates.js";
import { RequestId } from "../sync/protocol.js";
import {
  canonicalizeUdfPath,
  QueryToken,
  serializePathAndArgs,
} from "../sync/udf_path_utils.js";

export type OverlayQuery = {
  udfPath: string;
  args: Record<string, Value>;
  /** `undefined` is a query an update set to loading. */
  result: FunctionResult | undefined;
};

export type Overlay = Map<QueryToken, OverlayQuery>;

/** Subscribed queries with a server result, in subscription order. */
export type ServerResults = {
  get(token: QueryToken): FunctionResult | undefined;
  all(): Iterable<OverlayQuery & { token: QueryToken }>;
};

/** An optimistic update curried over its mutation's arguments. */
export type WrappedOptimisticUpdate = (store: OptimisticLocalStore) => void;

export type OptimisticUpdateAndId = {
  update: WrappedOptimisticUpdate;
  mutationId: RequestId;
};

// An errored query reads as loading, so one failed query doesn't break an
// update that reads it.
function queryValue(result: FunctionResult | undefined): Value | undefined {
  if (result === undefined || !result.success) return undefined;
  return result.value;
}

/** A store writing into `overlay`; `modifiedQueries` collects the tokens it sets. */
export class OptimisticLocalStoreImpl implements OptimisticLocalStore {
  readonly modifiedQueries: QueryToken[] = [];

  constructor(
    private readonly overlay: Overlay,
    private readonly server: ServerResults,
  ) {}

  getQuery<
    Query extends
      | FunctionReference<"query">
      | FunctionReference_future<"query">,
  >(
    query: Query,
    ...args: OptionalRestArgs<Query>
  ): undefined | FunctionReturnType<Query> {
    const token = serializePathAndArgs(
      getFunctionName(query),
      parseArgs(args[0]),
    );
    const local = this.overlay.get(token);
    return queryValue(local ? local.result : this.server.get(token));
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
    const udfPath = canonicalizeUdfPath(getFunctionName(query));
    const queries: {
      args: FunctionArgs<Query>;
      value: undefined | FunctionReturnType<Query>;
    }[] = [];
    // Server results keep their position when an update overrides them;
    // queries only updates know about come after.
    const seen = new Set<QueryToken>();
    for (const subscribed of this.server.all()) {
      seen.add(subscribed.token);
      if (subscribed.udfPath !== udfPath) continue;
      const local = this.overlay.get(subscribed.token);
      queries.push({
        args: subscribed.args as FunctionArgs<Query>,
        value: queryValue(local ? local.result : subscribed.result),
      });
    }
    for (const [token, local] of this.overlay) {
      if (seen.has(token) || local.udfPath !== udfPath) continue;
      queries.push({
        args: local.args as FunctionArgs<Query>,
        value: queryValue(local.result),
      });
    }
    return queries;
  }

  setQuery<
    Query extends
      | FunctionReference<"query">
      | FunctionReference_future<"query">,
  >(
    queryReference: Query,
    args: FunctionArgs<Query>,
    value: undefined | FunctionReturnType<Query>,
  ): void {
    const queryArgs = parseArgs(args);
    const name = getFunctionName(queryReference);
    const token = serializePathAndArgs(name, queryArgs);
    this.overlay.set(token, {
      udfPath: canonicalizeUdfPath(name),
      args: queryArgs,
      // An optimistic update has no function logs to show.
      result:
        value === undefined
          ? undefined
          : { success: true, value, logLines: [] },
    });
    this.modifiedQueries.push(token);
  }
}
