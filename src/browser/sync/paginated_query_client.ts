/**
 * PaginatedQueryClient maps subscriptions to paginated queries to the
 * individual page queries and handles page splits.
 *
 * In order to process all modified queries, paginated and normal, in the same
 * synchronous call the PaginatedQueryClient transition should be used exclusively.
 *
 * Like the BaseConvexClient, this client is not Convex Function type-aware: it deals
 * with queries as functions that return Value, not the specific value.
 * Use a higher-level library to get types.
 *
 * The page lists are the paginated queries of the base client's
 * {@link ReatomConvexClient}.
 */
import { noop, type Frame } from "@reatom/core";
import { Value } from "../../values/index.js";
import { Long } from "../../vendor/long.js";
import { ReatomConvexClient } from "../reatom/client.js";
import { ReatomPaginatedQuery } from "../reatom/paginated.js";
import { BaseConvexClientInterface, Transition } from "./client.js";
import { PaginatedQueryResult } from "./pagination.js";
import { TS } from "./protocol.js";
import {
  canonicalizeUdfPath,
  PaginatedQueryToken,
  serializePaginatedPathAndArgs,
} from "./udf_path_utils.js";

export interface SubscribeToPaginatedQueryOptions {
  initialNumItems: number;
  id: number;
}

type AnyPaginatedQueryResult = PaginatedQueryResult<Value>;

export type PaginatedQueryModification =
  | { kind: "Updated"; result: AnyPaginatedQueryResult | undefined }
  | { kind: "Removed" };

export type ExtendedTransition = Transition & {
  paginatedQueries: Array<{
    token: PaginatedQueryToken;
    modification: PaginatedQueryModification;
  }>;
};

type Subscription = {
  query: ReatomPaginatedQuery;
  numSubscribers: number;
  unsubscribe: () => void;
};

type ReatomBacked = { reatom: ReatomConvexClient; reatomFrame: Frame };

const isReatomBacked = (
  client: BaseConvexClientInterface,
): client is BaseConvexClientInterface & ReatomBacked =>
  "reatom" in client && "reatomFrame" in client;

/** @internal */
export class PaginatedQueryClient {
  private subscriptions = new Map<PaginatedQueryToken, Subscription>();
  // Synthetic load-more transitions carry this timestamp.
  private lastTransitionTs: TS;

  constructor(
    private client: BaseConvexClientInterface,
    private onTransition: (transition: ExtendedTransition) => void,
  ) {
    this.lastTransitionTs = Long.fromNumber(0);
    this.client.addOnTransitionHandler((transition: Transition) =>
      this.onBaseTransition(transition),
    );
  }

  private get backing(): ReatomBacked {
    if (!isReatomBacked(this.client)) {
      throw new Error(
        "Paginated queries need a BaseConvexClient from convex/browser",
      );
    }
    return this.client;
  }

  private run<T>(fn: () => T): T {
    return this.backing.reatomFrame.run(fn);
  }

  /**
   * Subscribe to a paginated query.
   *
   * @param name - The name of the paginated query function
   * @param args - Arguments for the query (excluding paginationOpts)
   * @param options - Pagination options including initialNumItems
   * @returns Object with paginatedQueryToken and unsubscribe function
   */
  subscribe(
    name: string,
    args: Record<string, Value>,
    options: SubscribeToPaginatedQueryOptions,
  ): {
    paginatedQueryToken: PaginatedQueryToken;
    unsubscribe: () => void;
  } {
    const { reatom } = this.backing;
    // Only the expected options are part of the token.
    const { initialNumItems, id } = options;
    const query = this.run(() =>
      reatom.paginatedQueryEntry(name, args, { initialNumItems, id }),
    );
    const token = query.token;
    const existing = this.subscriptions.get(token);
    if (existing) {
      existing.numSubscribers += 1;
    } else {
      this.subscriptions.set(token, {
        query,
        numSubscribers: 1,
        unsubscribe: this.run(() => query.state.subscribe(noop)),
      });
    }
    return {
      paginatedQueryToken: token,
      unsubscribe: () => this.removePaginatedQuerySubscriber(token),
    };
  }

  /**
   * Get current results for a paginated query based on local state.
   *
   * Throws an error when one of the pages has errored.
   */
  localQueryResult(
    name: string,
    args: Record<string, Value>,
    options: { initialNumItems: number; id: number },
  ): AnyPaginatedQueryResult | undefined {
    const token = serializePaginatedPathAndArgs(
      canonicalizeUdfPath(name),
      args,
      options,
    );
    return this.localQueryResultByToken(token);
  }

  /**
   * @internal
   */
  localQueryResultByToken(
    token: PaginatedQueryToken,
  ): AnyPaginatedQueryResult | undefined {
    const subscription = this.subscriptions.get(token);
    if (!subscription) return undefined;
    const { results, status, error } = this.run(() =>
      subscription.query.state(),
    );
    if (error) throw error;
    return {
      results,
      status,
      loadMore: (numItems: number) =>
        this.loadMoreOfPaginatedQuery(token, numItems),
    };
  }

  private onBaseTransition(transition: Transition) {
    let paginatedQueries: ExtendedTransition["paginatedQueries"] = [];
    if (transition.queries.length > 0 && this.subscriptions.size > 0) {
      const changedBaseTokens = new Set<string>(
        transition.queries.map((q) => q.token),
      );
      // Reading the pages also starts and completes their splits.
      const changed = this.run(() =>
        Array.from(this.subscriptions).filter(([, { query }]) =>
          query.pageTokens().some((token) => changedBaseTokens.has(token)),
        ),
      );
      paginatedQueries = changed.map(([token]) => ({
        token,
        modification: {
          kind: "Updated",
          result: this.resultOrUndefined(token),
        },
      }));
    }
    this.onTransition({ ...transition, paginatedQueries });
  }

  // A failed page throws when the subscriber reads the result.
  private resultOrUndefined(token: PaginatedQueryToken) {
    try {
      return this.localQueryResultByToken(token);
    } catch {
      return undefined;
    }
  }

  /**
   * Load more items for a paginated query.
   *
   * This *always* causes a transition, the status of the query
   * has probably changed from "CanLoadMore" to "LoadingMore".
   */
  private loadMoreOfPaginatedQuery(
    token: PaginatedQueryToken,
    numItems: number,
  ): boolean {
    const subscription = this.subscriptions.get(token);
    if (!subscription) {
      throw new Error("paginated query no longer exists for token " + token);
    }
    if (!this.run(() => subscription.query.loadMore(numItems))) return false;
    this.onTransition({
      timestamp: this.lastTransitionTs,
      reflectedMutations: [],
      queries: [],
      paginatedQueries: [
        {
          token,
          modification: {
            kind: "Updated",
            result: this.resultOrUndefined(token),
          },
        },
      ],
    });
    return true;
  }

  private removePaginatedQuerySubscriber(token: PaginatedQueryToken): void {
    const subscription = this.subscriptions.get(token);
    if (!subscription) return;
    subscription.numSubscribers -= 1;
    if (subscription.numSubscribers > 0) return;
    this.subscriptions.delete(token);
    this.run(subscription.unsubscribe);
  }
}
