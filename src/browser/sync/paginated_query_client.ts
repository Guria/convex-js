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
 */

import { Value } from "../../values/index.js";
import {
  PaginatedQueryToken,
  QueryToken,
  serializePaginatedPathAndArgs,
  canonicalizeUdfPath,
} from "./udf_path_utils.js";
import { BaseConvexClientInterface, Transition } from "./client.js";
import {
  PaginatedQueryResult,
  PaginationStatus,
  asPaginationResult,
} from "./pagination.js";
import { TS } from "./protocol.js";
import { Long } from "../../vendor/long.js";
import { action, atom, Atom, bind, context, Frame } from "./reatom.js";

type QueryPageKey = number;

type PageQuery = {
  queryToken: QueryToken;
  unsubscribe: () => void;
  cursor: string | null;
};

/**
 * The pages of a paginated query.
 *
 * To know the order of pages it's necessary to consult `pageKeys`.
 * The pages in this array are active, they constitute a gapless sequence of results.
 * Some pages are not in this array: they may be waiting for data for a page split.
 */
type Pages = {
  nextPageKey: QueryPageKey;
  pageKeys: readonly QueryPageKey[]; // These pages make up the active page queries.
  // Map page keys to their query subscriptions
  pageKeyToQuery: ReadonlyMap<QueryPageKey, PageQuery>;
  ongoingSplits: ReadonlyMap<
    QueryPageKey,
    readonly [QueryPageKey, QueryPageKey]
  >;
};

/**
 * Represents a paginated query subscription with multiple pages.
 */
type LocalPaginatedQuery = {
  token: PaginatedQueryToken;
  canonicalizedUdfPath: string;
  args: Record<string, Value>; // WITHOUT paginationOpts
  numSubscribers: Atom<number>;
  options: { initialNumItems: number };
  pages: Atom<Pages>;

  // Give separate uses of the query separate identities,
  // which may be removed in the future to improve caching.
  id: number;
};

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

/**
 * Maps subscriptions to paginated queries to their page queries. Page lists
 * live in atoms; `loadMore` and page splits are actions.
 *
 * @internal
 */
export class PaginatedQueryClient {
  // Indexes: paginated queries by token, and the paginated queries each page
  // query belongs to.
  private paginatedQuerySet: Map<PaginatedQueryToken, LocalPaginatedQuery> =
    new Map();
  private pageOwners: Map<QueryToken, Set<PaginatedQueryToken>> = new Map();
  // hold onto a real Transition so we can construct synthetic ones with that timestamp
  private readonly lastTransitionTs: Atom<TS>;
  private readonly root: Frame;
  private readonly loadMoreAction: (
    token: PaginatedQueryToken,
    numItems: number,
  ) => boolean;

  constructor(
    private client: BaseConvexClientInterface,
    private onTransition: (transition: ExtendedTransition) => void,
  ) {
    this.root = context();
    // Nonsense initial value to construct synthetic Transitions
    this.lastTransitionTs = atom(
      Long.fromNumber(0),
      "paginatedQueryClient.lastTransitionTs",
    );
    this.loadMoreAction = bind(
      action(
        (token: PaginatedQueryToken, numItems: number) =>
          this.loadMoreOfPaginatedQuery(token, numItems),
        "paginatedQueryClient.loadMore",
      ),
      this.root,
    );
    this.client.addOnTransitionHandler(
      bind(
        (transition: Transition) => this.onBaseTransition(transition),
        this.root,
      ),
    );
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
    return this.root.run(() => {
      const canonicalizedUdfPath = canonicalizeUdfPath(name);
      // Note that only the expected options are included in the serialization.
      const token = serializePaginatedPathAndArgs(
        canonicalizedUdfPath,
        args,
        options,
      );

      const unsubscribe = bind(
        () => this.removePaginatedQuerySubscriber(token),
        this.root,
      );

      const existingEntry = this.paginatedQuerySet.get(token);
      if (existingEntry) {
        existingEntry.numSubscribers.set((count) => count + 1);
        return {
          paginatedQueryToken: token,
          unsubscribe,
        };
      }

      // Create new paginated query
      this.paginatedQuerySet.set(token, {
        token,
        canonicalizedUdfPath,
        args,
        numSubscribers: atom(1, "paginatedQuery.numSubscribers"),
        options: { initialNumItems: options.initialNumItems },
        pages: atom<Pages>(
          {
            nextPageKey: 0,
            pageKeys: [],
            pageKeyToQuery: new Map(),
            ongoingSplits: new Map(),
          },
          "paginatedQuery.pages",
        ),
        id: options.id,
      });

      this.addPageToPaginatedQuery(token, null, options.initialNumItems);

      return {
        paginatedQueryToken: token,
        unsubscribe,
      };
    });
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
    const canonicalizedUdfPath = canonicalizeUdfPath(name);
    const token = serializePaginatedPathAndArgs(
      canonicalizedUdfPath,
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
    const paginatedQuery = this.paginatedQuerySet.get(token);
    if (!paginatedQuery) {
      return undefined;
    }
    const loadMore = (numItems: number) => this.loadMoreAction(token, numItems);

    const activePages = this.root.run(() =>
      this.activePageQueryTokens(paginatedQuery),
    );
    if (activePages.length === 0) {
      return {
        results: [],
        status: "LoadingFirstPage",
        loadMore,
      };
    }

    let allResults: Value[] = [];

    // Some page is loading (this isn't supposed to happen to any page but the last)
    let hasUndefined = false;
    let isDone = false;

    for (const pageToken of activePages) {
      // This throws, don't catch it, it should bubble up.
      // It might be a InvalidCursor Error. If it is, this query
      // should be reset (for now, use a new ID to ensure new state).
      //
      // In the future this might be caught and dealt with here but
      // an ID-based solution won't work here, ID is an intrinsic property
      // of this paginated query.
      const result = this.client.localQueryResultByToken(pageToken);

      if (result === undefined) {
        hasUndefined = true;
        isDone = false;
        continue;
      }

      const paginationResult = asPaginationResult(result);
      allResults = allResults.concat(paginationResult.page);
      // logic only relevant to the last page, we just happen to run it each time
      isDone = !!paginationResult.isDone;
    }

    let status: PaginationStatus;
    if (hasUndefined) {
      status = allResults.length === 0 ? "LoadingFirstPage" : "LoadingMore";
    } else if (isDone) {
      status = "Exhausted";
    } else {
      status = "CanLoadMore";
    }

    return {
      results: allResults,
      status,
      loadMore,
    };
  }

  private onBaseTransition(transition: Transition) {
    const changed = this.queriesContainingTokens(
      transition.queries.map((q) => q.token),
    );

    let paginatedQueries: Array<{
      token: PaginatedQueryToken;
      modification: PaginatedQueryModification;
    }> = [];

    if (changed.length > 0) {
      this.processPaginatedQuerySplits(changed, (token) =>
        this.client.localQueryResultByToken(token),
      );

      paginatedQueries = changed.map((token) => ({
        token,
        modification: {
          kind: "Updated" as const,
          result: this.localQueryResultByToken(token),
        },
      }));
    }

    const extendedTransition: ExtendedTransition = {
      ...transition,
      paginatedQueries,
    };

    this.onTransition(extendedTransition);
  }

  /**
   * Load more items for a paginated query.
   *
   * This *always* causes a transition, the status of the query
   * has probably changed from "CanLoadMore" to "LoadingMore".
   * Data might have changed too: maybe a subscription to this page
   * query already exists (unlikely but possible) or this page query
   * has an optimistic update providing some initial data.
   *
   * @internal
   */
  private loadMoreOfPaginatedQuery(
    token: PaginatedQueryToken,
    numItems: number,
  ): boolean {
    this.mustGetPaginatedQuery(token);

    const lastPageToken = this.queryTokenForLastPageOfPaginatedQuery(token);
    const lastPageResult = this.client.localQueryResultByToken(lastPageToken);

    if (!lastPageResult) {
      // Still loading a page and concurrent loads are not allowed
      return false;
    }

    const paginationResult = asPaginationResult(lastPageResult);
    if (paginationResult.isDone) {
      // No more pages available
      return false;
    }

    this.addPageToPaginatedQuery(
      token,
      paginationResult.continueCursor,
      numItems,
    );

    const loadMoreTransition: ExtendedTransition = {
      timestamp: this.lastTransitionTs(),
      reflectedMutations: [],
      queries: [],
      paginatedQueries: [
        {
          token,
          modification: {
            kind: "Updated" as const,
            result: this.localQueryResultByToken(token),
          },
        },
      ],
    };
    this.onTransition(loadMoreTransition);

    return true;
  }

  /**
   * @internal
   */
  private queriesContainingTokens(
    queryTokens: QueryToken[],
  ): PaginatedQueryToken[] {
    const changed = new Set<PaginatedQueryToken>();
    for (const queryToken of queryTokens) {
      const owners = this.pageOwners.get(queryToken);
      if (owners === undefined) continue;
      for (const owner of owners) changed.add(owner);
    }
    return [...changed];
  }

  /**
   * @internal
   */
  private processPaginatedQuerySplits(
    changed: PaginatedQueryToken[],
    getResult: (token: QueryToken) => Value | undefined,
  ): void {
    for (const paginatedQueryToken of changed) {
      const paginatedQuery = this.mustGetPaginatedQuery(paginatedQueryToken);

      // Check for any completed splits
      for (const [pageKey, [splitKey1, splitKey2]] of paginatedQuery.pages()
        .ongoingSplits) {
        const { pageKeyToQuery } = paginatedQuery.pages();
        const bothNewPagesLoaded =
          getResult(pageKeyToQuery.get(splitKey1)!.queryToken) !== undefined &&
          getResult(pageKeyToQuery.get(splitKey2)!.queryToken) !== undefined;

        if (bothNewPagesLoaded) {
          this.completePaginatedQuerySplit(
            paginatedQuery,
            pageKey,
            splitKey1,
            splitKey2,
          );
        }
      }

      // Check each active page for splits needed
      for (const pageKey of paginatedQuery.pages().pageKeys) {
        const { ongoingSplits, pageKeyToQuery } = paginatedQuery.pages();
        if (ongoingSplits.has(pageKey)) {
          continue; // Already splitting
        }

        const pageEntry = pageKeyToQuery.get(pageKey);
        if (!pageEntry) {
          throw new Error(`No page query for active pageKey ${pageKey}`);
        }
        const pageResult = getResult(pageEntry.queryToken);
        if (!pageResult) {
          continue;
        }
        const result = asPaginationResult(pageResult);

        // Check if this page needs splitting
        const shouldSplit =
          result.splitCursor &&
          (result.pageStatus === "SplitRecommended" ||
            result.pageStatus === "SplitRequired" ||
            // This client-driven page splitting condition will change in the future.
            result.page.length > paginatedQuery.options.initialNumItems * 2);

        if (shouldSplit) {
          this.splitPaginatedQueryPage(
            paginatedQuery,
            pageKey,
            pageEntry.cursor,
            result.splitCursor!, // we just checked
            result.continueCursor,
          );
        }
      }
    }
  }

  private subscribePage(
    paginatedQuery: LocalPaginatedQuery,
    paginationOpts: Value,
    cursor: string | null,
  ): PageQuery {
    const subscription = this.client.subscribe(
      paginatedQuery.canonicalizedUdfPath,
      { ...paginatedQuery.args, paginationOpts },
    );
    const { queryToken } = subscription;
    let owners = this.pageOwners.get(queryToken);
    if (owners === undefined) {
      owners = new Set();
      this.pageOwners.set(queryToken, owners);
    }
    owners.add(paginatedQuery.token);
    return {
      queryToken,
      cursor,
      unsubscribe: () => {
        subscription.unsubscribe();
        const current = this.pageOwners.get(queryToken);
        if (current === undefined) return;
        // Another page of the same paginated query may share the token.
        const stillOwned = [
          ...paginatedQuery.pages().pageKeyToQuery.values(),
        ].some((page) => page.queryToken === queryToken);
        if (!stillOwned) current.delete(paginatedQuery.token);
        if (current.size === 0) this.pageOwners.delete(queryToken);
      },
    };
  }

  private splitPaginatedQueryPage(
    paginatedQuery: LocalPaginatedQuery,
    pageKey: QueryPageKey,
    // Where the page being split started.
    startCursor: string | null,
    // The split position - will be end of the first split page and the start
    // of the second.
    splitCursor: string,
    // Where the page being split ended.
    continueCursor: string | null,
  ): void {
    const pages = paginatedQuery.pages();
    const splitKey1 = pages.nextPageKey;
    const splitKey2 = pages.nextPageKey + 1;

    const paginationOpts = {
      numItems: paginatedQuery.options.initialNumItems,
      id: paginatedQuery.id,
    };

    // First split page: same cursor as the original page, ending at splitCursor
    const first = this.subscribePage(
      paginatedQuery,
      { ...paginationOpts, cursor: startCursor, endCursor: splitCursor },
      startCursor,
    );
    // Second split page: cursor starts at splitCursor, endCursor is the original continueCursor
    const second = this.subscribePage(
      paginatedQuery,
      { ...paginationOpts, cursor: splitCursor, endCursor: continueCursor },
      splitCursor,
    );

    paginatedQuery.pages.set({
      ...pages,
      nextPageKey: pages.nextPageKey + 2,
      pageKeyToQuery: new Map(pages.pageKeyToQuery)
        .set(splitKey1, first)
        .set(splitKey2, second),
      ongoingSplits: new Map(pages.ongoingSplits).set(pageKey, [
        splitKey1,
        splitKey2,
      ]),
    });
  }

  /**
   * @internal
   */
  private addPageToPaginatedQuery(
    token: PaginatedQueryToken,
    continueCursor: string | null,
    numItems: number,
  ): PageQuery {
    const paginatedQuery = this.mustGetPaginatedQuery(token);
    const pages = paginatedQuery.pages();
    const pageKey = pages.nextPageKey;

    const subscription = this.subscribePage(
      paginatedQuery,
      { cursor: continueCursor, numItems, id: paginatedQuery.id },
      continueCursor,
    );

    paginatedQuery.pages.set({
      ...pages,
      nextPageKey: pageKey + 1,
      pageKeys: [...pages.pageKeys, pageKey],
      pageKeyToQuery: new Map(pages.pageKeyToQuery).set(pageKey, subscription),
    });
    return subscription;
  }

  private removePaginatedQuerySubscriber(token: PaginatedQueryToken): void {
    const paginatedQuery = this.paginatedQuerySet.get(token);
    if (!paginatedQuery) {
      return;
    }

    const numSubscribers = paginatedQuery.numSubscribers.set(
      (count) => count - 1,
    );
    if (numSubscribers > 0) {
      return;
    }

    this.paginatedQuerySet.delete(token);
    // Remove all page subscriptions by calling their unsubscribe callbacks
    const subscriptions = [...paginatedQuery.pages().pageKeyToQuery.values()];
    paginatedQuery.pages.set({
      nextPageKey: 0,
      pageKeys: [],
      pageKeyToQuery: new Map(),
      ongoingSplits: new Map(),
    });
    for (const subscription of subscriptions) {
      subscription.unsubscribe();
    }
  }

  private completePaginatedQuerySplit(
    paginatedQuery: LocalPaginatedQuery,
    pageKey: QueryPageKey,
    splitKey1: QueryPageKey,
    splitKey2: QueryPageKey,
  ): void {
    const pages = paginatedQuery.pages();
    const originalQuery = pages.pageKeyToQuery.get(pageKey)!;
    const pageKeyToQuery = new Map(pages.pageKeyToQuery);
    pageKeyToQuery.delete(pageKey);
    const ongoingSplits = new Map(pages.ongoingSplits);
    ongoingSplits.delete(pageKey);
    paginatedQuery.pages.set({
      ...pages,
      pageKeys: pages.pageKeys.flatMap((key) =>
        key === pageKey ? [splitKey1, splitKey2] : [key],
      ),
      pageKeyToQuery,
      ongoingSplits,
    });
    originalQuery.unsubscribe();
  }

  /** The query tokens for all active pages, in result order */
  private activePageQueryTokens(
    paginatedQuery: LocalPaginatedQuery,
  ): QueryToken[] {
    const { pageKeys, pageKeyToQuery } = paginatedQuery.pages();
    return pageKeys.map((pageKey) => pageKeyToQuery.get(pageKey)!.queryToken);
  }

  private queryTokenForLastPageOfPaginatedQuery(
    token: PaginatedQueryToken,
  ): QueryToken {
    const paginatedQuery = this.mustGetPaginatedQuery(token);
    const { pageKeys, pageKeyToQuery } = paginatedQuery.pages();
    const lastPageKey = pageKeys[pageKeys.length - 1];
    if (lastPageKey === undefined) {
      throw new Error(`No pages for paginated query ${token}`);
    }
    return pageKeyToQuery.get(lastPageKey)!.queryToken;
  }

  private mustGetPaginatedQuery(
    token: PaginatedQueryToken,
  ): LocalPaginatedQuery {
    const paginatedQuery = this.paginatedQuerySet.get(token);
    if (!paginatedQuery) {
      throw new Error("paginated query no longer exists for token " + token);
    }
    return paginatedQuery;
  }
}
