/**
 * Reactive pagination over page queries. Loaded pages stay subscribed and
 * keep their boundaries; a page the server asks to split is replaced by two
 * once both halves have loaded. The page list derives itself from its
 * previous value and the page results it reads, so subscribing it subscribes
 * exactly the pages in use.
 */
import {
  action,
  atom,
  computed,
  noop,
  withComputed,
  type Action,
  type Atom,
  type AtomLike,
  type Computed,
} from "@reatom/core";
import { Value } from "convex/values";
import { FunctionResult } from "../sync/function_result.js";
import { asPaginationResult, PaginationStatus } from "../sync/pagination.js";
import {
  PaginatedQueryToken,
  QueryToken,
  serializePaginatedPathAndArgs,
} from "../sync/udf_path_utils.js";
import { withConnectionHooks } from "./connection_hooks.js";
import { functionError } from "./errors.js";

/** What a paginated query needs of a page query. */
export type PageQuery = {
  readonly token: QueryToken;
  readonly result: AtomLike<FunctionResult | undefined, []>;
};

type Page = { readonly query: PageQuery; readonly cursor: string | null };

type PageList = {
  /** Active pages in result order, by key. */
  readonly keys: readonly number[];
  readonly pages: ReadonlyMap<number, Page>;
  /** Pages being split, by the key of the page they replace. */
  readonly splits: ReadonlyMap<number, readonly [number, number]>;
  readonly nextKey: number;
};

/**
 * The state of a {@link ReatomPaginatedQuery}.
 *
 * @public
 */
export type PaginatedQueryState<Item> = {
  results: Item[];
  status: PaginationStatus;
  /** The error a page failed with. */
  error?: Error;
};

/**
 * A paginated query of a {@link ReatomConvexClient}.
 *
 * Reading its atoms inside a subscribed Reatom graph subscribes its pages.
 * Once nothing reads it, it drops its pages and starts over from the first
 * one.
 *
 * @public
 */
export interface ReatomPaginatedQuery<Item = Value> {
  readonly token: PaginatedQueryToken;
  readonly state: Computed<PaginatedQueryState<Item>>;
  readonly results: Computed<Item[]>;
  readonly status: Computed<PaginationStatus>;
  readonly error: Computed<Error | undefined>;
  /**
   * Load `numItems` more items. Returns whether a page load started: not
   * while a page loads or once the query is exhausted.
   */
  readonly loadMore: Action<[numItems: number], boolean>;
  /**
   * The page queries in use, splits included.
   *
   * @internal
   */
  readonly pageTokens: () => QueryToken[];
}

export type PaginatedOptions = { initialNumItems: number; id: number };

type PageFactory = (
  udfPath: string,
  args: Record<string, Value>,
  paginationOpts: Record<string, Value>,
) => PageQuery;

const loadedPage = (query: PageQuery) => {
  const result = query.result();
  return result?.success ? asPaginationResult(result.value) : undefined;
};

export function reatomPaginatedQuery(
  page: PageFactory,
  udfPath: string,
  args: Record<string, Value>,
  options: PaginatedOptions,
  name: string,
): ReatomPaginatedQuery {
  const { initialNumItems, id } = options;
  const token = serializePaginatedPathAndArgs(udfPath, args, options);

  const pageAt = (
    paginationOpts: Record<string, Value>,
    cursor: string | null,
  ) => ({
    query: page(udfPath, args, paginationOpts),
    cursor,
  });

  const firstPage = (): PageList => ({
    keys: [0],
    pages: new Map([
      [0, pageAt({ cursor: null, numItems: initialNumItems, id }, null)],
    ]),
    splits: new Map(),
    nextKey: 1,
  });

  /** Swaps in the halves of each split once both have loaded. */
  const completeSplits = (list: PageList): PageList => {
    let next = list;
    for (const [key, [first, second]] of list.splits) {
      const halvesLoaded =
        list.pages.get(first)!.query.result() !== undefined &&
        list.pages.get(second)!.query.result() !== undefined;
      if (!halvesLoaded) continue;
      const pages = new Map(next.pages);
      pages.delete(key);
      const splits = new Map(next.splits);
      splits.delete(key);
      next = {
        ...next,
        keys: next.keys.flatMap((current) =>
          current === key ? [first, second] : [current],
        ),
        pages,
        splits,
      };
    }
    return next;
  };

  /** Starts splitting each active page the server or its size asks to split. */
  const startSplits = (list: PageList): PageList => {
    let next = list;
    for (const key of list.keys) {
      if (next.splits.has(key)) continue;
      const current = list.pages.get(key)!;
      const result = loadedPage(current.query);
      if (!result) continue;
      const shouldSplit =
        result.splitCursor &&
        (result.pageStatus === "SplitRecommended" ||
          result.pageStatus === "SplitRequired" ||
          // This client-driven page splitting condition may change.
          result.page.length > initialNumItems * 2);
      if (!shouldSplit) continue;
      const splitCursor = result.splitCursor!;
      const first = next.nextKey;
      const second = first + 1;
      const paginationOpts = { numItems: initialNumItems, id };
      const pages = new Map(next.pages);
      pages.set(
        first,
        pageAt(
          { ...paginationOpts, cursor: current.cursor, endCursor: splitCursor },
          current.cursor,
        ),
      );
      pages.set(
        second,
        pageAt(
          {
            ...paginationOpts,
            cursor: splitCursor,
            endCursor: result.continueCursor,
          },
          splitCursor,
        ),
      );
      // Read now, so the halves loading derives the list again.
      pages.get(first)!.query.result();
      pages.get(second)!.query.result();
      next = {
        ...next,
        pages,
        splits: new Map(next.splits).set(key, [first, second]),
        nextKey: second + 1,
      };
    }
    return next;
  };

  const pages: Atom<PageList | null> = atom<PageList | null>(
    null,
    `${name}.pages`,
  ).extend(
    withComputed((current) => {
      const next = startSplits(completeSplits(current ?? firstPage()));
      // Exactly the pages in the list are read, so a replaced page drops.
      for (const { query } of next.pages.values()) query.result();
      return next;
    }),
    // Like `PaginatedQueryClient`, forget loaded pages once unused.
    withConnectionHooks(noop, () => {
      pages.set(null);
    }),
  );

  const state = computed((): PaginatedQueryState<Value> => {
    const list = pages()!;
    let results: Value[] = [];
    // A page is loading; only the last one is supposed to.
    let hasUndefined = false;
    let isDone = false;
    for (const key of list.keys) {
      const result = list.pages.get(key)!.query.result();
      if (result === undefined) {
        hasUndefined = true;
        isDone = false;
        continue;
      }
      if (!result.success) {
        return {
          results: [],
          status: "LoadingFirstPage",
          error: functionError("query", udfPath, result),
        };
      }
      const paginationResult = asPaginationResult(result.value);
      results = results.concat(paginationResult.page);
      isDone = !!paginationResult.isDone;
    }
    const status: PaginationStatus = hasUndefined
      ? results.length === 0
        ? "LoadingFirstPage"
        : "LoadingMore"
      : isDone
        ? "Exhausted"
        : "CanLoadMore";
    return { results, status };
  }, `${name}.state`);

  const loadMore = action((numItems: number): boolean => {
    const list = pages()!;
    const last = list.pages.get(list.keys[list.keys.length - 1])!;
    const result = loadedPage(last.query);
    // Still loading a page, or no more pages.
    if (!result || result.isDone) return false;
    const key = list.nextKey;
    pages.set({
      ...list,
      keys: [...list.keys, key],
      pages: new Map(list.pages).set(
        key,
        pageAt(
          { cursor: result.continueCursor, numItems, id },
          result.continueCursor,
        ),
      ),
      nextKey: key + 1,
    });
    return true;
  }, `${name}.loadMore`);

  return {
    token,
    state,
    results: computed(() => state().results, `${name}.results`),
    status: computed(() => state().status, `${name}.status`),
    error: computed(() => state().error, `${name}.error`),
    loadMore,
    pageTokens: () =>
      Array.from(pages()!.pages.values(), ({ query }) => query.token),
  };
}
