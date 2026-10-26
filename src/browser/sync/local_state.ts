/**
 * The local state of the client:
 * - which queries are subscribed to, and the query set changes not sent yet
 * - the "Query Set Version," used to produce QuerySetModification messages
 * - the current auth token and "Identity Version"
 *
 * Each query has one `QueryEntry` for as long as it is subscribed, visible or
 * set by an optimistic update. The entry's `result` atom is what readers see;
 * `RemoteQuerySet` fills `serverResult` and `OptimisticQueryResults` decides
 * `result`.
 **/

import { convexToJson, Value } from "../../values/index.js";
import { FunctionResult } from "./function_result.js";
import {
  AddQuery,
  RemoveQuery,
  QueryId,
  QuerySetModification,
  Authenticate,
  QueryJournal,
  Transition,
  AdminAuthentication,
  UserIdentityAttributes,
} from "./protocol.js";
import { atom, Atom } from "./reatom.js";
import {
  canonicalizeUdfPath,
  QueryToken,
  serializePathAndArgs,
} from "./udf_path_utils.js";

export type QueryEntry = {
  readonly token: QueryToken;
  readonly udfPath: string;
  readonly args: Record<string, Value>;
  /** The result with optimistic updates applied, `undefined` while loading. */
  readonly result: Atom<FunctionResult | undefined>;
  /**
   * Whether `result` counts as a local result. An optimistic update can make
   * a query present with an `undefined` (loading) result.
   */
  present: boolean;
  /** The result received on the current connection. */
  serverResult: FunctionResult | undefined;
  /** Callers of `subscribe` that have not unsubscribed. */
  subscribers: number;
  /** In the server's query set, or about to be; `null` otherwise. */
  id: QueryId | null;
  /** A query set change not sent yet. */
  op: "Add" | "Remove" | null;
  journal: QueryJournal | undefined;
  componentPath: string | undefined;
};

export type AuthState =
  | {
      tokenType: "User";
      value: string;
    }
  | {
      tokenType: "Admin";
      value: string;
      impersonating?: UserIdentityAttributes | undefined;
    };

function addOf(entry: QueryEntry): AddQuery {
  return {
    type: "Add",
    queryId: entry.id!,
    udfPath: entry.udfPath,
    args: [convexToJson(entry.args)],
    journal: entry.journal,
    componentPath: entry.componentPath,
  };
}

export function reatomLocalSyncState(name: string) {
  const querySetVersion = atom(0, `${name}.querySetVersion`);
  const identityVersion = atom(0, `${name}.identityVersion`);
  const auth = atom<AuthState | undefined>(undefined, `${name}.auth`);
  /** Paused for an auth token: query set and identity changes wait. */
  const paused = atom(false, `${name}.paused`);
  const outstandingAuthOlderThanRestart = atom(
    false,
    `${name}.outstandingAuthOlderThanRestart`,
  );

  // Indexes over the entries; lookups stay O(1).
  const entries = new Map<QueryToken, QueryEntry>();
  const byId = new Map<QueryId, QueryEntry>();
  const pending = new Map<QueryId, QueryEntry>();
  /** Entries that left the query set; their results go on the next ingest. */
  const retired = new Set<QueryEntry>();
  const outstandingQueriesOlderThanRestart = new Set<QueryId>();
  let nextQueryId: QueryId = 0;

  const entry = (
    udfPath: string,
    args: Record<string, Value>,
    token = serializePathAndArgs(udfPath, args),
  ): QueryEntry => {
    const existing = entries.get(token);
    if (existing !== undefined) return existing;
    const created: QueryEntry = {
      token,
      udfPath: canonicalizeUdfPath(udfPath),
      args,
      result: atom<FunctionResult | undefined>(undefined, `${name}.query`),
      present: false,
      serverResult: undefined,
      subscribers: 0,
      id: null,
      op: null,
      journal: undefined,
      componentPath: undefined,
    };
    entries.set(token, created);
    return created;
  };

  const finalizeRemove = (removed: QueryEntry) => {
    byId.delete(removed.id!);
    outstandingQueriesOlderThanRestart.delete(removed.id!);
    removed.id = null;
    removed.serverResult = undefined;
    retired.add(removed);
  };

  /** Takes the query set changes not sent yet. */
  const takePending = (): (AddQuery | RemoveQuery)[] => {
    const modifications: (AddQuery | RemoveQuery)[] = [];
    for (const changed of pending.values()) {
      if (changed.op === "Add") {
        modifications.push(addOf(changed));
      } else {
        modifications.push({ type: "Remove", queryId: changed.id! });
        finalizeRemove(changed);
      }
      changed.op = null;
    }
    pending.clear();
    return modifications;
  };

  return {
    querySetVersion,
    identityVersion,
    auth,
    paused,
    entries,
    byId,
    retired,
    entry,

    hasSyncedPastLastReconnect(): boolean {
      return (
        outstandingQueriesOlderThanRestart.size === 0 &&
        !outstandingAuthOlderThanRestart()
      );
    },

    markAuthCompletion() {
      outstandingAuthOlderThanRestart.set(false);
    },

    /**
     * Adds a subscriber; returns whether the query set changed and needs a
     * flush.
     */
    subscribe(
      subscribed: QueryEntry,
      journal: QueryJournal | undefined,
      componentPath: string | undefined,
    ): boolean {
      subscribed.subscribers += 1;
      if (subscribed.subscribers > 1) return false;
      // Resubscribed before the removal went out: the server still has it.
      if (subscribed.op === "Remove") {
        subscribed.op = null;
        pending.delete(subscribed.id!);
        return false;
      }
      const queryId = nextQueryId++;
      subscribed.id = queryId;
      subscribed.op = "Add";
      subscribed.journal = journal;
      subscribed.componentPath = componentPath;
      byId.set(queryId, subscribed);
      pending.set(queryId, subscribed);
      return true;
    },

    /** Removes a subscriber; returns whether the query set changed. */
    unsubscribe(subscribed: QueryEntry): boolean {
      subscribed.subscribers -= 1;
      if (subscribed.subscribers > 0) return false;
      outstandingQueriesOlderThanRestart.delete(subscribed.id!);
      // Never sent: forget it.
      if (subscribed.op === "Add") {
        pending.delete(subscribed.id!);
        byId.delete(subscribed.id!);
        subscribed.id = null;
        subscribed.op = null;
        retired.add(subscribed);
        return false;
      }
      subscribed.op = "Remove";
      pending.set(subscribed.id!, subscribed);
      return true;
    },

    hasPendingQuerySetModifications(): boolean {
      return pending.size > 0;
    },

    /** The query set changes not sent yet as one message, unless paused. */
    takeQuerySetModification(): QuerySetModification | null {
      if (pending.size === 0 || paused()) return null;
      const baseVersion = querySetVersion();
      const modifications = takePending();
      querySetVersion.set(baseVersion + 1);
      return {
        type: "ModifyQuerySet",
        baseVersion,
        newVersion: baseVersion + 1,
        modifications,
      };
    },

    transition(transition: Transition) {
      for (const modification of transition.modifications) {
        switch (modification.type) {
          case "QueryUpdated":
          case "QueryFailed": {
            outstandingQueriesOlderThanRestart.delete(modification.queryId);
            const journal = modification.journal;
            if (journal !== undefined) {
              // We may have already unsubscribed to this query by the time the server
              // sends us the journal. If so, just ignore it.
              const updated = byId.get(modification.queryId);
              if (updated !== undefined) {
                updated.journal = journal;
              }
            }
            break;
          }
          case "QueryRemoved": {
            outstandingQueriesOlderThanRestart.delete(modification.queryId);
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
    },

    isCurrentOrNewerAuthVersion(version: number): boolean {
      return version >= identityVersion();
    },

    getAuth(): AuthState | undefined {
      return auth();
    },

    setAuth(value: string): Authenticate {
      const next = { tokenType: "User" as const, value };
      auth.set(next);
      const baseVersion = identityVersion();
      if (!paused()) {
        identityVersion.set(baseVersion + 1);
      }
      return { type: "Authenticate", baseVersion, ...next };
    },

    setAdminAuth(
      value: string,
      actingAs?: UserIdentityAttributes,
    ): AdminAuthentication {
      const next = {
        tokenType: "Admin" as const,
        value,
        impersonating: actingAs,
      };
      auth.set(next);
      const baseVersion = identityVersion();
      if (!paused()) {
        identityVersion.set(baseVersion + 1);
      }
      return { type: "Authenticate", baseVersion, ...next };
    },

    clearAuth(): Authenticate {
      auth.set(undefined);
      outstandingAuthOlderThanRestart.set(false);
      const baseVersion = identityVersion();
      if (!paused()) {
        identityVersion.set(baseVersion + 1);
      }
      return { type: "Authenticate", tokenType: "None", baseVersion };
    },

    hasAuth(): boolean {
      return !!auth();
    },

    isNewAuth(value: string): boolean {
      return auth()?.value !== value;
    },

    queryJournal(queryToken: QueryToken): QueryJournal | undefined {
      const subscribed = entries.get(queryToken);
      return subscribed !== undefined && subscribed.subscribers > 0
        ? subscribed.journal
        : undefined;
    },

    /**
     * A new connection starts from an empty query set: resend every
     * subscribed query and the auth.
     */
    restart(): [QuerySetModification, (Authenticate | undefined)?] {
      // Restart works whether we are paused or unpaused. The changes not sent
      // yet are dropped: the query set is sent from scratch.
      paused.set(false);
      for (const changed of pending.values()) {
        if (changed.op === "Remove") finalizeRemove(changed);
        changed.op = null;
      }
      pending.clear();

      outstandingQueriesOlderThanRestart.clear();
      const modifications: AddQuery[] = [];
      for (const subscribed of byId.values()) {
        modifications.push(addOf(subscribed));
        // Track all re-sent queries as outstanding so the backoff retry
        // counter doesn't reset until the server has re-confirmed results
        // for every active query.
        outstandingQueriesOlderThanRestart.add(subscribed.id!);
      }
      querySetVersion.set(1);
      const querySet: QuerySetModification = {
        type: "ModifyQuerySet",
        baseVersion: 0,
        newVersion: 1,
        modifications,
      };
      // If there's no auth, no need to send an update as the server will also start with an unknown identity.
      const current = auth();
      if (!current) {
        identityVersion.set(0);
        return [querySet, undefined];
      }
      outstandingAuthOlderThanRestart.set(true);
      identityVersion.set(1);
      return [querySet, { type: "Authenticate", baseVersion: 0, ...current }];
    },

    pause() {
      paused.set(true);
    },

    resume(): [QuerySetModification | undefined, Authenticate | undefined] {
      let querySet: QuerySetModification | undefined;
      if (pending.size > 0) {
        const baseVersion = querySetVersion();
        querySet = {
          type: "ModifyQuerySet",
          baseVersion,
          newVersion: baseVersion + 1,
          modifications: takePending(),
        };
        querySetVersion.set(baseVersion + 1);
      }
      const current = auth();
      let authenticate: Authenticate | undefined;
      if (current !== undefined) {
        const baseVersion = identityVersion();
        identityVersion.set(baseVersion + 1);
        authenticate = { type: "Authenticate", baseVersion, ...current };
      }
      paused.set(false);
      return [querySet, authenticate];
    },
  };
}

export type LocalSyncState = ReturnType<typeof reatomLocalSyncState>;
