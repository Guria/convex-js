import { jsonToConvex } from "../../values/index.js";
import { logForFunction, Logger } from "../logging.js";
import { Long } from "../../vendor/long.js";
import { FunctionResult } from "./function_result.js";
import {
  ActionRequest,
  ActionResponse,
  ClientMessage,
  MutationRequest,
  MutationResponse,
  RequestId,
} from "./protocol.js";
import { action, atom } from "./reatom.js";

type RequestStatus =
  | {
      status: "Requested" | "NotSent";
      onResult: (result: FunctionResult) => void;
      requestedAt: Date;
    }
  | {
      status: "Completed";
      result: FunctionResult;
      onResolve: () => void;
      ts: Long;
    };

type InflightRequest = {
  message: MutationRequest | ActionRequest;
  status: RequestStatus;
};

/** Mutations and actions sent and not yet reflected in query results. */
export function reatomRequestManager({
  logger,
  name,
}: {
  logger: Logger;
  name: string;
}) {
  const inflightRequests = atom<ReadonlyMap<RequestId, InflightRequest>>(
    new Map(),
    `${name}.inflightRequests`,
  );
  const requestsOlderThanRestart = atom<ReadonlySet<RequestId>>(
    new Set<RequestId>(),
    `${name}.requestsOlderThanRestart`,
  );

  const updateRequests = (
    update: (requests: Map<RequestId, InflightRequest>) => void,
  ) =>
    inflightRequests.set((current) => {
      const next = new Map(current);
      update(next);
      return next;
    });

  const forgetOlderThanRestart = (requestIds: RequestId[]) => {
    const older = requestsOlderThanRestart();
    if (!requestIds.some((requestId) => older.has(requestId))) return;
    const next = new Set(older);
    for (const requestId of requestIds) next.delete(requestId);
    requestsOlderThanRestart.set(next);
  };

  const countOf = (type: "Mutation" | "Action") => {
    let count = 0;
    for (const request of inflightRequests().values()) {
      if (request.message.type === type) count++;
    }
    return count;
  };

  return {
    inflightRequests,

    request: action(
      (
        message: MutationRequest | ActionRequest,
        sent: boolean,
      ): Promise<FunctionResult> =>
        new Promise<FunctionResult>((resolve) => {
          const status = sent ? "Requested" : "NotSent";
          updateRequests((requests) =>
            requests.set(message.requestId, {
              message,
              status: { status, requestedAt: new Date(), onResult: resolve },
            }),
          );
        }),
      `${name}.request`,
    ),

    /**
     * Update the state after receiving a response.
     *
     * @returns A RequestId if the request is complete and its optimistic update
     * can be dropped, null otherwise.
     */
    onResponse: action(
      (
        response: MutationResponse | ActionResponse,
      ): { requestId: RequestId; result: FunctionResult } | null => {
        const requestInfo = inflightRequests().get(response.requestId);
        if (requestInfo === undefined) {
          // Annoyingly we can occasionally get responses to mutations that we're no
          // longer tracking. One flow where this happens is:
          // 1. Client sends mutation 1
          // 2. Client gets response for mutation 1. The sever says that it was committed at ts=10.
          // 3. Client is disconnected
          // 4. Client reconnects and re-issues queries and this mutation.
          // 5. Server sends transition message to ts=20
          // 6. Client drops mutation because it's already been observed.
          // 7. Client receives a second response for mutation 1 but doesn't know about it anymore.

          // The right fix for this is probably to add a reconciliation phase on
          // reconnection where we receive responses to all the mutations before
          // the transition message so this flow could never happen (CX-1513).

          // For now though, we can just ignore this message.
          return null;
        }

        // Because `.restart()` re-requests completed requests, we may get some
        // responses for requests that are already in the "Completed" state.
        // We can safely ignore those because we've already notified the UI about
        // their results.
        const status = requestInfo.status;
        if (status.status === "Completed") {
          return null;
        }

        const udfType =
          requestInfo.message.type === "Mutation" ? "mutation" : "action";
        const udfPath = requestInfo.message.udfPath;

        for (const line of response.logLines) {
          logForFunction(logger, "info", udfType, udfPath, line);
        }

        let result: FunctionResult;
        if (response.success) {
          result = {
            success: true,
            logLines: response.logLines,
            value: jsonToConvex(response.result),
          };
        } else {
          const errorMessage = response.result as string;
          const { errorData } = response;
          logForFunction(logger, "error", udfType, udfPath, errorMessage);
          result = {
            success: false,
            errorMessage,
            errorData:
              errorData !== undefined ? jsonToConvex(errorData) : undefined,
            logLines: response.logLines,
          };
        }
        const onResolve = () => status.onResult(result);

        // We can resolve Mutation failures immediately since they don't have any
        // side effects. Actions are intentionally decoupled from
        // queries/mutations here on the sync protocol since they have different
        // guarantees.
        if (response.type === "ActionResponse" || !response.success) {
          onResolve();
          updateRequests((requests) => requests.delete(response.requestId));
          forgetOlderThanRestart([response.requestId]);
          return { requestId: response.requestId, result };
        }

        // We have to wait to resolve the request promise until after we transition
        // past this timestamp so clients can read their own writes.
        updateRequests((requests) =>
          requests.set(response.requestId, {
            message: requestInfo.message,
            status: { status: "Completed", result, ts: response.ts, onResolve },
          }),
        );
        return null;
      },
      `${name}.onResponse`,
    ),

    /** Removes and returns completed requests. */
    removeCompleted: action((ts: Long): Map<RequestId, FunctionResult> => {
      const completeRequests: Map<RequestId, FunctionResult> = new Map();
      for (const [requestId, requestInfo] of inflightRequests()) {
        const status = requestInfo.status;
        if (status.status === "Completed" && status.ts.lessThanOrEqual(ts)) {
          status.onResolve();
          completeRequests.set(requestId, status.result);
        }
      }
      if (completeRequests.size > 0) {
        updateRequests((requests) => {
          for (const requestId of completeRequests.keys()) {
            requests.delete(requestId);
          }
        });
        forgetOlderThanRestart([...completeRequests.keys()]);
      }
      return completeRequests;
    }, `${name}.removeCompleted`),

    restart: action((): ClientMessage[] => {
      // When we reconnect to the backend, re-request all requests that are safe
      // to be resend.
      const allMessages: ClientMessage[] = [];
      const lost: ((result: FunctionResult) => void)[] = [];
      const older = new Set(inflightRequests().keys());
      updateRequests((requests) => {
        for (const [requestId, value] of requests) {
          if (value.status.status === "NotSent") {
            requests.set(requestId, {
              message: value.message,
              status: { ...value.status, status: "Requested" },
            });
            allMessages.push(value.message);
            continue;
          }

          if (value.message.type === "Mutation") {
            // This includes ones that have already been completed because we still
            // want to tell the backend to transition the client past the completed
            // timestamp. This is safe since mutations are idempotent.
            allMessages.push(value.message);
          } else if (value.message.type === "Action") {
            // Unlike mutations, actions are not idempotent. When we reconnect to the
            // backend, we don't know if it is safe to resend in-flight actions, so we
            // cancel them and consider them failed.
            requests.delete(requestId);
            older.delete(requestId);
            if (value.status.status === "Completed") {
              throw new Error("Action should never be in 'Completed' state");
            }
            lost.push(value.status.onResult);
          }
        }
      });
      requestsOlderThanRestart.set(older);
      for (const onResult of lost) {
        onResult({
          success: false,
          errorMessage: "Connection lost while action was in flight",
          logLines: [],
        });
      }
      return allMessages;
    }, `${name}.restart`),

    resume: action((): ClientMessage[] => {
      const allMessages: ClientMessage[] = [];
      updateRequests((requests) => {
        for (const [requestId, value] of requests) {
          if (value.status.status === "NotSent") {
            requests.set(requestId, {
              message: value.message,
              status: { ...value.status, status: "Requested" },
            });
            allMessages.push(value.message);
          }
        }
      });
      return allMessages;
    }, `${name}.resume`),

    /**
     * @returns true if there are any requests that have been requested but have
     * not be completed yet.
     */
    hasIncompleteRequests(): boolean {
      for (const requestInfo of inflightRequests().values()) {
        if (requestInfo.status.status === "Requested") {
          return true;
        }
      }
      return false;
    },

    /**
     * @returns true if there are any inflight requests, including ones that have
     * completed on the server, but have not been applied.
     */
    hasInflightRequests(): boolean {
      return inflightRequests().size > 0;
    },

    /**
     * @returns true if there are any inflight requests, that have been hanging around
     * since prior to the most recent restart.
     */
    hasSyncedPastLastReconnect(): boolean {
      return requestsOlderThanRestart().size === 0;
    },

    timeOfOldestInflightRequest(): Date | null {
      const requests = inflightRequests();
      if (requests.size === 0) {
        return null;
      }
      let oldestInflightRequest = Date.now();
      for (const request of requests.values()) {
        if (request.status.status !== "Completed") {
          if (request.status.requestedAt.getTime() < oldestInflightRequest) {
            oldestInflightRequest = request.status.requestedAt.getTime();
          }
        }
      }
      return new Date(oldestInflightRequest);
    },

    /**
     * @returns The number of mutations currently in flight.
     */
    inflightMutations(): number {
      return countOf("Mutation");
    },

    /**
     * @returns The number of actions currently in flight.
     */
    inflightActions(): number {
      return countOf("Action");
    },
  };
}

export type RequestManager = ReturnType<typeof reatomRequestManager>;
