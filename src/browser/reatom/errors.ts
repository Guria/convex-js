import { ConvexError } from "convex/values";
import { createHybridErrorStacktrace, forwardData } from "../logging.js";
import { FunctionFailure } from "../sync/function_result.js";

/** The error a failed function call surfaces as: `ConvexError` when it carries data. */
export function functionError(
  type: "query" | "mutation" | "action",
  udfPath: string,
  result: FunctionFailure,
): Error {
  return result.errorData !== undefined
    ? forwardData(
        result,
        new ConvexError(createHybridErrorStacktrace(type, udfPath, result)),
      )
    : new Error(createHybridErrorStacktrace(type, udfPath, result));
}
