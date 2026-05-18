/**
 * Cloudflare Worker entrypoint for streamed PAR-style repair.
 *
 * Request contract:
 * - layout arrives on the request URL query string:
 *   - `n` or `original_count`
 *   - `r` or `recovery_count`
 *   - `s` or `shard_size`
 *   - repeated `i` or `missing_indices`
 *   - optional `total_shard_count`
 * - `multipart/form-data` body containing only:
 *   - an optional `digests` part with `slot_count` contiguous 32-byte SHA-256 hashes
 *     (an all-zero hash disables verification for that slot)
 *   - shard parts named `shard_<slotIndex>` whose bodies are raw shard bytes
 *
 * The worker only returns the shard indexes explicitly requested by `missing_indices`, but the
 * codec must still see every unreceived slot as missing. That distinction is the critical rule for
 * correct repairs when a client intentionally omits unneeded recovery shards from the request.
 */

import {
  MultipartParseError,
  MultipartTransformError,
  MultipartTransformStream,
  parseMultipartBoundary,
} from "./lib/multipart.js";
import { Par3 } from "./lib/mod.js";

const wasmReady = Promise.resolve();

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.name = "HttpError";
    this.status = status;
  }
}

function badRequest(message) {
  return new HttpError(400, message);
}

function settlePromise(promise) {
  return promise.then(
    () => undefined,
    () => undefined,
  );
}

function resolveLockedLayout(request) {
  const layout = Par3.resolveLayoutFromSearchParams(new URL(request.url).searchParams, {
    createError: badRequest,
  });

  if (!layout.slotCountLocked) {
    throw badRequest("query parameters must declare r or total_shard_count");
  }

  return layout;
}

function createRepairPipeline(request, layout) {
  const codec = new Par3(layout, { createError: badRequest });
  const requestBoundary = parseMultipartBoundary(request.headers.get("content-type"));
  const responseBoundary = `par3-${crypto.randomUUID()}`;
  const ingressAbort = new AbortController();
  const stream = MultipartTransformStream.create({
    codec,
    createError(status, message) {
      return new HttpError(status, message);
    },
    missingIndices: layout.requestedMissingIndices,
    requestBoundary,
    responseBoundary,
    stopInput(reason) {
      ingressAbort.abort(reason);
    },
  });

  return {
    responseBoundary,
    responseBody: request.body.pipeThrough(stream, { signal: ingressAbort.signal }),
    stream,
  };
}

function errorResponse(error) {
  if (error instanceof HttpError || error instanceof MultipartTransformError) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: error.status,
      headers: { "content-type": "application/json" },
    });
  }

  if (error instanceof MultipartParseError) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  console.error(error);
  return new Response(JSON.stringify({ error: "internal server error" }), {
    status: 500,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(request, env, ctx) {
    void env;

    if (request.method !== "POST") {
      return new Response(null, {
        status: 405,
        headers: { allow: "POST" },
      });
    }

    let layout;
    try {
      layout = resolveLockedLayout(request);
    } catch (error) {
      return errorResponse(error);
    }

    if (!request.body) {
      return errorResponse(new HttpError(400, "request body is required"));
    }

    try {
      await wasmReady;

      const pipeline = createRepairPipeline(request, layout);
      const outcomePromise = pipeline.stream.outcome;

      if (typeof ctx?.waitUntil === "function") {
        ctx.waitUntil(settlePromise(outcomePromise));
      }

      const outcome = await outcomePromise;
      if (outcome.nothingToRepair) {
        return new Response(null, { status: 204 });
      }

      return new Response(pipeline.responseBody, {
        status: 200,
        headers: {
          "cache-control": "no-store",
          "content-type": `multipart/form-data; boundary=${pipeline.responseBoundary}`,
          "x-par3-repaired-count": String(outcome.repairedCount),
        },
      });
    } catch (error) {
      return errorResponse(error);
    }
  },
};
