/**
 * Cloudflare Worker entrypoint for streamed PAR-style repair.
 *
 * Request contract:
 * - `multipart/form-data` body
 * - first part named `metadata` containing JSON with `original_count`, `shard_size`, and either
 *   `recovery_count` or `total_shard_count`
 * - shard parts named `shard_<slotIndex>` whose bodies are raw shard bytes
 *
 * The worker only returns the shard indexes explicitly requested by `missing_indices`, but the
 * codec must still see every unreceived slot as missing. That distinction is the critical rule for
 * correct repairs when a client intentionally omits unneeded recovery shards from the request.
 */

import { MultipartParseError, parseMultipartRequest } from "@mjackson/multipart-parser";
import { Par3 } from "./lib/mod.js";

const encoder = new TextEncoder();
const SHARD_PART_NAME = /^shard_(\d+)$/;
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

/**
 * Parse and validate the metadata part.
 *
 * The worker accepts either an explicit `total_shard_count` or the more common `recovery_count`.
 * When neither is present, the slot count is inferred from the largest requested missing index so
 * the request can still stream recovery shards with higher indexes later on.
 */
function parseMetadata(text) {
  let parsed;

  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new HttpError(400, `metadata is not valid JSON: ${error.message}`);
  }

  const createError = badRequest;
  const layout = Par3.resolveLayout({
    originalCount: parsed.original_count,
    recoveryCount: parsed.recovery_count,
    requestedMissingIndices: parsed.missing_indices,
    shardSize: parsed.shard_size,
    totalShardCount: parsed.total_shard_count,
    createError,
  });

  return new Par3(layout, { createError });
}

function createState() {
  return {
    codec: null,
  };
}

function currentMissingIndices(state) {
  return state.codec.currentRequestedMissingIndices();
}

function freeArena(state) {
  if (!state.codec) {
    return;
  }

  state.codec.free();
}

/**
 * Grow the shard arena when the request introduces a higher slot index than currently allocated.
 *
 * This is only legal when the client did not lock the layout with `recovery_count` or
 * `total_shard_count`. Existing bytes are copied into the expanded arena before the old allocation
 * is released.
 */
function ensureArenaCapacity(state, requiredSlotCount) {
  if (!state.codec) {
    throw new HttpError(400, "metadata part must arrive before shard parts");
  }

  state.codec.ensureCapacity(requiredSlotCount);
}

async function drainReadableStream(stream) {
  const reader = stream.getReader();
  try {
    while (true) {
      const { done } = await reader.read();
      if (done) {
        break;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function writeShardPart(part, slotIndex, state) {
  ensureArenaCapacity(state, slotIndex + 1);

  if (state.codec.receivedIndices.has(slotIndex)) {
    throw new HttpError(409, `duplicate shard part received for slot ${slotIndex}`);
  }

  const target = state.codec.prepareWritableShard(slotIndex);
  let written = 0;
  const reader = part.body.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      if (written + value.byteLength > state.codec.shardSize) {
        throw new HttpError(
          400,
          `part ${part.name} exceeds declared shard_size ${state.codec.shardSize}`,
        );
      }

      target.set(value, written);
      written += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }

  if (written !== state.codec.shardSize) {
    throw new HttpError(
      400,
      `part ${part.name} wrote ${written} bytes but shard_size is ${state.codec.shardSize}`,
    );
  }

  state.codec.commitShard(slotIndex);
}

function thresholdReached(state) {
  return Boolean(state.codec) && state.codec.thresholdReached();
}

/**
 * Stream the repaired shard bytes back as multipart/form-data.
 *
 * Each shard is copied out of wasm memory before being enqueued so the response stays valid even
 * after the underlying arena is released.
 */
function createMultipartResponse(state, missingIndices) {
  const boundary = `par3-${crypto.randomUUID()}`;
  let freed = false;

  const releaseArena = () => {
    if (freed) {
      return;
    }

    freed = true;
    freeArena(state);
  };

  const body = new ReadableStream({
    async start(controller) {
      try {
        for (const index of missingIndices) {
          controller.enqueue(
            encoder.encode(
              `--${boundary}\r\n` +
              `Content-Disposition: form-data; name="shard_${index}"; filename="shard_${index}.bin"\r\n` +
              `Content-Type: application/octet-stream\r\n` +
              `X-Shard-Index: ${index}\r\n\r\n`,
            ),
          );

          controller.enqueue(state.codec.readShard(index));
          controller.enqueue(encoder.encode("\r\n"));
        }

        controller.enqueue(encoder.encode(`--${boundary}--\r\n`));
        controller.close();
      } finally {
        releaseArena();
      }
    },
    cancel() {
      releaseArena();
    },
  });

  return new Response(body, {
    status: 200,
    headers: {
      "cache-control": "no-store",
      "content-type": `multipart/form-data; boundary=${boundary}`,
      "x-par3-repaired-count": String(missingIndices.length),
    },
  });
}

/**
 * Read the multipart request until enough shards have arrived to repair the missing set.
 *
 * Once the repair threshold is met the worker stops reading additional parts. This keeps the
 * request path streaming-friendly and avoids buffering extra shard bodies that are not required for
 * reconstruction.
 */
async function ingestRequest(request) {
  const state = createState();

  try {
    for await (const part of parseMultipartRequest(request)) {
      if (part.name === "metadata") {
        if (state.codec) {
          throw new HttpError(400, "metadata part may only appear once");
        }

        state.codec = parseMetadata(await part.text());
        ensureArenaCapacity(state, state.codec.slotCount);
      } else {
        const match = SHARD_PART_NAME.exec(part.name);
        if (!match) {
          await drainReadableStream(part.body);
        } else {
          const slotIndex = Par3.assertInteger(Number.parseInt(match[1], 10), part.name, 0, badRequest);
          await writeShardPart(part, slotIndex, state);
        }
      }

      if (thresholdReached(state)) {
        break;
      }
    }

    if (!state.codec) {
      throw new HttpError(400, "metadata part is required");
    }

    const responseMissingIndices = currentMissingIndices(state);
    if (responseMissingIndices.length === 0) {
      freeArena(state);
      return { nothingToRepair: true };
    }

    if (!thresholdReached(state)) {
      throw new HttpError(
        422,
        `repair threshold not reached: received ${state.codec.receivedIndices.size} shards but require ${state.codec.originalCount}`,
      );
    }

    state.codec.repair();

    return { state, missingIndices: responseMissingIndices };
  } catch (error) {
    freeArena(state);
    throw error;
  }
}

function errorResponse(error) {
  if (error instanceof HttpError) {
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
    void ctx;

    if (request.method !== "POST") {
      return new Response(null, {
        status: 405,
        headers: { allow: "POST" },
      });
    }

    if (!request.body) {
      return errorResponse(new HttpError(400, "request body is required"));
    }

    try {
      await wasmReady;
      const outcome = await ingestRequest(request);
      if (outcome.nothingToRepair) {
        return new Response(null, { status: 204 });
      }

      return createMultipartResponse(outcome.state, outcome.missingIndices);
    } catch (error) {
      return errorResponse(error);
    }
  },
};
