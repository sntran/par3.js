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

import { MultipartParseError, MultipartStreamReader, parseMultipartBoundary } from "./lib/multipart.js";
import { Par3 } from "./lib/mod.js";

const encoder = new TextEncoder();
const MAX_METADATA_BYTES = 64 * 1024;
const SHA256_HEX_PATTERN = /^[0-9a-fA-F]{64}$/;
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

function toHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function readTextPart(parser, { fieldName, maxBytes } = {}) {
  const textDecoder = new TextDecoder();
  let text = "";
  let totalBytes = 0;

  const hasNextPart = await parser.readPartBody(async (chunk) => {
    totalBytes += chunk.byteLength;
    if (totalBytes > maxBytes) {
      throw new HttpError(413, `${fieldName} exceeds maximum size of ${maxBytes} bytes`);
    }

    text += textDecoder.decode(chunk, { stream: true });
  });

  return {
    hasNextPart,
    text: text + textDecoder.decode(),
  };
}

function parseDigests(rawDigests, slotCount) {
  if (rawDigests === undefined) {
    return null;
  }

  if (!Array.isArray(rawDigests)) {
    throw badRequest("digests must be an array of SHA-256 hex strings or null values");
  }

  if (rawDigests.length !== slotCount) {
    throw badRequest(`digests must contain exactly ${slotCount} entries`);
  }

  return rawDigests.map((digest, index) => {
    if (digest === null) {
      return null;
    }

    if (typeof digest !== "string") {
      throw badRequest(
        `digests[${index}] must be a 64-character hexadecimal SHA-256 digest or null`,
      );
    }

    if (!SHA256_HEX_PATTERN.test(digest)) {
      throw badRequest(
        `digests[${index}] must be a 64-character hexadecimal SHA-256 digest or null`,
      );
    }

    return digest.toLowerCase();
  });
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

  if (!layout.slotCountLocked) {
    throw badRequest("metadata must declare recovery_count or total_shard_count");
  }

  return {
    codec: new Par3(layout, { createError }),
    slotDigests: parseDigests(parsed.digests, layout.slotCount),
  };
}

function createState() {
  return {
    codec: null,
    slotDigests: null,
  };
}

function currentMissingIndices(state) {
  if (state.codec.currentRequestedMissingIndices().length === 0) {
    return [];
  }

  return state.codec.selectOutputIndices();
}

function freeArena(state) {
  if (!state.codec) {
    return;
  }

  state.codec.free();
  state.codec = null;
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

async function verifyShardDigest(slotIndex, bytes, state) {
  const expectedDigest = state.slotDigests?.[slotIndex] ?? null;
  if (!expectedDigest) {
    return;
  }

  const digestBytes = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  if (toHex(digestBytes) !== expectedDigest) {
    throw new HttpError(422, `digest mismatch for slot ${slotIndex}`);
  }
}

async function skipPart(parser) {
  return parser.readPartBody(async () => {});
}

async function writeShardPart(parser, partName, slotIndex, state) {
  ensureArenaCapacity(state, slotIndex + 1);

  if (state.codec.receivedIndices.has(slotIndex)) {
    throw new HttpError(409, `duplicate shard part received for slot ${slotIndex}`);
  }

  const target = state.codec.prepareWritableShard(slotIndex);
  let written = 0;

  const hasNextPart = await parser.readPartBody(async (chunk) => {
    if (written + chunk.byteLength > state.codec.shardSize) {
      throw new HttpError(
        400,
        `part ${partName} exceeds declared shard_size ${state.codec.shardSize}`,
      );
    }

    target.set(chunk, written);
    written += chunk.byteLength;
  });

  if (written !== state.codec.shardSize) {
    throw new HttpError(
      400,
      `part ${partName} wrote ${written} bytes but shard_size is ${state.codec.shardSize}`,
    );
  }

  await verifyShardDigest(slotIndex, target, state);
  state.codec.commitShard(slotIndex);
  return hasNextPart;
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
  let nextIndex = 0;

  const body = new ReadableStream({
    pull(controller) {
      try {
        if (nextIndex >= missingIndices.length) {
          controller.enqueue(encoder.encode(`--${boundary}--\r\n`));
          controller.close();
          freeArena(state);
          return;
        }

        const index = missingIndices[nextIndex];
        nextIndex += 1;

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
      } catch (error) {
        freeArena(state);
        throw error;
      }
    },
    cancel() {
      freeArena(state);
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
  const parser = new MultipartStreamReader(
    request.body,
    parseMultipartBoundary(request.headers.get("content-type")),
  );

  try {
    let hasNextPart = await parser.start();
    while (hasNextPart) {
      const part = await parser.readHeaders();

      if (part.name === "metadata") {
        if (state.codec) {
          throw new HttpError(400, "metadata part may only appear once");
        }

        const metadataText = await readTextPart(parser, {
          fieldName: "metadata",
          maxBytes: MAX_METADATA_BYTES,
        });
        const metadata = parseMetadata(metadataText.text);
        state.codec = metadata.codec;
        state.slotDigests = metadata.slotDigests;
        ensureArenaCapacity(state, state.codec.slotCount);
        hasNextPart = metadataText.hasNextPart;
      } else {
        const match = SHARD_PART_NAME.exec(part.name);
        if (!match) {
          hasNextPart = await skipPart(parser);
        } else {
          const slotIndex = Par3.assertInteger(Number.parseInt(match[1], 10), part.name, 0, badRequest);
          hasNextPart = await writeShardPart(parser, part.name, slotIndex, state);
        }
      }

      if (thresholdReached(state)) {
        await parser.cancel("repair threshold reached");
        break;
      }
    }

    if (!state.codec) {
      throw new HttpError(400, "metadata part is required");
    }

    const responseMissingIndices = currentMissingIndices(state);
    if (responseMissingIndices.length === 0) {
      freeArena(state);
      parser.release();
      return { nothingToRepair: true };
    }

    if (!thresholdReached(state)) {
      throw new HttpError(
        422,
        `repair threshold not reached: received ${state.codec.receivedIndices.size} shards but require ${state.codec.originalCount}`,
      );
    }

    state.codec.repair();

    parser.release();
    return { state, missingIndices: responseMissingIndices };
  } catch (error) {
    await parser.cancel("request ingestion failed");
    freeArena(state);
    parser.release();
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
