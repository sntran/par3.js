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
  encodeBoundary,
  encodeClosingBoundary,
  encodePart,
  getMultipartPartName,
  MultipartDecoder,
  MultipartParseError,
  parseMultipartBoundary,
} from "./lib/multipart.js";
import { Par3 } from "./lib/mod.js";

const DIGEST_BYTE_LENGTH = 32;
const DIGEST_PART_NAME = "digests";
const SHARD_PART_NAME = /^shard_(\d+)$/u;
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

function resolveLockedLayout(request) {
  const layout = Par3.resolveLayoutFromSearchParams(new URL(request.url).searchParams, {
    createError: badRequest,
  });

  if (!layout.slotCountLocked) {
    throw badRequest("query parameters must declare r or total_shard_count");
  }

  return layout;
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

function toUint8Array(bytes) {
  if (bytes instanceof Uint8Array) {
    return bytes;
  }

  if (ArrayBuffer.isView(bytes)) {
    return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  if (bytes instanceof ArrayBuffer) {
    return new Uint8Array(bytes);
  }

  return new Uint8Array(bytes);
}

function isZeroDigest(bytes) {
  for (const byte of bytes) {
    if (byte !== 0) {
      return false;
    }
  }

  return true;
}

function parseDigestBlob(bytes, slotCount) {
  return Array.from({ length: slotCount }, (_, slotIndex) => {
    const start = slotIndex * DIGEST_BYTE_LENGTH;
    const digest = bytes.subarray(start, start + DIGEST_BYTE_LENGTH);
    return isZeroDigest(digest) ? null : digest;
  });
}

function bytesEqual(left, right) {
  if (left.byteLength !== right.byteLength) {
    return false;
  }

  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }

  return true;
}

async function verifyShardDigest(slotIndex, bytes, slotDigests) {
  const expectedDigest = slotDigests?.[slotIndex] ?? null;
  if (!expectedDigest) {
    return;
  }

  const digestBytes = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  if (!bytesEqual(digestBytes, expectedDigest)) {
    throw new HttpError(422, `digest mismatch for slot ${slotIndex}`);
  }
}

function responseMissingIndices(codec, requestedMissingIndices) {
  if (requestedMissingIndices.length === 0) {
    return [];
  }

  if (codec.currentRequestedMissingIndices().length === 0) {
    return [];
  }

  return codec.selectOutputIndices();
}

function normalizeIngressError(error) {
  if (error instanceof HttpError || error instanceof MultipartParseError) {
    return error;
  }

  if (error instanceof Error) {
    return new MultipartParseError(error.message);
  }

  return new MultipartParseError(String(error));
}

async function readPartBytes(part, expectedByteLength, overflowStatus, overflowMessage, shortMessage) {
  const bytes = new Uint8Array(expectedByteLength);
  let written = 0;

  for await (const chunk of part.body) {
    const chunkBytes = toUint8Array(chunk);
    if (written + chunkBytes.byteLength > expectedByteLength) {
      throw new HttpError(overflowStatus, overflowMessage);
    }

    bytes.set(chunkBytes, written);
    written += chunkBytes.byteLength;
  }

  if (written !== expectedByteLength) {
    throw new HttpError(400, shortMessage);
  }

  return bytes;
}

async function writeShardPart(codec, part, slotIndex, slotDigests) {
  const partName = getMultipartPartName(part.headers);
  if (codec.receivedIndices.has(slotIndex)) {
    throw new HttpError(409, `duplicate shard part received for slot ${slotIndex}`);
  }

  const target = codec.prepareWritableShard(slotIndex);
  let written = 0;

  for await (const chunk of part.body) {
    const chunkBytes = toUint8Array(chunk);
    if (written + chunkBytes.byteLength > codec.shardSize) {
      throw new HttpError(
        400,
        `part ${partName} exceeds declared shard_size ${codec.shardSize}`,
      );
    }

    target.set(chunkBytes, written);
    written += chunkBytes.byteLength;
  }

  if (written !== codec.shardSize) {
    throw new HttpError(
      400,
      `part ${partName} wrote ${written} bytes but shard_size is ${codec.shardSize}`,
    );
  }

  await verifyShardDigest(slotIndex, target, slotDigests);
  codec.commitShard(slotIndex);
}

function resolveShardIndex(partName) {
  const match = SHARD_PART_NAME.exec(partName);
  if (!match) {
    throw badRequest(
      `unexpected multipart part ${partName}; only digests and shard_<slotIndex> are allowed`,
    );
  }

  return Par3.assertInteger(
    Number.parseInt(match[1], 10),
    partName,
    0,
    (message) => new HttpError(400, message),
  );
}

async function ingestMultipartRequest({ request, requestBoundary, codec, requestedMissingIndices }) {
  const decodedStream = request.body.pipeThrough(MultipartDecoder.create({ boundary: requestBoundary }));
  let sawDigestsPart = false;
  let sawShardPart = false;
  let slotDigests = null;
  let shouldCancel = false;

  try {
    for await (const part of decodedStream.values({ preventCancel: true })) {
      const partName = getMultipartPartName(part.headers);

      if (partName === DIGEST_PART_NAME) {
        if (sawDigestsPart) {
          throw badRequest("digests part may only appear once");
        }

        if (sawShardPart) {
          throw badRequest("digests part must arrive before shard parts");
        }

        sawDigestsPart = true;
        slotDigests = parseDigestBlob(
          await readPartBytes(
            part,
            codec.slotCount * DIGEST_BYTE_LENGTH,
            413,
            `digests exceeds expected size of ${codec.slotCount * DIGEST_BYTE_LENGTH} bytes`,
            `digests must contain exactly ${codec.slotCount} contiguous 32-byte hashes`,
          ),
          codec.slotCount,
        );
      } else {
        sawShardPart = true;
        await writeShardPart(codec, part, resolveShardIndex(partName), slotDigests);
      }

      if (codec.thresholdReached()) {
        shouldCancel = true;
        break;
      }
    }
  } catch (error) {
    throw normalizeIngressError(error);
  }

  if (shouldCancel) {
    void decodedStream.cancel("repair threshold reached");
  }

  if (!codec.thresholdReached()) {
    throw new HttpError(
      422,
      `repair threshold not reached: received ${codec.receivedIndices.size} shards but require ${codec.originalCount}`,
    );
  }

  return responseMissingIndices(codec, requestedMissingIndices);
}

async function writeMultipartResponse(writer, responseBoundary, codec, missingIndices) {
  for (const index of missingIndices) {
    await writer.write(encodeBoundary(responseBoundary));

    const partHeaders = {
      "Content-Disposition": `form-data; name="shard_${index}"; filename="shard_${index}.bin"`,
      "Content-Type": "application/octet-stream",
      "X-Shard-Index": String(index),
    };

    for await (const chunk of encodePart(partHeaders, codec.shardView(index))) {
      await writer.write(chunk);
    }
  }

  await writer.write(encodeClosingBoundary(responseBoundary));
}

async function processRepairPipeline(
  request,
  requestBoundary,
  responseBoundary,
  layout,
  codec,
  writable,
) {
  const writer = writable.getWriter();

  try {
    const missingIndices = await ingestMultipartRequest({
      request,
      requestBoundary,
      codec,
      requestedMissingIndices: layout.requestedMissingIndices,
    });

    if (missingIndices.length !== 0) {
      codec.repair();
    }

    await writeMultipartResponse(writer, responseBoundary, codec, missingIndices);
    await writer.close();
  } catch (error) {
    await writer.abort(error).catch(() => undefined);
    throw error;
  } finally {
    writer.releaseLock();
  }
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

      const requestBoundary = parseMultipartBoundary(request.headers.get("content-type"));
      const responseBoundary = `par3-${crypto.randomUUID()}`;
      const codec = new Par3(layout, { createError: badRequest });
      const { readable, writable } = new TransformStream();
      const cleanupTransform = new TransformStream({
        flush() {
          codec.free();
        },
        cancel() {
          codec.free();
        },
      });
      const response = new Response(readable.pipeThrough(cleanupTransform), {
        status: 200,
        headers: {
          "cache-control": "no-store",
          "content-type": `multipart/form-data; boundary=${responseBoundary}`,
        },
      });
      const backgroundPromise = processRepairPipeline(
        request,
        requestBoundary,
        responseBoundary,
        layout,
        codec,
        writable,
      );

      if (typeof ctx?.waitUntil === "function") {
        ctx.waitUntil(backgroundPromise);
      } else {
        void backgroundPromise.catch(() => undefined);
      }

      return response;
    } catch (error) {
      return errorResponse(error);
    }
  },
};
