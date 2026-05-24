/**
 * Cloudflare Worker entrypoint for streamed PAR-style encode and repair.
 *
 * Request contract:
 * - `POST /` ingests original shards and streams generated recovery shards back.
 * - `PATCH /` ingests present shards and streams repaired outputs back.
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
  constructor(status, message, headers = {}) {
    super(message);
    this.name = "HttpError";
    this.headers = headers;
    this.status = status;
  }
}

function badRequest(message) {
  return new HttpError(400, message);
}

function resolveLockedLayout(searchParams) {
  const layout = Par3.resolveLayoutFromSearchParams(searchParams, {
    createError: badRequest,
  });

  if (!layout.slotCountLocked) {
    throw badRequest("query parameters must declare r or total_shard_count");
  }

  return layout;
}

function resolveOperation(request) {
  const requestUrl = new URL(request.url);

  if (requestUrl.pathname !== "/") {
    throw new HttpError(404, `unsupported path ${requestUrl.pathname}`);
  }

  // Keep the resource path fixed. HTTP verbs select encode or repair, which keeps proxies and
  // clients on one REST-style endpoint without path-specific stream handling.
  if (request.method === "POST") {
    return "encode";
  }

  if (request.method === "PATCH") {
    return "repair";
  }

  throw new HttpError(405, `unsupported method ${request.method}`, {
    allow: "POST, PATCH",
  });
}

function errorResponse(error) {
  if (error instanceof HttpError) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: error.status,
      headers: {
        "content-type": "application/json",
        ...error.headers,
      },
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
    const chunkBytes = Uint8Array.from(toUint8Array(chunk));
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

  let written = 0;

  for await (const chunk of part.body) {
    const chunkBytes = Uint8Array.from(toUint8Array(chunk));
    if (written + chunkBytes.byteLength > codec.shardSize) {
      throw new HttpError(
        400,
        `part ${partName} exceeds declared shard_size ${codec.shardSize}`,
      );
    }

    codec.prepareWritableShard(slotIndex).set(chunkBytes, written);
    written += chunkBytes.byteLength;
  }

  if (written !== codec.shardSize) {
    throw new HttpError(
      400,
      `part ${partName} wrote ${written} bytes but shard_size is ${codec.shardSize}`,
    );
  }

  await verifyShardDigest(slotIndex, codec.shardView(slotIndex), slotDigests);
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

async function ingestMultipartRequest({
  request,
  requestBoundary,
  codec,
  digestSlotCount = codec.slotCount,
  thresholdReached = (candidateCodec) => candidateCodec.thresholdReached(),
  thresholdMessage = null,
  validateShardIndex = () => {},
}) {
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
            digestSlotCount * DIGEST_BYTE_LENGTH,
            413,
            `digests exceeds expected size of ${digestSlotCount * DIGEST_BYTE_LENGTH} bytes`,
            `digests must contain exactly ${digestSlotCount} contiguous 32-byte hashes`,
          ),
          digestSlotCount,
        );
      } else {
        sawShardPart = true;
        const slotIndex = resolveShardIndex(partName);
        validateShardIndex(slotIndex, partName);
        await writeShardPart(codec, part, slotIndex, slotDigests);
      }

      if (thresholdReached(codec)) {
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

  if (!thresholdReached(codec)) {
    throw new HttpError(
      422,
      thresholdMessage
        ? thresholdMessage(codec)
        : `repair threshold not reached: received ${codec.receivedIndices.size} shards but require ${codec.originalCount}`,
    );
  }
}

async function writeMultipartResponse(writer, responseBoundary, codec, missingIndices) {
  for (const index of missingIndices) {
    await writer.write(encodeBoundary(responseBoundary));
    const shardBytes = Uint8Array.from(codec.shardView(index));

    const partHeaders = {
      "Content-Disposition": `form-data; name="shard_${index}"; filename="shard_${index}.bin"`,
      "Content-Type": "application/octet-stream",
      "X-Shard-Index": String(index),
    };

    for await (const chunk of encodePart(partHeaders, shardBytes)) {
      await writer.write(chunk);
    }
  }

  await writer.write(encodeClosingBoundary(responseBoundary));
}

async function processMultipartPipeline({
  request,
  requestBoundary,
  responseBoundary,
  codec,
  writable,
  ingestOptions,
  execute,
}) {
  const writer = writable.getWriter();

  try {
    await ingestMultipartRequest({
      request,
      requestBoundary,
      codec,
      ...ingestOptions,
    });

    const outputIndices = await execute(codec);

    await writeMultipartResponse(writer, responseBoundary, codec, outputIndices);
    await writer.close();
  } catch (error) {
    await writer.abort(error).catch(() => undefined);
    throw error;
  } finally {
    writer.releaseLock();
  }
}

async function processRepairPipeline(
  request,
  requestBoundary,
  responseBoundary,
  layout,
  codec,
  writable,
) {
  return processMultipartPipeline({
    request,
    requestBoundary,
    responseBoundary,
    codec,
    writable,
    ingestOptions: {},
    execute: async (candidateCodec) => {
      const missingIndices = responseMissingIndices(candidateCodec, layout.requestedMissingIndices);

      if (missingIndices.length !== 0) {
        await candidateCodec.repair();
      }

      return missingIndices;
    },
  });
}

async function processEncodePipeline(
  request,
  requestBoundary,
  responseBoundary,
  codec,
  writable,
) {
  return processMultipartPipeline({
    request,
    requestBoundary,
    responseBoundary,
    codec,
    writable,
    ingestOptions: {
      digestSlotCount: codec.originalCount,
      thresholdMessage: (candidateCodec) =>
        `encode threshold not reached: received ${candidateCodec.receivedIndices.size} original shards but require ${candidateCodec.originalCount}`,
      validateShardIndex: (slotIndex) => {
        if (slotIndex >= codec.originalCount) {
          throw badRequest(
            `encode only accepts original shard parts below original_count ${codec.originalCount}`,
          );
        }
      },
    },
    execute: (candidateCodec) => candidateCodec.encode(),
  });
}

export default {
  async fetch(request, env, ctx) {
    void env;

    let operation;
    try {
      operation = resolveOperation(request);
    } catch (error) {
      return errorResponse(error);
    }

    const requestUrl = new URL(request.url);
    let layout;
    try {
      layout = resolveLockedLayout(requestUrl.searchParams);
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
      // Tie wasm arena lifetime to the HTTP response, not to the background pump alone. A complete
      // response flush and a client-side cancellation both release codec memory at the stream edge.
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
      // Return headers now. The promise below owns ingress, repair/encode, and multipart egress
      // under waitUntil so the request path stays streaming instead of buffering the body.
      const operationPromise = operation === "encode"
        ? processEncodePipeline(
          request,
          requestBoundary,
          responseBoundary,
          codec,
          writable,
        )
        : processRepairPipeline(
          request,
          requestBoundary,
          responseBoundary,
          layout,
          codec,
          writable,
        );

      if (typeof ctx?.waitUntil === "function") {
        ctx.waitUntil(operationPromise);
      } else {
        void operationPromise.catch(() => undefined);
      }

      return response;
    } catch (error) {
      return errorResponse(error);
    }
  },
};
