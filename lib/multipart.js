import { Par3 } from "./mod.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CRLF = encoder.encode("\r\n");
const DOUBLE_CRLF = encoder.encode("\r\n\r\n");
const DASH_DASH = encoder.encode("--");
const DIGEST_BYTE_LENGTH = 32;
const DIGEST_PART_NAME = "digests";
const SHARD_PART_NAME = /^shard_(\d+)$/u;

export const MAX_PART_HEADER_BYTES = 8 * 1024;

export class MultipartParseError extends Error {
  constructor(message) {
    super(message);
    this.name = "MultipartParseError";
  }
}

export class MultipartTransformError extends Error {
  constructor(status, message) {
    super(message);
    this.name = "MultipartTransformError";
    this.status = status;
  }
}

function invalidMultipart(message) {
  return new MultipartParseError(message);
}

function defaultCreateError(status, message) {
  return new MultipartTransformError(status, message);
}

function appendBytes(left, right) {
  if (left.byteLength === 0) {
    return right;
  }

  const merged = new Uint8Array(left.byteLength + right.byteLength);
  merged.set(left);
  merged.set(right, left.byteLength);
  return merged;
}

function startsWithBytes(bytes, prefix) {
  if (bytes.byteLength < prefix.byteLength) {
    return false;
  }

  for (let index = 0; index < prefix.byteLength; index += 1) {
    if (bytes[index] !== prefix[index]) {
      return false;
    }
  }

  return true;
}

function indexOfBytes(bytes, needle) {
  const limit = bytes.byteLength - needle.byteLength;
  for (let start = 0; start <= limit; start += 1) {
    let matches = true;
    for (let offset = 0; offset < needle.byteLength; offset += 1) {
      if (bytes[start + offset] !== needle[offset]) {
        matches = false;
        break;
      }
    }

    if (matches) {
      return start;
    }
  }

  return -1;
}

function isQuotedValue(value) {
  return /^".*"$/u.test(value);
}

export function parseMultipartBoundary(contentType) {
  if (!contentType) {
    throw invalidMultipart("content-type must be multipart/form-data with a boundary");
  }

  const [rawType, ...rawParameters] = contentType.split(";");
  if (rawType.trim().toLowerCase() !== "multipart/form-data") {
    throw invalidMultipart("content-type must be multipart/form-data with a boundary");
  }

  const boundaryParameter = rawParameters.find((parameter) => parameter.trim().toLowerCase().startsWith("boundary="));
  if (!boundaryParameter) {
    throw invalidMultipart("content-type must be multipart/form-data with a boundary");
  }

  let boundary = boundaryParameter.slice(boundaryParameter.indexOf("=") + 1).trim();
  if (isQuotedValue(boundary)) {
    boundary = boundary.slice(1, -1);
  }

  if (!boundary) {
    throw invalidMultipart("content-type must be multipart/form-data with a boundary");
  }

  return boundary;
}

function parseHeaderParameters(value) {
  const [rawType, ...rawParameters] = value.split(";");
  const parameters = new Map();

  for (const rawParameter of rawParameters) {
    const trimmed = rawParameter.trim();
    const separator = trimmed.indexOf("=");
    if (separator <= 0) {
      continue;
    }

    const key = trimmed.slice(0, separator).trim().toLowerCase();
    let parameterValue = trimmed.slice(separator + 1).trim();
    if (isQuotedValue(parameterValue)) {
      parameterValue = parameterValue.slice(1, -1);
    }

    parameters.set(key, parameterValue);
  }

  return {
    type: rawType.trim().toLowerCase(),
    parameters,
  };
}

function parsePartHeaders(rawHeaders) {
  const headers = new Map();

  for (const line of rawHeaders.split("\r\n")) {
    const separator = line.indexOf(":");
    if (separator <= 0) {
      throw invalidMultipart(`multipart header is malformed: ${line}`);
    }

    headers.set(line.slice(0, separator).trim().toLowerCase(), line.slice(separator + 1).trim());
  }

  const contentDisposition = headers.get("content-disposition");
  if (!contentDisposition) {
    throw invalidMultipart("multipart part is missing content-disposition");
  }

  const parsedContentDisposition = parseHeaderParameters(contentDisposition);
  if (parsedContentDisposition.type !== "form-data") {
    throw invalidMultipart("multipart content-disposition must be form-data");
  }

  const name = parsedContentDisposition.parameters.get("name");
  if (!name) {
    throw invalidMultipart("multipart part is missing a name");
  }

  return {
    headers,
    name,
  };
}

function encodeShardResponseHeader(boundary, index) {
  return encoder.encode(
    `--${boundary}\r\n`
    + `Content-Disposition: form-data; name="shard_${index}"; filename="shard_${index}.bin"\r\n`
    + "Content-Type: application/octet-stream\r\n"
    + `X-Shard-Index: ${index}\r\n\r\n`,
  );
}

function encodeClosingBoundary(boundary) {
  return encoder.encode(`--${boundary}--\r\n`);
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

async function verifyShardDigest(slotIndex, bytes, slotDigests, createError) {
  const expectedDigest = slotDigests?.[slotIndex] ?? null;
  if (!expectedDigest) {
    return;
  }

  const digestBytes = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  if (!bytesEqual(digestBytes, expectedDigest)) {
    throw createError(422, `digest mismatch for slot ${slotIndex}`);
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

function createMultipartResponseReadable({ codec, outcomePromise, responseBoundary }) {
  let outcome = null;
  let partIndex = 0;
  let phase = "header";

  async function awaitOutcome(controller) {
    if (outcome) {
      return outcome;
    }

    try {
      outcome = await outcomePromise;
      return outcome;
    } catch (error) {
      codec.free();
      controller.error(error);
      return null;
    }
  }

  return new ReadableStream({
    async pull(controller) {
      try {
        const resolvedOutcome = await awaitOutcome(controller);
        if (!resolvedOutcome) {
          return;
        }

        if (resolvedOutcome.nothingToRepair) {
          codec.free();
          controller.close();
          return;
        }

        const missingIndices = resolvedOutcome.missingIndices;
        if (partIndex >= missingIndices.length) {
          if (phase === "closing") {
            controller.enqueue(encodeClosingBoundary(responseBoundary));
            phase = "done";
            return;
          }

          codec.free();
          controller.close();
          return;
        }

        const index = missingIndices[partIndex];
        if (phase === "header") {
          controller.enqueue(encodeShardResponseHeader(responseBoundary, index));
          phase = "body";
          return;
        }

        if (phase === "body") {
          controller.enqueue(codec.shardView(index));
          phase = "footer";
          return;
        }

        controller.enqueue(CRLF);
        partIndex += 1;
        phase = partIndex >= missingIndices.length ? "closing" : "header";
      } catch (error) {
        codec.free();
        controller.error(error);
      }
    },
    cancel() {
      codec.free();
    },
  }, { highWaterMark: 0 });
}

function createIngressDriver({
  codec,
  requestBoundary,
  requestedMissingIndices,
  createError,
  maxPartHeaderBytes,
  resolveOutcome,
  rejectOutcome,
  stopInput,
}) {
  let buffer = new Uint8Array(0);
  let currentPart = null;
  let inputClosed = false;
  let phase = "start";
  let sawDigestsPart = false;
  let sawShardPart = false;
  let slotDigests = null;
  let settled = false;
  let terminated = false;

  const initialBoundary = encoder.encode(`--${requestBoundary}`);
  const partBoundary = encoder.encode(`\r\n--${requestBoundary}`);

  const settleOutcome = (outcome) => {
    settled = true;
    resolveOutcome(outcome);
  };

  const reject = (error) => {
    settled = true;
    rejectOutcome(error);
  };

  const consume = (byteLength) => {
    const chunk = buffer.subarray(0, byteLength);
    buffer = buffer.subarray(byteLength);
    return chunk;
  };

  const requireBytes = (byteLength, message) => {
    if (buffer.byteLength >= byteLength) {
      return true;
    }

    if (inputClosed) {
      throw invalidMultipart(message);
    }

    return false;
  };

  const openPart = (part) => {
    if (part.name === DIGEST_PART_NAME) {
      if (sawDigestsPart) {
        throw createError(400, "digests part may only appear once");
      }

      if (sawShardPart) {
        throw createError(400, "digests part must arrive before shard parts");
      }

      sawDigestsPart = true;
      currentPart = {
        kind: "digests",
        expectedByteLength: codec.slotCount * DIGEST_BYTE_LENGTH,
        bytes: new Uint8Array(codec.slotCount * DIGEST_BYTE_LENGTH),
        written: 0,
      };
      return;
    }

    const match = SHARD_PART_NAME.exec(part.name);
    if (!match) {
      throw createError(
        400,
        `unexpected multipart part ${part.name}; only digests and shard_<slotIndex> are allowed`,
      );
    }

    sawShardPart = true;
    const slotIndex = Par3.assertInteger(
      Number.parseInt(match[1], 10),
      part.name,
      0,
      (message) => createError(400, message),
    );
    codec.ensureCapacity(slotIndex + 1);

    if (codec.receivedIndices.has(slotIndex)) {
      throw createError(409, `duplicate shard part received for slot ${slotIndex}`);
    }

    currentPart = {
      kind: "shard",
      name: part.name,
      slotIndex,
      target: codec.prepareWritableShard(slotIndex),
      written: 0,
    };
  };

  const writeCurrentPartChunk = async (chunk) => {
    if (currentPart.kind === "digests") {
      if (currentPart.written + chunk.byteLength > currentPart.expectedByteLength) {
        throw createError(
          413,
          `digests exceeds expected size of ${currentPart.expectedByteLength} bytes`,
        );
      }

      currentPart.bytes.set(chunk, currentPart.written);
      currentPart.written += chunk.byteLength;
      return;
    }

    if (currentPart.written + chunk.byteLength > codec.shardSize) {
      throw createError(
        400,
        `part ${currentPart.name} exceeds declared shard_size ${codec.shardSize}`,
      );
    }

    currentPart.target.set(chunk, currentPart.written);
    currentPart.written += chunk.byteLength;
  };

  const finalizeCurrentPart = async () => {
    if (currentPart.kind === "digests") {
      if (currentPart.written !== currentPart.expectedByteLength) {
        throw createError(
          400,
          `digests must contain exactly ${codec.slotCount} contiguous 32-byte hashes`,
        );
      }

      slotDigests = parseDigestBlob(currentPart.bytes, codec.slotCount);
      currentPart = null;
      return;
    }

    if (currentPart.written !== codec.shardSize) {
      throw createError(
        400,
        `part ${currentPart.name} wrote ${currentPart.written} bytes but shard_size is ${codec.shardSize}`,
      );
    }

    await verifyShardDigest(currentPart.slotIndex, currentPart.target, slotDigests, createError);
    codec.commitShard(currentPart.slotIndex);
    currentPart = null;
  };

  const tryConsumeInitialBoundary = () => {
    if (!requireBytes(initialBoundary.byteLength, "multipart body terminated before the first boundary")) {
      return false;
    }

    if (!startsWithBytes(buffer, initialBoundary)) {
      throw invalidMultipart("multipart body does not start with the declared boundary");
    }

    const suffix = buffer.subarray(initialBoundary.byteLength);
    if (startsWithBytes(suffix, DASH_DASH)) {
      const trailing = suffix.subarray(DASH_DASH.byteLength);
      if (trailing.byteLength === 0 && !inputClosed) {
        return false;
      }

      if (trailing.byteLength > 0 && !startsWithBytes(trailing, CRLF)) {
        throw invalidMultipart("multipart boundary must end with CRLF");
      }

      consume(initialBoundary.byteLength + DASH_DASH.byteLength);
      if (startsWithBytes(buffer, CRLF)) {
        consume(CRLF.byteLength);
      }
      phase = "done";
      return true;
    }

    if (!requireBytes(initialBoundary.byteLength + CRLF.byteLength, "multipart body terminated before the first boundary")) {
      return false;
    }

    if (!startsWithBytes(suffix, CRLF)) {
      throw invalidMultipart("multipart boundary must end with CRLF");
    }

    consume(initialBoundary.byteLength + CRLF.byteLength);
    phase = "headers";
    return true;
  };

  const tryReadHeaders = () => {
    const delimiterIndex = indexOfBytes(buffer, DOUBLE_CRLF);
    if (delimiterIndex !== -1) {
      if (delimiterIndex > maxPartHeaderBytes) {
        throw invalidMultipart(`multipart headers exceed maximum size of ${maxPartHeaderBytes} bytes`);
      }

      const headerBytes = consume(delimiterIndex);
      consume(DOUBLE_CRLF.byteLength);
      openPart(parsePartHeaders(decoder.decode(headerBytes)));
      phase = "body";
      return true;
    }

    if (buffer.byteLength > maxPartHeaderBytes) {
      throw invalidMultipart(`multipart headers exceed maximum size of ${maxPartHeaderBytes} bytes`);
    }

    if (inputClosed) {
      throw invalidMultipart("multipart body terminated in part headers");
    }

    return false;
  };

  const transitionToEmission = async () => {
    const missingIndices = responseMissingIndices(codec, requestedMissingIndices);
    if (missingIndices.length === 0) {
      codec.free();
      settleOutcome({
        missingIndices: [],
        nothingToRepair: true,
        repairedCount: 0,
      });
    } else {
      codec.repair();
      settleOutcome({
        missingIndices,
        nothingToRepair: false,
        repairedCount: missingIndices.length,
      });
    }

    stopInput("repair threshold reached");
    terminated = true;
    phase = "terminated";
  };

  const tryReadPartBody = async () => {
    const boundaryIndex = indexOfBytes(buffer, partBoundary);
    if (boundaryIndex !== -1) {
      if (!requireBytes(
        boundaryIndex + partBoundary.byteLength + 2,
        "multipart body terminated after a part boundary",
      )) {
        return false;
      }

      if (boundaryIndex > 0) {
        await writeCurrentPartChunk(consume(boundaryIndex));
      }

      const trailing = buffer.subarray(partBoundary.byteLength);
      if (startsWithBytes(trailing, DASH_DASH)) {
        const closingSuffix = trailing.subarray(DASH_DASH.byteLength);
        if (closingSuffix.byteLength === 0 && !inputClosed) {
          return false;
        }

        if (closingSuffix.byteLength > 0 && !startsWithBytes(closingSuffix, CRLF)) {
          throw invalidMultipart("multipart boundary must end with CRLF");
        }

        consume(partBoundary.byteLength + DASH_DASH.byteLength);
        if (startsWithBytes(buffer, CRLF)) {
          consume(CRLF.byteLength);
        }
        await finalizeCurrentPart();
        if (codec.thresholdReached()) {
          await transitionToEmission();
          return true;
        }

        phase = "done";
        return true;
      }

      if (!startsWithBytes(trailing, CRLF)) {
        throw invalidMultipart("multipart boundary must end with CRLF");
      }

      consume(partBoundary.byteLength + CRLF.byteLength);
      await finalizeCurrentPart();
      if (codec.thresholdReached()) {
        await transitionToEmission();
        return true;
      }

      phase = "headers";
      return true;
    }

    const safeByteLength = buffer.byteLength - (partBoundary.byteLength - 1);
    if (safeByteLength > 0) {
      await writeCurrentPartChunk(consume(safeByteLength));
      return true;
    }

    if (inputClosed) {
      throw invalidMultipart("multipart body terminated before the next boundary");
    }

    return false;
  };

  const drain = async () => {
    while (!settled && !terminated) {
      if (phase === "start") {
        if (!tryConsumeInitialBoundary()) {
          return;
        }
        continue;
      }

      if (phase === "headers") {
        if (!tryReadHeaders()) {
          return;
        }
        continue;
      }

      if (phase === "body") {
        if (!(await tryReadPartBody())) {
          return;
        }
        continue;
      }

      if (phase === "done") {
        throw createError(
          422,
          `repair threshold not reached: received ${codec.receivedIndices.size} shards but require ${codec.originalCount}`,
        );
      }
    }
  };

  return {
    async push(chunk) {
        if (!settled && !terminated) {
          buffer = appendBytes(buffer, chunk);
          await drain();
      }
    },
    async finish() {
        if (!settled && !terminated) {
          inputClosed = true;
          await drain();
      }
    },
    reject,
    terminated() {
      return terminated;
    },
  };
}

export class MultipartTransformStream extends TransformStream {
  static create(options) {
    return new MultipartTransformStream(options);
  }

  constructor({
    codec,
    requestBoundary,
    responseBoundary,
    missingIndices = codec.currentRequestedMissingIndices(),
    createError = defaultCreateError,
    maxPartHeaderBytes = MAX_PART_HEADER_BYTES,
    stopInput = () => {},
  }) {
    let resolveOutcome;
    let rejectOutcome;
    const outcome = new Promise((resolve, reject) => {
      resolveOutcome = resolve;
      rejectOutcome = reject;
    });

    const ingress = createIngressDriver({
      codec,
      createError,
      maxPartHeaderBytes,
      requestBoundary,
      requestedMissingIndices: missingIndices,
      resolveOutcome,
      rejectOutcome,
      stopInput,
    });

    super();

    Object.defineProperty(this, "writable", {
      configurable: true,
      enumerable: true,
      value: new WritableStream({
        async write(chunk) {
          try {
            const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
            await ingress.push(bytes);
          } catch (error) {
            ingress.reject(error);
            stopInput("request ingestion failed");
            codec.free();
            throw error;
          }
        },
        async close() {
          try {
            await ingress.finish();
          } catch (error) {
            ingress.reject(error);
            stopInput("request ingestion failed");
            codec.free();
            throw error;
          }
        },
        abort(reason) {
          if (ingress.terminated()) {
            return;
          }

          const error = invalidMultipart(reason instanceof Error ? reason.message : String(reason));
          ingress.reject(error);
          codec.free();
        },
      }),
    });

    Object.defineProperty(this, "readable", {
      configurable: true,
      enumerable: true,
      value: createMultipartResponseReadable({
        codec,
        outcomePromise: outcome,
        responseBoundary,
      }),
    });

    this.outcome = outcome;
  }
}