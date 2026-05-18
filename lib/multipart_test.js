import assert from "node:assert/strict";
import test from "node:test";

import {
  MAX_PART_HEADER_BYTES,
  MultipartParseError,
  MultipartTransformStream,
  parseMultipartBoundary,
} from "./multipart.js";
import { Par3 } from "./mod.js";
import * as wasmModule from "../pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode, shard_arena_ptr } from "../pkg/par3.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CRLF = encoder.encode("\r\n");
const DOUBLE_CRLF = encoder.encode("\r\n\r\n");
const DASH_DASH = encoder.encode("--");
const { memory } = wasmModule;

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

function shardAt(view, shardSize, index) {
  const start = index * shardSize;
  return view.subarray(start, start + shardSize);
}

function buildOriginalShards(originalCount, shardSize) {
  return Array.from({ length: originalCount }, (_, shardIndex) => {
    const shard = new Uint8Array(shardSize);
    for (let offset = 0; offset < shardSize; offset += 1) {
      shard[offset] = (shardIndex * 37 + offset * 17) % 251;
    }
    return shard;
  });
}

function buildRecoveryShards(originalShards, recoveryCount) {
  const originalCount = originalShards.length;
  const shardSize = originalShards[0].byteLength;
  const slotCount = originalCount + recoveryCount;
  const arenaHandle = alloc_shard_arena(slotCount, shardSize);
  const arenaPtr = shard_arena_ptr(arenaHandle);

  try {
    const bytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);
    for (let index = 0; index < originalCount; index += 1) {
      bytes.set(originalShards[index], index * shardSize);
    }

    leopard_encode(originalCount, shardSize, arenaHandle);

    const encodedBytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);
    return Array.from({ length: recoveryCount }, (_, recoveryIndex) =>
      Uint8Array.from(shardAt(encodedBytes, shardSize, originalCount + recoveryIndex)),
    );
  } finally {
    free_shard_arena(arenaHandle);
  }
}

function buildMultipartBytes(parts, boundary, { trailingCrlf = true } = {}) {
  const chunks = [];
  let totalLength = 0;

  for (const part of parts) {
    const headerBytes = encoder.encode(
      `--${boundary}\r\n`
      + `Content-Disposition: form-data; name="${part.name}"${part.filename ? `; filename="${part.filename}"` : ""}\r\n`
      + `Content-Type: ${part.contentType}\r\n\r\n`,
    );
    const bodyBytes = typeof part.body === "string" ? encoder.encode(part.body) : part.body;
    const footerBytes = encoder.encode("\r\n");

    chunks.push(headerBytes, bodyBytes, footerBytes);
    totalLength += headerBytes.byteLength + bodyBytes.byteLength + footerBytes.byteLength;
  }

  const closingBoundary = encoder.encode(`--${boundary}--${trailingCrlf ? "\r\n" : ""}`);
  chunks.push(closingBoundary);
  totalLength += closingBoundary.byteLength;

  const bytes = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return bytes;
}

function streamFromChunks(chunks, { onCancel = () => {}, onPull = () => {} } = {}) {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      onPull(index);
      if (index >= chunks.length) {
        controller.close();
        return;
      }

      controller.enqueue(chunks[index]);
      index += 1;
    },
    cancel(reason) {
      onCancel(reason);
    },
  });
}

async function readAllParts(stream, boundary) {
  const boundaryBytes = encoder.encode(`--${boundary}`);
  const partBoundary = encoder.encode(`\r\n--${boundary}`);
  let buffer = await readStream(stream);
  const parts = [];

  assert.equal(startsWithBytes(buffer, boundaryBytes), true);
  buffer = buffer.subarray(boundaryBytes.byteLength);
  assert.equal(startsWithBytes(buffer, CRLF), true);
  buffer = buffer.subarray(CRLF.byteLength);

  while (buffer.byteLength > 0) {
    const headerEnd = indexOfBytes(buffer, DOUBLE_CRLF);
    assert.notEqual(headerEnd, -1);

    const rawHeaders = decoder.decode(buffer.subarray(0, headerEnd));
    const partNameMatch = /name="([^"]+)"/u.exec(rawHeaders);
    assert.ok(partNameMatch);

    buffer = buffer.subarray(headerEnd + DOUBLE_CRLF.byteLength);
    const boundaryIndex = indexOfBytes(buffer, partBoundary);
    assert.notEqual(boundaryIndex, -1);

    parts.push({
      body: Uint8Array.from(buffer.subarray(0, boundaryIndex)),
      name: partNameMatch[1],
    });

    buffer = buffer.subarray(boundaryIndex + partBoundary.byteLength);
    if (startsWithBytes(buffer, DASH_DASH)) {
      buffer = buffer.subarray(DASH_DASH.byteLength);
      if (startsWithBytes(buffer, CRLF)) {
        buffer = buffer.subarray(CRLF.byteLength);
      }
      break;
    }

    assert.equal(startsWithBytes(buffer, CRLF), true);
    buffer = buffer.subarray(CRLF.byteLength);
  }

  return parts;
}

async function readStream(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let totalLength = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      chunks.push(Uint8Array.from(value));
      totalLength += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return bytes;
}

test("parseMultipartBoundary accepts quoted boundaries and rejects invalid content types", () => {
  assert.equal(parseMultipartBoundary('multipart/form-data; boundary="abc123"'), "abc123");
  assert.equal(parseMultipartBoundary("multipart/form-data; charset=utf-8; boundary=plain"), "plain");

  assert.throws(() => parseMultipartBoundary(null), MultipartParseError);
  assert.throws(() => parseMultipartBoundary("text/plain"), /multipart\/form-data/);
  assert.throws(() => parseMultipartBoundary("multipart/form-data"), /boundary/);
});

test("MultipartTransformStream repairs streamed shards across chunk boundaries", async () => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const inputBoundary = "parts";
  const outputBoundary = "response";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: inputBoundary,
    responseBoundary: outputBoundary,
  });
  const bytes = buildMultipartBytes([
    {
      name: "shard_0",
      filename: "shard_0.bin",
      contentType: "application/octet-stream",
      body: originalShards[0],
    },
    {
      name: "shard_2",
      filename: "shard_2.bin",
      contentType: "application/octet-stream",
      body: recoveryShards[0],
    },
  ], inputBoundary);
  const responseStream = streamFromChunks([
    bytes.subarray(0, 4),
    bytes.subarray(4, 19),
    bytes.subarray(19, 41),
    bytes.subarray(41),
  ]).pipeThrough(transform);

  const outcome = await transform.outcome;
  const parts = await readAllParts(responseStream, outputBoundary);

  assert.deepEqual(outcome, {
    missingIndices: [1],
    nothingToRepair: false,
    repairedCount: 1,
  });
  assert.deepEqual(parts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(parts[0].body, originalShards[1]);
});

test("MultipartTransformStream reports nothingToRepair when requested outputs are already present", async () => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const inputBoundary = "nothing-to-repair";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: inputBoundary,
    responseBoundary: "unused",
  });
  const responseStream = streamFromChunks([
    buildMultipartBytes([
      {
        name: "shard_1",
        filename: "shard_1.bin",
        contentType: "application/octet-stream",
        body: originalShards[1],
      },
      {
        name: "shard_2",
        filename: "shard_2.bin",
        contentType: "application/octet-stream",
        body: recoveryShards[0],
      },
    ], inputBoundary),
  ]).pipeThrough(transform);

  const outcome = await transform.outcome;
  const bodyBytes = await readStream(responseStream);

  assert.deepEqual(outcome, {
    missingIndices: [],
    nothingToRepair: true,
    repairedCount: 0,
  });
  assert.equal(bodyBytes.byteLength, 0);
});

test("MultipartTransformStream stops pulling once the repair threshold is satisfied", async () => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const trailingBody = new Uint8Array(512).fill(9);
  const inputBoundary = "stop-early";
  const outputBoundary = "stop-early-response";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const abortController = new AbortController();
  let pulls = 0;
  let stopReason = null;
  const bytes = buildMultipartBytes([
    {
      name: "shard_0",
      filename: "shard_0.bin",
      contentType: "application/octet-stream",
      body: originalShards[0],
    },
    {
      name: "shard_2",
      filename: "shard_2.bin",
      contentType: "application/octet-stream",
      body: recoveryShards[0],
    },
    {
      name: "shard_1",
      filename: "shard_1.bin",
      contentType: "application/octet-stream",
      body: trailingBody,
    },
  ], inputBoundary);
  const chunks = Array.from({ length: Math.ceil(bytes.byteLength / 13) }, (_, index) =>
    bytes.subarray(index * 13, Math.min(bytes.byteLength, (index + 1) * 13)),
  );
  const source = streamFromChunks(chunks, {
    onPull() {
      pulls += 1;
    },
  });
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: inputBoundary,
    responseBoundary: outputBoundary,
    stopInput(reason) {
      stopReason = reason;
      abortController.abort(reason);
    },
  });
  const responseStream = source.pipeThrough(transform, { signal: abortController.signal });

  await transform.outcome;
  const pullsAtOutcome = pulls;
  await readAllParts(responseStream, outputBoundary);

  assert.equal(stopReason, "repair threshold reached");
  assert.ok(pullsAtOutcome < chunks.length);
  assert.equal(pulls, pullsAtOutcome);
});

test("MultipartTransformStream rejects malformed multipart state", async (t) => {
  const cases = [
    {
      body: [encoder.encode("----bad\r\nContent-Disposition\r\n\r\n{}\r\n----bad--\r\n")],
      boundary: "--bad",
      pattern: /multipart header is malformed/,
    },
    {
      body: [encoder.encode(`----huge\r\nX-Test: ${"x".repeat(MAX_PART_HEADER_BYTES + 64)}`)],
      boundary: "--huge",
      pattern: /multipart headers exceed maximum size/,
    },
    {
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=shard_0; filename=shard_0.bin\r\nContent-Type: application/octet-stream\r\n\r\n\x00\x01\x02\x03\r\n----edge")],
      boundary: "--edge",
      pattern: /multipart body terminated after a part boundary/,
    },
    {
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=shard_0; filename=shard_0.bin\r\nContent-Type: application/octet-stream\r\n\r\n\x00\x01\x02\x03\r\n----edge--oops")],
      boundary: "--edge",
      pattern: /multipart boundary must end with CRLF/,
    },
    {
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=shard_0")],
      boundary: "--edge",
      pattern: /multipart body terminated in part headers/,
    },
    {
      body: [encoder.encode("----edge--oops")],
      boundary: "--edge",
      pattern: /multipart boundary must end with CRLF/,
    },
  ];

  for (const [index, testCase] of cases.entries()) {
    await t.test(`case ${index + 1}`, async () => {
      const codec = new Par3(Par3.resolveLayout({
        originalCount: 1,
        recoveryCount: 1,
        requestedMissingIndices: [1],
        shardSize: 4,
      }));
      const transform = new MultipartTransformStream({
        codec,
        missingIndices: [1],
        requestBoundary: testCase.boundary,
        responseBoundary: "ignored",
      });

      void streamFromChunks(testCase.body).pipeThrough(transform);
      await assert.rejects(() => transform.outcome, testCase.pattern);
    });
  }
});

test("MultipartTransformStream emits shard views lazily and surfaces read failures", { concurrency: false }, async (t) => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const inputBoundary = "lazy-read";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: inputBoundary,
    responseBoundary: "lazy-response",
  });
  const responseStream = streamFromChunks([
    buildMultipartBytes([
      {
        name: "shard_0",
        filename: "shard_0.bin",
        contentType: "application/octet-stream",
        body: originalShards[0],
      },
      {
        name: "shard_2",
        filename: "shard_2.bin",
        contentType: "application/octet-stream",
        body: recoveryShards[0],
      },
    ], inputBoundary),
  ]).pipeThrough(transform);

  await transform.outcome;
  t.mock.method(codec, "shardView", () => {
    throw new Error("forced shard read failure");
  });

  await assert.rejects(() => readStream(responseStream), /forced shard read failure/);
});

test("MultipartTransformStream rejects invalid shard indexes with default transform errors", async () => {
  const invalidPartName = "shard_9007199254740992";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 1,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: "bad-index",
    responseBoundary: "bad-index-response",
  });
  const responseStream = streamFromChunks([
    encoder.encode(
      "--bad-index\r\n"
      + `Content-Disposition: form-data; name="${invalidPartName}"; filename="${invalidPartName}.bin"\r\n`
      + 'Content-Type: application/octet-stream\r\n\r\n'
      + '\x00\x01\x02\x03\r\n'
      + '--bad-index--\r\n',
    ),
  ]).pipeThrough(transform);

  const pattern = new RegExp(`${invalidPartName} must be an integer greater than or equal to 0`);
  await assert.rejects(() => transform.outcome, pattern);
  await assert.rejects(() => readStream(responseStream), pattern);
});

test("MultipartTransformStream rejects digest mismatches while ingesting shard parts", async () => {
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 1,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: "digest-mismatch",
    responseBoundary: "digest-mismatch-response",
  });
  const digestBytes = new Uint8Array(64);
  digestBytes.set([1, 1, 1, 1], 0);
  const responseStream = streamFromChunks([
    buildMultipartBytes([
      {
        name: "digests",
        filename: "digests.bin",
        contentType: "application/octet-stream",
        body: digestBytes,
      },
      {
        name: "shard_0",
        filename: "shard_0.bin",
        contentType: "application/octet-stream",
        body: Uint8Array.from([0, 1, 2, 3]),
      },
    ], "digest-mismatch"),
  ]).pipeThrough(transform);

  await assert.rejects(() => transform.outcome, /digest mismatch for slot 0/);
  await assert.rejects(() => readStream(responseStream), /digest mismatch for slot 0/);
});

test("MultipartTransformStream accepts direct ArrayBuffer writes and ignores extra writes after repair", async () => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: "array-buffer",
    responseBoundary: "array-buffer-response",
  });
  const writer = transform.writable.getWriter();
  const bytes = buildMultipartBytes([
    {
      name: "shard_0",
      filename: "shard_0.bin",
      contentType: "application/octet-stream",
      body: originalShards[0],
    },
    {
      name: "shard_2",
      filename: "shard_2.bin",
      contentType: "application/octet-stream",
      body: recoveryShards[0],
    },
  ], "array-buffer");

  await writer.write(bytes.slice(0, 11).buffer);
  await writer.write(bytes.slice(11).buffer);
  await transform.outcome;
  await writer.write(Uint8Array.from([9, 9]).buffer);
  await writer.close();

  const parts = await readAllParts(transform.readable, "array-buffer-response");
  assert.deepEqual(parts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(parts[0].body, originalShards[1]);
});

test("MultipartTransformStream aborts the writable before completion", async () => {
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 1,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: "abort-early",
    responseBoundary: "abort-early-response",
  });
  const writer = transform.writable.getWriter();

  await writer.abort(new Error("forced abort"));
  await assert.rejects(() => transform.outcome, /forced abort/);
  await assert.rejects(() => readStream(transform.readable), /forced abort/);
});

test("MultipartTransformStream propagates default transform errors to the readable side", async () => {
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 1,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: "metadata-error",
    responseBoundary: "metadata-error-response",
  });
  const responseStream = streamFromChunks([
    encoder.encode(
      "--metadata-error\r\n"
      + 'Content-Disposition: form-data; name="metadata"\r\n'
      + 'Content-Type: application/json\r\n\r\n'
      + '{}\r\n'
      + '--metadata-error--\r\n',
    ),
  ]).pipeThrough(transform);

  await assert.rejects(() => transform.outcome, /unexpected multipart part metadata/);
  await assert.rejects(() => readStream(responseStream), /unexpected multipart part metadata/);
});

test("MultipartTransformStream frees the codec when the caller cancels early", { concurrency: false }, async (t) => {
  const originalShards = buildOriginalShards(2, 4);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const inputBoundary = "cancel-early";
  const codec = new Par3(Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    requestedMissingIndices: [1],
    shardSize: 4,
  }));
  const originalFree = codec.free;
  let freeCalls = 0;

  t.mock.method(codec, "free", function freeCodec() {
    freeCalls += 1;
    return originalFree.call(this);
  });

  const transform = new MultipartTransformStream({
    codec,
    missingIndices: [1],
    requestBoundary: inputBoundary,
    responseBoundary: "cancel-response",
  });
  const responseStream = streamFromChunks([
    buildMultipartBytes([
      {
        name: "shard_0",
        filename: "shard_0.bin",
        contentType: "application/octet-stream",
        body: originalShards[0],
      },
      {
        name: "shard_2",
        filename: "shard_2.bin",
        contentType: "application/octet-stream",
        body: recoveryShards[0],
      },
    ], inputBoundary),
  ]).pipeThrough(transform);

  await transform.outcome;
  await responseStream.cancel();
  assert.ok(freeCalls >= 1);
});
