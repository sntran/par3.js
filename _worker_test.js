import assert from "node:assert/strict";
import test from "node:test";

import worker from "./_worker.js";
import { MultipartDecoder, parseMultipartBoundary } from "./lib/multipart.js";
import { Par3 } from "./lib/mod.js";
import * as wasmModule from "./pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode, shard_arena_ptr } from "./pkg/par3.js";

const { memory } = wasmModule;
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CRLF = encoder.encode("\r\n");
const DOUBLE_CRLF = encoder.encode("\r\n\r\n");
const DASH_DASH = encoder.encode("--");

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

function createRng(seed) {
  let state = seed >>> 0;

  if (state === 0) {
    state = 0x6d2b79f5;
  }

  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 0x100000000;
  };
}

function randomInt(rng, minimum, maximumExclusive) {
  if (maximumExclusive <= minimum) {
    return minimum;
  }

  return minimum + Math.floor(rng() * (maximumExclusive - minimum));
}

function range(length) {
  return Array.from({ length }, (_, index) => index);
}

function shuffle(rng, values) {
  const shuffled = [...values];

  for (let index = shuffled.length - 1; index > 0; index -= 1) {
    const nextIndex = randomInt(rng, 0, index + 1);
    [shuffled[index], shuffled[nextIndex]] = [shuffled[nextIndex], shuffled[index]];
  }

  return shuffled;
}

function pickSortedSubset(rng, values, count) {
  return shuffle(rng, values)
    .slice(0, count)
    .sort((left, right) => left - right);
}

function shardAt(view, shardSize, index) {
  const start = index * shardSize;
  return view.subarray(start, start + shardSize);
}

function buildOriginalShards(rng, originalCount, shardSize) {
  return Array.from({ length: originalCount }, () => {
    const shard = new Uint8Array(shardSize);

    for (let offset = 0; offset < shardSize; offset += 1) {
      shard[offset] = randomInt(rng, 0, 256);
    }

    return shard;
  });
}

function toReferenceSlots(originalShards, recoveryShards) {
  return [...originalShards, ...recoveryShards].map((shard) => Uint8Array.from(shard));
}

function buildPart(name, body, contentType = "application/octet-stream") {
  return {
    name,
    filename: `${name}.bin`,
    contentType,
    body,
  };
}

function toHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(bytes) {
  return toHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
}

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);

  for (let offset = 0; offset < bytes.length; offset += 1) {
    bytes[offset] = Number.parseInt(hex.slice(offset * 2, offset * 2 + 2), 16);
  }

  return bytes;
}

function createLayoutSearchParams(layout = {}) {
  const searchParams = new URLSearchParams();

  if (layout.original_count !== undefined) {
    searchParams.set("n", String(layout.original_count));
  }

  if (layout.recovery_count !== undefined) {
    searchParams.set("r", String(layout.recovery_count));
  }

  if (layout.shard_size !== undefined) {
    searchParams.set("s", String(layout.shard_size));
  }

  if (layout.total_shard_count !== undefined) {
    searchParams.set("total_shard_count", String(layout.total_shard_count));
  }

  for (const index of layout.missing_indices ?? []) {
    searchParams.append("i", String(index));
  }

  return searchParams;
}

function createWorkerUrl(layoutOrSearchParams = {}) {
  const url = new URL("https://example.com/repair");
  const searchParams = layoutOrSearchParams instanceof URLSearchParams
    ? layoutOrSearchParams
    : createLayoutSearchParams(layoutOrSearchParams);
  const query = searchParams.toString();

  if (query) {
    url.search = query;
  }

  return url.toString();
}

function defaultWorkerSearchParams() {
  return createLayoutSearchParams({
    original_count: 1,
    recovery_count: 1,
    shard_size: 4,
    missing_indices: [1],
  });
}

function buildDigestBlob(slotDigests) {
  const digestBytes = new Uint8Array(slotDigests.length * 32);

  for (const [slotIndex, digest] of slotDigests.entries()) {
    if (digest === null || digest === undefined) {
      continue;
    }

    digestBytes.set(hexToBytes(digest), slotIndex * 32);
  }

  return digestBytes;
}

function buildWorkerParts(parts, metadata = {}) {
  const workerParts = [];

  if (metadata.digests !== undefined) {
    workerParts.push({
      name: "digests",
      filename: "digests.bin",
      contentType: "application/octet-stream",
      body: buildDigestBlob(metadata.digests),
    });
  }

  workerParts.push(...parts);
  return workerParts;
}

function wait(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function createStreamingMultipart(parts, boundary, rng) {
  return new ReadableStream({
    async start(controller) {
      for (const part of parts) {
        controller.enqueue(
          encoder.encode(
            `--${boundary}\r\n` +
            `Content-Disposition: form-data; name="${part.name}"${part.filename ? `; filename="${part.filename}"` : ""}\r\n` +
            `Content-Type: ${part.contentType}\r\n\r\n`,
          ),
        );

        const body = typeof part.body === "string" ? encoder.encode(part.body) : part.body;
        let offset = 0;
        while (offset < body.length) {
          const chunkSize = Math.min(body.length - offset, randomInt(rng, 1, 19));
          controller.enqueue(body.subarray(offset, offset + chunkSize));
          offset += chunkSize;
        }

        controller.enqueue(encoder.encode("\r\n"));
      }

      controller.enqueue(encoder.encode(`--${boundary}--\r\n`));
      controller.close();
    },
  });
}

function createPullMultipart(
  parts,
  boundary,
  { chunkSize = 17, onCancel = () => {}, onChunk = () => {} } = {},
) {
  const chunks = [];

  for (const part of parts) {
    chunks.push({
      label: `${part.name}:header`,
      bytes: encoder.encode(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="${part.name}"${part.filename ? `; filename="${part.filename}"` : ""}\r\n` +
        `Content-Type: ${part.contentType}\r\n\r\n`,
      ),
    });

    const body = typeof part.body === "string" ? encoder.encode(part.body) : part.body;
    for (let offset = 0; offset < body.length; offset += chunkSize) {
      chunks.push({
        label: part.name,
        bytes: body.subarray(offset, Math.min(offset + chunkSize, body.length)),
      });
    }

    chunks.push({ label: `${part.name}:footer`, bytes: encoder.encode("\r\n") });
  }

  chunks.push({ label: "closing-boundary", bytes: encoder.encode(`--${boundary}--\r\n`) });

  let index = 0;
  return new ReadableStream({
    pull(controller) {
      if (index >= chunks.length) {
        controller.close();
        return;
      }

      const chunk = chunks[index];
      index += 1;
      onChunk(chunk.label);
      controller.enqueue(chunk.bytes);
    },
    cancel(reason) {
      return onCancel(reason);
    },
  });
}

function countPullMultipartChunks(parts, chunkSize) {
  return parts.reduce((total, part) => {
    const body = typeof part.body === "string" ? encoder.encode(part.body) : part.body;
    return total + 2 + Math.ceil(body.byteLength / chunkSize);
  }, 1);
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

  const merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return merged;
}

async function buildRecoveryShards(originalShards, recoveryCount) {
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

async function collectResponseParts(response) {
  const boundary = parseMultipartBoundary(response.headers.get("content-type"));
  const boundaryBytes = encoder.encode(`--${boundary}`);
  const partBoundary = encoder.encode(`\r\n--${boundary}`);
  let buffer = await readStream(response.body);
  const parts = [];

  assert.equal(startsWithBytes(buffer, boundaryBytes), true);
  buffer = buffer.subarray(boundaryBytes.byteLength);

   if (startsWithBytes(buffer, DASH_DASH)) {
    buffer = buffer.subarray(DASH_DASH.byteLength);
    if (startsWithBytes(buffer, CRLF)) {
      buffer = buffer.subarray(CRLF.byteLength);
    }

    assert.equal(buffer.byteLength, 0);
    return parts;
  }

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
      bytes: Uint8Array.from(buffer.subarray(0, boundaryIndex)),
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

async function assertMultipartStreamFailure(response, pattern) {
  assert.equal(response.status, 200);
  await assert.rejects(() => readStream(response.body), pattern);
}

async function invokeMultipart(parts, seed, { searchParams = defaultWorkerSearchParams() } = {}) {
  const boundary = `----par3-${seed}`;
  const rng = createRng(seed ^ 0xa5a5a5a5);
  const requestBody = createStreamingMultipart(parts, boundary, rng);

  return worker.fetch(
    new Request(createWorkerUrl(searchParams), {
      method: "POST",
      headers: {
        "content-type": `multipart/form-data; boundary=${boundary}`,
      },
      body: requestBody,
      duplex: "half",
    }),
    {},
    {},
  );
}

async function invokeRawRequest({
  body,
  contentType,
  searchParams = defaultWorkerSearchParams(),
}) {
  return worker.fetch(
    new Request(createWorkerUrl(searchParams), {
      method: "POST",
      headers: contentType ? { "content-type": contentType } : {},
      body,
      duplex: "half",
    }),
    {},
    {},
  );
}

function encodeMultipartBytes(parts, boundary, { trailingCrlf = true } = {}) {
  const chunks = [];
  let totalLength = 0;

  for (const part of parts) {
    const headerBytes = encoder.encode(
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="${part.name}"${part.filename ? `; filename="${part.filename}"` : ""}\r\n` +
      `Content-Type: ${part.contentType}\r\n\r\n`,
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

async function invokeWorker(parts, metadata, seed) {
  return invokeMultipart(buildWorkerParts(parts, metadata), seed, {
    searchParams: createLayoutSearchParams(metadata),
  });
}

async function createRepairCase(seed, requestedMode = "subset") {
  const rng = createRng(seed);
  const originalCount = randomInt(rng, 2, 8);
  const recoveryCount = randomInt(rng, 1, 6);
  const shardSize = randomInt(rng, 1, 33) * 2;
  const originalShards = buildOriginalShards(rng, originalCount, shardSize);
  const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
  const referenceSlots = toReferenceSlots(originalShards, recoveryShards);
  const slotCount = originalCount + recoveryCount;
  const actualMissingCount = randomInt(rng, 1, recoveryCount + 1);
  const actualMissingIndices = pickSortedSubset(rng, range(slotCount), actualMissingCount);
  const availableIndices = range(slotCount).filter((index) => !actualMissingIndices.includes(index));
  const sentIndices = shuffle(rng, pickSortedSubset(rng, availableIndices, originalCount));
  const trailingCandidates = availableIndices.filter((index) => !sentIndices.includes(index));
  const trailingIndices = shuffle(
    rng,
    trailingCandidates,
  ).slice(0, randomInt(rng, 0, Math.min(3, trailingCandidates.length) + 1));
  const requestedMissingCount = requestedMode === "none"
    ? 0
    : requestedMode === "all"
      ? actualMissingIndices.length
      : randomInt(rng, 1, actualMissingIndices.length + 1);
  const requestedMissingIndices = requestedMode === "none"
    ? []
    : pickSortedSubset(rng, actualMissingIndices, requestedMissingCount);
  const parts = [];

  for (const slotIndex of [...sentIndices, ...trailingIndices]) {
    parts.push(buildPart(`shard_${slotIndex}`, referenceSlots[slotIndex]));
  }

  return {
    metadata: {
      original_count: originalCount,
      recovery_count: recoveryCount,
      shard_size: shardSize,
      missing_indices: requestedMissingIndices,
    },
    originalCount,
    recoveryCount,
    referenceSlots,
    requestedMissingIndices,
    actualMissingIndices,
    parts,
  };
}

test("repairs requested missing shards across deterministic streamed cases", async (t) => {
  for (let seed = 1; seed <= 32; seed += 1) {
    await t.test(`seed ${seed}`, async () => {
      const testCase = await createRepairCase(seed);
      const response = await invokeWorker(testCase.parts, testCase.metadata, seed);
      const repairedParts = await collectResponseParts(response);

      assert.equal(response.status, 200);
      assert.deepEqual(
        repairedParts.map((part) => part.name),
        testCase.requestedMissingIndices.map((index) => `shard_${index}`),
      );

      for (const [offset, index] of testCase.requestedMissingIndices.entries()) {
        assert.deepEqual(repairedParts[offset].bytes, testCase.referenceSlots[index]);
      }
    });
  }
});

test("streams an empty multipart response when omitted shards are not requested outputs", async (t) => {
  for (let seed = 101; seed <= 116; seed += 1) {
    await t.test(`seed ${seed}`, async () => {
      const testCase = await createRepairCase(seed, "none");
      const response = await invokeWorker(testCase.parts, testCase.metadata, seed);

      assert.equal(response.status, 200);
      assert.deepEqual(await collectResponseParts(response), []);
    });
  }
});

test("streams an empty multipart response when requested outputs are already present", async () => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const response = await invokeWorker(
    [
      buildPart("shard_1", originalShards[1]),
      buildPart("shard_2", recoveryShards[0]),
    ],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    },
    120,
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await collectResponseParts(response), []);
});

test("returns 400 when required query parameters are missing", async () => {
  const response = await invokeMultipart(
    [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))],
    140,
    { searchParams: new URLSearchParams() },
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /n must be an integer greater than or equal to 1/);
});

test("rejects unexpected metadata parts in strict binary mode", async () => {
  const response = await invokeMultipart(
    [{
      name: "metadata",
      contentType: "application/json",
      body: JSON.stringify({ original_count: 2 }),
    }],
    141,
  );

  await assertMultipartStreamFailure(response, /unexpected multipart part metadata/);
});

test("rejects duplicate shard parts", async () => {
  const response = await invokeWorker(
    [
      buildPart("shard_0", Uint8Array.from([0, 1, 2, 3])),
      buildPart("shard_0", Uint8Array.from([0, 1, 2, 3])),
    ],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    },
    211,
  );

  await assertMultipartStreamFailure(response, /duplicate shard part received/);
});

test("rejects shard part names with invalid slot indexes", async () => {
  const invalidPartName = "shard_9007199254740992";
  const response = await invokeMultipart(
    [
      {
        name: invalidPartName,
        contentType: "application/octet-stream",
        body: Uint8Array.from([0, 1, 2, 3]),
      },
    ],
    212,
    {
      searchParams: createLayoutSearchParams({
        original_count: 1,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      }),
    },
  );

  await assertMultipartStreamFailure(
    response,
    new RegExp(`${invalidPartName} must be an integer greater than or equal to 0`),
  );
});

test("rejects requests that never reach the repair threshold", async () => {
  const response = await invokeWorker(
    [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    },
    307,
  );

  await assertMultipartStreamFailure(response, /repair threshold not reached/);
});

test("maps malformed multipart bodies to 400", async () => {
  const response = await worker.fetch(
    new Request(createWorkerUrl(defaultWorkerSearchParams()), {
      method: "POST",
      headers: {
        "content-type": "multipart/form-data; boundary=broken",
      },
      body: "not a multipart body",
      duplex: "half",
    }),
    {},
    {},
  );

  await assertMultipartStreamFailure(
    response,
    /multipart body does not start with the declared boundary/,
  );
});

test("rejects multipart bodies that terminate before the first boundary", async () => {
  const response = await invokeRawRequest({
    contentType: "multipart/form-data; boundary=broken",
    body: "",
  });

  await assertMultipartStreamFailure(response, /multipart body terminated before the first boundary/);
});

test("maps request stream read failures to 400", async () => {
  const response = await invokeRawRequest({
    contentType: "multipart/form-data; boundary=broken",
    body: new ReadableStream({
      start(controller) {
        controller.error(new Error("forced multipart parse failure"));
      },
    }),
  });

  await assertMultipartStreamFailure(response, /forced multipart parse failure/);
});

test("maps non-Error request stream failures to 400", async () => {
  const response = await invokeRawRequest({
    contentType: "multipart/form-data; boundary=broken",
    body: new ReadableStream({
      start(controller) {
        controller.error("forced string parse failure");
      },
    }),
  });

  await assertMultipartStreamFailure(response, /forced string parse failure/);
});

test("ignores response writer abort failures when ingress parsing rejects", { concurrency: false }, async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      const stream = new originalTransformStream();

      return {
        readable: stream.readable,
        writable: {
          getWriter() {
            const writer = stream.writable.getWriter();

            return {
              abort(reason) {
                void writer.abort(reason);
                return Promise.reject(new Error("forced abort failure"));
              },
              close() {
                return writer.close();
              },
              releaseLock() {
                writer.releaseLock();
              },
              write(chunk) {
                return writer.write(chunk);
              },
            };
          },
        },
      };
    }
  };

  try {
    const response = await invokeRawRequest({
      contentType: "multipart/form-data; boundary=broken",
      body: "not a multipart body",
    });

    await assertMultipartStreamFailure(
      response,
      /multipart body does not start with the declared boundary/,
    );
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("validates multipart content-type boundaries before reading the body", async (t) => {
  const cases = [
    {
      name: "missing content-type",
      contentType: null,
      body: Uint8Array.from([120]),
    },
    {
      name: "wrong content-type",
      contentType: "text/plain",
      body: "irrelevant",
    },
    {
      name: "missing boundary parameter",
      contentType: "multipart/form-data",
      body: "irrelevant",
    },
    {
      name: "empty boundary parameter",
      contentType: 'multipart/form-data; boundary=""',
      body: "irrelevant",
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.name, async () => {
      const response = await invokeRawRequest({
        contentType: testCase.contentType,
        body: testCase.body,
      });

      assert.equal(response.status, 400);
      assert.match((await response.json()).error, /content-type must be multipart\/form-data with a boundary/);
    });
  }
});

test("rejects malformed multipart headers during direct request parsing", async (t) => {
  const boundary = "----par3-bad-headers";
  const cases = [
    {
      name: "malformed header line",
      body:
        `--${boundary}\r\n` +
        "Content-Disposition\r\n\r\n" +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart header is malformed: Content-Disposition/,
    },
    {
      name: "missing all part headers",
      body:
        `--${boundary}\r\n` +
        "\r\n" +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart body terminated in part headers/,
    },
    {
      name: "missing content-disposition",
      body:
        `--${boundary}\r\n` +
        "Content-Type: application/json\r\n\r\n" +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart part is missing content-disposition/,
    },
    {
      name: "non form-data disposition",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: attachment; name="metadata"\r\n\r\n' +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart content-disposition must be form-data/,
    },
    {
      name: "missing part name",
      body:
        `--${boundary}\r\n` +
        "Content-Disposition: form-data; ignored\r\n\r\n" +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart part is missing a name/,
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.name, async () => {
      const response = await invokeRawRequest({
        contentType: `multipart/form-data; boundary=${boundary}`,
        body: testCase.body,
      });

      await assertMultipartStreamFailure(response, testCase.pattern);
    });
  }
});

test("rejects digests parts that arrive after shard parts", async () => {
  const response = await invokeMultipart(
    [
      buildPart("shard_0", Uint8Array.from([0, 1, 2, 3])),
      {
        name: "digests",
        contentType: "application/octet-stream",
        body: new Uint8Array(64),
      },
    ],
    401,
    {
      searchParams: createLayoutSearchParams({
        original_count: 2,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      }),
    },
  );

  await assertMultipartStreamFailure(response, /digests part must arrive before shard parts/);
});

test("accepts unquoted content-disposition parameters", async () => {
  const boundary = "----par3-unquoted-params";
  const shardHeader = encoder.encode(
    `--${boundary}\r\n` +
    "Content-Disposition: form-data; ignored=value; name=shard_0; filename=shard_0.bin\r\n" +
    "Content-Type: application/octet-stream\r\n\r\n",
  );
  const shardBytes = Uint8Array.from([0, 1, 2, 3]);
  const closingBoundary = encoder.encode(`\r\n--${boundary}--\r\n`);
  const body = new Uint8Array(shardHeader.byteLength + shardBytes.byteLength + closingBoundary.byteLength);
  body.set(shardHeader);
  body.set(shardBytes, shardHeader.byteLength);
  body.set(closingBoundary, shardHeader.byteLength + shardBytes.byteLength);
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body,
  });

  const repairedParts = await collectResponseParts(response);
  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
});

test("rejects truncated or oversized multipart parser states", async (t) => {
  const boundary = "----par3-parser-edge-cases";
  const shardBody = "abcd";
  const oversizedHeader = "X-Oversized: " + "x".repeat(9 * 1024);
  const cases = [
    {
      name: "oversized part headers",
      body:
        `--${boundary}\r\n` +
        `${oversizedHeader}\r\n\r\n` +
        "{}\r\n" +
        `--${boundary}--\r\n`,
      pattern: /multipart headers exceed maximum size of 8192 bytes/,
    },
    {
      name: "streaming oversized part headers without a delimiter",
      body:
        `--${boundary}\r\n` +
        oversizedHeader,
      pattern: /multipart headers exceed maximum size of 8192 bytes/,
    },
    {
      name: "truncated part headers",
      body: `--${boundary}\r\nContent-Disposition: form-data; name="metadata"`,
      pattern: /multipart body terminated in part headers/,
    },
    {
      name: "missing next boundary after part body",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: form-data; name="shard_0"\r\n' +
        "Content-Type: application/octet-stream\r\n\r\n" +
        shardBody,
      pattern: /multipart body terminated before the next boundary/,
    },
    {
      name: "truncated bytes after a boundary marker",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: form-data; name="shard_0"\r\n' +
        "Content-Type: application/octet-stream\r\n\r\n" +
        `${shardBody}\r\n--${boundary}`,
      pattern: /multipart body terminated after a part boundary/,
    },
    {
      name: "invalid separator after a part boundary",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: form-data; name="shard_0"\r\n' +
        "Content-Type: application/octet-stream\r\n\r\n" +
        `${shardBody}\r\n--${boundary}xx`,
      pattern: /multipart boundary must end with CRLF/,
    },
    {
      name: "invalid separator after the opening boundary",
      body: `--${boundary}xx`,
      pattern: /multipart boundary must end with CRLF/,
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.name, async () => {
      const response = await invokeRawRequest({
        contentType: `multipart/form-data; boundary=${boundary}`,
        body: testCase.body,
      });

      await assertMultipartStreamFailure(response, testCase.pattern);
    });
  }
});

test("accepts a multipart stream that closes immediately with the final boundary", async () => {
  const boundary = "----par3-empty-multipart";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: `--${boundary}--\r\n`,
  });

  await assertMultipartStreamFailure(response, /repair threshold not reached/);
});

test("accepts an immediate final boundary without a trailing CRLF", async () => {
  const boundary = "----par3-empty-multipart-no-crlf";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: `--${boundary}--`,
  });

  await assertMultipartStreamFailure(response, /repair threshold not reached/);
});

test("accepts a final multipart boundary without a trailing CRLF", async () => {
  const boundary = "----par3-no-final-crlf";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: encodeMultipartBytes(
      [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))],
      boundary,
      { trailingCrlf: false },
    ),
  });

  const repairedParts = await collectResponseParts(response);
  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
});

test("rejects invalid query parameters", async (t) => {
  const cases = [
    {
      name: "n below minimum",
      searchParams: new URLSearchParams([["n", "0"], ["r", "1"], ["s", "4"], ["i", "1"]]),
      pattern: /n must be an integer greater than or equal to 1/,
    },
    {
      name: "r below minimum",
      searchParams: new URLSearchParams([["n", "2"], ["r", "-1"], ["s", "4"], ["i", "1"]]),
      pattern: /r must be an integer greater than or equal to 0/,
    },
    {
      name: "odd s",
      searchParams: new URLSearchParams([["n", "2"], ["r", "1"], ["s", "3"], ["i", "1"]]),
      pattern: /s must be even/,
    },
    {
      name: "i must be an integer",
      searchParams: new URLSearchParams([["n", "2"], ["r", "1"], ["s", "4"], ["i", "nope"]]),
      pattern: /i\[\] must be an integer greater than or equal to 0/,
    },
    {
      name: "i below minimum",
      searchParams: new URLSearchParams([["n", "2"], ["r", "1"], ["s", "4"], ["i", "-1"]]),
      pattern: /i\[\] must be an integer greater than or equal to 0/,
    },
  ];

  for (const [offset, testCase] of cases.entries()) {
    await t.test(testCase.name, async () => {
      const response = await invokeMultipart([], 410 + offset, { searchParams: testCase.searchParams });

      assert.equal(response.status, 400);
      assert.match((await response.json()).error, testCase.pattern);
    });
  }
});

test("rejects duplicate digests parts", async () => {
  const response = await invokeMultipart(
    [
      { name: "digests", contentType: "application/octet-stream", body: new Uint8Array(64) },
      { name: "digests", contentType: "application/octet-stream", body: new Uint8Array(64) },
    ],
    420,
    {
      searchParams: createLayoutSearchParams({
        original_count: 1,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      }),
    },
  );

  await assertMultipartStreamFailure(response, /digests part may only appear once/);
});

test("rejects undersized digests parts", async () => {
  const response = await invokeMultipart(
    [{ name: "digests", contentType: "application/octet-stream", body: new Uint8Array(31) }],
    421,
  );

  await assertMultipartStreamFailure(response, /digests must contain exactly 2 contiguous 32-byte hashes/);
});

test("rejects oversized shard parts", async () => {
  const response = await invokeWorker(
    [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3, 4]))],
    {
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    },
    430,
  );

  await assertMultipartStreamFailure(response, /exceeds declared shard_size 4/);
});

test("rejects undersized shard parts", async () => {
  const response = await invokeWorker(
    [buildPart("shard_0", Uint8Array.from([0, 1]))],
    {
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    },
    431,
  );

  await assertMultipartStreamFailure(response, /wrote 2 bytes but shard_size is 4/);
});

test("rejects worker requests without a locked slot count", async () => {
  const response = await invokeWorker(
    [
      buildPart("shard_0", Uint8Array.from([0, 1, 2, 3])),
      buildPart("shard_2", Uint8Array.from([8, 9, 10, 11])),
    ],
    {
      original_count: 3,
      shard_size: 4,
      missing_indices: [1],
    },
    440,
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /query parameters must declare r or total_shard_count/);
});

test("rejects shard indexes outside a declared total_shard_count", async () => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
    Uint8Array.from([8, 9, 10, 11]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 2);
  const response = await invokeWorker(
    [
      buildPart("shard_0", originalShards[0]),
      buildPart("shard_2", originalShards[2]),
      buildPart("shard_4", recoveryShards[1]),
    ],
    {
      original_count: 3,
      shard_size: 4,
      total_shard_count: 4,
      missing_indices: [1],
    },
    441,
  );

  await assertMultipartStreamFailure(response, /outside declared total_shard_count/);
});

test("rejects POST requests without a body", async () => {
  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
    }),
    {},
    {},
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /request body is required/);
});

test("rejects invalid layouts before allocating or reading request bytes", { concurrency: false }, async (t) => {
  const originalAllocShardArena = Par3.bindings.alloc_shard_arena;
  const originalCreateDecoder = MultipartDecoder.create;
  let allocCalls = 0;
  let createCalls = 0;
  const body = new ReadableStream({
    pull(controller) {
      controller.enqueue(encoder.encode("unused"));
      controller.close();
    },
  });
  const request = new Request(createWorkerUrl({
    original_count: 2,
    recovery_count: 1,
    shard_size: 4,
    missing_indices: [3],
  }), {
    method: "POST",
    headers: {
      "content-type": "multipart/form-data; boundary=----par3-no-read",
    },
    body,
    duplex: "half",
  });

  t.mock.method(Par3.bindings, "alloc_shard_arena", (...args) => {
    allocCalls += 1;
    return originalAllocShardArena(...args);
  });
  t.mock.method(MultipartDecoder, "create", (...args) => {
    createCalls += 1;
    return originalCreateDecoder(...args);
  });

  const response = await worker.fetch(request, {}, {});

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /i\[\] must be less than total_shard_count 3/);
  assert.equal(allocCalls, 0);
  assert.equal(createCalls, 0);
});

test("releases response resources when the caller cancels a multipart response", async () => {
  const testCase = await createRepairCase(450, "all");
  const response = await invokeWorker(testCase.parts, testCase.metadata, 450);

  assert.equal(response.status, 200);
  await response.body.cancel();
});

test("releases response resources even when the readable cancel hook rejects", { concurrency: false }, async (t) => {
  const originalTransformStream = globalThis.TransformStream;
  const originalFree = Par3.prototype.free;
  const freeDone = Promise.withResolvers();
  let freeCalls = 0;

  t.mock.method(Par3.prototype, "free", function mockFree(...args) {
    freeCalls += 1;
    freeDone.resolve();
    return originalFree.apply(this, args);
  });

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          cancel() {
            return Promise.reject(new Error("forced readable cancel failure"));
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.resolve();
              },
              releaseLock() {},
              write() {
                return Promise.resolve();
              },
            };
          },
        },
      };
    }
  };

  try {
    const response = await invokeWorker(
      [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))],
      {
        original_count: 1,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      },
      4501,
    );

    assert.equal(response.status, 200);
    await response.body.cancel();
    await freeDone.promise;
    assert.equal(freeCalls, 1);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("keeps wasm-backed response bytes alive until a slow consumer finishes reading", { concurrency: false }, async (t) => {
  const originalRepair = Par3.prototype.repair;
  const originalFree = Par3.prototype.free;
  const repairDone = Promise.withResolvers();
  let freeCalls = 0;

  t.mock.method(Par3.prototype, "repair", function mockRepair(...args) {
    const result = originalRepair.apply(this, args);
    repairDone.resolve();
    return result;
  });

  t.mock.method(Par3.prototype, "free", function mockFree(...args) {
    freeCalls += 1;
    if (this.arenaPtr !== 0 && this.arenaSlotCount !== 0) {
      this.memoryView(this.arenaPtr, this.arenaSlotCount * this.shardSize).fill(0);
    }

    return originalFree.apply(this, args);
  });

  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
      headers: {
        "content-type": "multipart/form-data; boundary=----par3-slow-consumer",
      },
      body: encodeMultipartBytes([
        buildPart("shard_0", originalShards[0]),
        buildPart("shard_2", recoveryShards[0]),
      ], "----par3-slow-consumer"),
      duplex: "half",
    }),
    {},
    {},
  );

  assert.equal(response.status, 200);

  const slowBytesPromise = (async () => {
    const reader = response.body.getReader();
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
        await wait(10);
      }
    } finally {
      reader.releaseLock();
    }

    const merged = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }

    return merged;
  })();

  await repairDone.promise;
  assert.equal(freeCalls, 0);

  const slowBytes = await slowBytesPromise;
  const repairedParts = await collectResponseParts(new Response(slowBytes, {
    headers: {
      "content-type": response.headers.get("content-type"),
    },
  }));

  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(repairedParts[0].bytes, originalShards[1]);
  assert.equal(freeCalls, 1);
});

test("releases response resources when streaming a shard throws", { concurrency: false }, async (t) => {
  const testCase = await createRepairCase(451, "all");

  t.mock.method(Par3.prototype, "shardView", () => {
    throw new Error("forced shard read failure");
  });

  const response = await invokeWorker(testCase.parts, testCase.metadata, 451);

  assert.equal(response.status, 200);
  await assert.rejects(() => readStream(response.body), /forced shard read failure/);
});

test("rejects oversized digests parts before buffering them entirely", async () => {
  const response = await invokeMultipart(
    [{
      name: "digests",
      contentType: "application/octet-stream",
      body: new Uint8Array(128),
    }],
    470,
    {
      searchParams: createLayoutSearchParams({
        original_count: 2,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      }),
    },
  );

  await assertMultipartStreamFailure(response, /digests exceeds expected size of 96 bytes/);
});

test("accepts decoder body chunks that are views, ArrayBuffers, and array-like values", { concurrency: false }, async (t) => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const digestBytes = new Uint8Array(96);

  t.mock.method(MultipartDecoder, "create", () => ({
    readable: new ReadableStream({
      start(controller) {
        const enqueuePart = (name, chunks) => {
          controller.enqueue(new Response(new ReadableStream({
            start(bodyController) {
              for (const chunk of chunks) {
                bodyController.enqueue(chunk);
              }
              bodyController.close();
            },
          }), {
            headers: {
              "content-disposition": `form-data; name="${name}"; filename="${name}.bin"`,
              "content-type": "application/octet-stream",
            },
          }));
        };

        enqueuePart("digests", [new Uint16Array(digestBytes.buffer)]);
        enqueuePart("shard_0", [Array.from(originalShards[0])]);
        enqueuePart(
          "shard_2",
          [recoveryShards[0].buffer.slice(
            recoveryShards[0].byteOffset,
            recoveryShards[0].byteOffset + recoveryShards[0].byteLength,
          )],
        );
        controller.close();
      },
    }),
    writable: new WritableStream({
      write() {},
      close() {},
      abort() {},
    }),
  }));

  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
      headers: {
        "content-type": "multipart/form-data; boundary=mocked",
      },
      body: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      duplex: "half",
    }),
    {},
    {},
  );
  const repairedParts = await collectResponseParts(response);

  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(repairedParts[0].bytes, originalShards[1]);
});

test("verifies optional per-shard digests before committing input shards", async () => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const response = await invokeWorker(
    [
      buildPart("shard_0", originalShards[0]),
      buildPart("shard_2", recoveryShards[0]),
    ],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
      digests: [await sha256Hex(originalShards[0]), null, await sha256Hex(recoveryShards[0])],
    },
    471,
  );

  const repairedParts = await collectResponseParts(response);
  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(repairedParts[0].bytes, originalShards[1]);
});

test("treats non-32-byte digest results as mismatches", { concurrency: false }, async (t) => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const slotDigests = [
    await sha256Hex(originalShards[0]),
    null,
    await sha256Hex(recoveryShards[0]),
  ];

  t.mock.method(crypto.subtle, "digest", async () => new Uint8Array(31).buffer);

  const response = await invokeWorker(
    [
      buildPart("shard_0", originalShards[0]),
      buildPart("shard_2", recoveryShards[0]),
    ],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
      digests: slotDigests,
    },
    4711,
  );

  await assertMultipartStreamFailure(response, /digest mismatch for slot 0/);
});

test("rejects shard parts whose digest does not match the digests part", async () => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const response = await invokeWorker(
    [
      buildPart("shard_0", originalShards[0]),
      buildPart("shard_2", recoveryShards[0]),
    ],
    {
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
      digests: ["11".repeat(32), null, await sha256Hex(recoveryShards[0])],
    },
    472,
  );

  await assertMultipartStreamFailure(response, /digest mismatch for slot 0/);
});

test("rejects out-of-range requested missing indices before shard reads", async (t) => {
  for (let seed = 501; seed <= 516; seed += 1) {
    await t.test(`seed ${seed}`, async () => {
      const testCase = await createRepairCase(seed, "all");
      const slotCount = testCase.originalCount + testCase.recoveryCount;
      const response = await invokeWorker(
        testCase.parts,
        {
          ...testCase.metadata,
          total_shard_count: slotCount,
          recovery_count: undefined,
          missing_indices: [...testCase.requestedMissingIndices, slotCount + 1],
        },
        seed,
      );

      assert.equal(response.status, 400);
      assert.match(
        (await response.json()).error,
        /i\[\] must be less than total_shard_count \d+/,
      );
    });
  }
});

test("returns before request ingress is fully consumed once the repair threshold is satisfied", async () => {
  const originalShards = [
    Uint8Array.from([0, 1, 2, 3]),
    Uint8Array.from([4, 5, 6, 7]),
  ];
  const recoveryShards = await buildRecoveryShards(originalShards, 1);
  const trailingBody = new Uint8Array(1024).fill(9);
  const boundary = "----par3-stop-early";
  const chunkSize = 17;
  let cancellationReason = null;
  let pulledChunks = 0;
  const pulledLabels = [];
  const parts = [
    buildPart("shard_0", originalShards[0]),
    buildPart("shard_2", recoveryShards[0]),
    buildPart("shard_1", trailingBody),
  ];
  const totalPullableChunks = countPullMultipartChunks(parts, chunkSize);
  const body = createPullMultipart(
    parts,
    boundary,
    {
      chunkSize,
      onCancel(reason) {
        cancellationReason = reason;
      },
      onChunk(label) {
        pulledChunks += 1;
        pulledLabels.push(label);
      },
    },
  );

  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 2,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
      headers: {
        "content-type": `multipart/form-data; boundary=${boundary}`,
      },
      body,
      duplex: "half",
    }),
    {},
    {},
  );

  assert.equal(response.status, 200);
  const pulledChunksAtResolution = pulledChunks;
  assert.ok(pulledChunksAtResolution < totalPullableChunks);
  assert.equal(cancellationReason, null);

  const repairedParts = await collectResponseParts(response);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);

  await wait(100);
  const pulledChunksAfterCancellation = pulledChunks;

  assert.ok(pulledChunksAfterCancellation < totalPullableChunks);

  await wait(100);

  assert.equal(pulledChunks, pulledChunksAfterCancellation);
  if (cancellationReason !== null) {
    assert.match(String(cancellationReason), /repair threshold reached|AbortError/);
  }
});

test("maps unexpected runtime failures to a 500 response", { concurrency: false }, async (t) => {
  const testCase = await createRepairCase(460, "all");

  t.mock.method(crypto, "randomUUID", () => {
    throw new Error("forced response construction failure");
  });
  t.mock.method(console, "error", () => {});

  const response = await invokeWorker(testCase.parts, testCase.metadata, 460);

  assert.equal(response.status, 500);
  assert.match((await response.json()).error, /internal server error/);
});

test("registers the background pump with ctx.waitUntil when available", async () => {
  const waitUntilCalls = [];
  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
      headers: {
        "content-type": "multipart/form-data; boundary=----par3-wait-until",
      },
      body: encodeMultipartBytes([buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))], "----par3-wait-until"),
      duplex: "half",
    }),
    {},
    {
      waitUntil(promise) {
        waitUntilCalls.push(promise);
      },
    },
  );

  assert.equal(response.status, 200);
  assert.equal(waitUntilCalls.length, 1);
  const repairedParts = await collectResponseParts(response);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
  await waitUntilCalls[0];
});

test("waitUntil also tracks rejected background pumps", async () => {
  const waitUntilCalls = [];
  const boundary = "----par3-wait-until-error";
  const response = await worker.fetch(
    new Request(createWorkerUrl({
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }), {
      method: "POST",
      headers: {
        "content-type": `multipart/form-data; boundary=${boundary}`,
      },
      body: encoder.encode(`--${boundary}\r\nContent-Disposition\r\n\r\nboom\r\n--${boundary}--\r\n`),
      duplex: "half",
    }),
    {},
    {
      waitUntil(promise) {
        waitUntilCalls.push(promise);
      },
    },
  );

  await assertMultipartStreamFailure(response, /multipart header is malformed/);
  assert.equal(waitUntilCalls.length, 1);
  await assert.rejects(waitUntilCalls[0], /multipart header is malformed/);
});

test("returns 405 for unsupported methods", async () => {
  const response = await worker.fetch(
    new Request("https://example.com/repair", {
      method: "GET",
    }),
    {},
    {},
  );

  assert.equal(response.status, 405);
  assert.equal(response.headers.get("allow"), "POST");
});
