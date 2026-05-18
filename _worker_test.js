import assert from "node:assert/strict";
import test from "node:test";

import worker from "./_worker.js";
import { MultipartStreamReader, parseMultipartBoundary } from "./lib/multipart.js";
import { Par3 } from "./lib/mod.js";
import * as wasmModule from "./pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode, shard_arena_ptr } from "./pkg/par3.js";

const { memory } = wasmModule;
const encoder = new TextEncoder();

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
      onCancel(reason);
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

      chunks.push(value);
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
  if (response.status === 204) {
    return [];
  }

  const contentType = response.headers.get("content-type");
  assert.match(contentType ?? "", /boundary=/);
  const boundary = parseMultipartBoundary(contentType);
  const parser = new MultipartStreamReader(response.body, boundary);
  const parts = [];

  let hasNextPart = await parser.start();
  try {
    while (hasNextPart) {
      const part = await parser.readHeaders();
      const chunks = [];
      let totalLength = 0;

      hasNextPart = await parser.readPartBody(async (chunk) => {
        chunks.push(Uint8Array.from(chunk));
        totalLength += chunk.byteLength;
      });

      const bytes = new Uint8Array(totalLength);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }

      parts.push({
        name: part.name,
        bytes,
      });
    }
  } finally {
    parser.release();
  }

  return parts;
}

async function invokeMultipart(parts, seed) {
  const boundary = `----par3-${seed}`;
  const rng = createRng(seed ^ 0xa5a5a5a5);
  const requestBody = createStreamingMultipart(parts, boundary, rng);

  return worker.fetch(
    new Request("https://example.com/repair", {
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

async function invokeRawRequest({ body, contentType }) {
  return worker.fetch(
    new Request("https://example.com/repair", {
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
  return invokeMultipart(
    [
      {
        name: "metadata",
        contentType: "application/json",
        body: JSON.stringify(metadata),
      },
      ...parts,
    ],
    seed,
  );
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

  if ((seed & 1) === 0) {
    parts.push({
      name: "ignored_note",
      contentType: "text/plain",
      body: `seed:${seed}`,
    });
  }

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
      assert.equal(
        response.headers.get("x-par3-repaired-count"),
        String(testCase.requestedMissingIndices.length),
      );
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

test("returns 204 when omitted shards are not requested outputs", async (t) => {
  for (let seed = 101; seed <= 116; seed += 1) {
    await t.test(`seed ${seed}`, async () => {
      const testCase = await createRepairCase(seed, "none");
      const response = await invokeWorker(testCase.parts, testCase.metadata, seed);

      assert.equal(response.status, 204);
      assert.equal(await response.text(), "");
    });
  }
});

test("returns 400 when the multipart body never provides metadata", async () => {
  const response = await invokeMultipart(
    [{
      name: "ignored_note",
      contentType: "text/plain",
      body: "hello",
    }],
    140,
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata part is required/);
});

test("rejects shard parts that arrive before metadata", async () => {
  const boundary = "----par3-no-metadata";
  const body = createStreamingMultipart(
    [buildPart("shard_0", Uint8Array.from([0, 1, 2, 3]))],
    boundary,
    createRng(77),
  );

  const response = await worker.fetch(
    new Request("https://example.com/repair", {
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata part must arrive before shard parts/);
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

  assert.equal(response.status, 409);
  assert.match((await response.json()).error, /duplicate shard part received/);
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

  assert.equal(response.status, 422);
  assert.match((await response.json()).error, /repair threshold not reached/);
});

test("maps malformed multipart bodies to 400", async () => {
  const response = await worker.fetch(
    new Request("https://example.com/repair", {
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

  assert.equal(response.status, 400);
});

test("rejects multipart bodies that terminate before the first boundary", async () => {
  const response = await invokeRawRequest({
    contentType: "multipart/form-data; boundary=broken",
    body: "",
  });

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /multipart body terminated before the first boundary/);
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /forced multipart parse failure/);
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /forced string parse failure/);
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

      assert.equal(response.status, 400);
      assert.match((await response.json()).error, testCase.pattern);
    });
  }
});

test("rejects invalid JSON metadata", async () => {
  const response = await invokeMultipart(
    [{
      name: "metadata",
      contentType: "application/json",
      body: "{not-json",
    }],
    401,
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata is not valid JSON/);
});

test("rejects empty metadata parts", async () => {
  const boundary = "----par3-empty-metadata";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body:
      `--${boundary}\r\n` +
      'Content-Disposition: form-data; name="metadata"\r\n' +
      "Content-Type: application/json\r\n\r\n" +
      `\r\n--${boundary}--\r\n`,
  });

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata is not valid JSON/);
});

test("accepts unquoted content-disposition parameters", async () => {
  const boundary = "----par3-unquoted-params";
  const metadataHeader = encoder.encode(
    `--${boundary}\r\n` +
    "Content-Disposition: form-data; ignored=value; name=metadata\r\n" +
    "Content-Type: application/json\r\n\r\n" +
    JSON.stringify({
      original_count: 1,
      recovery_count: 1,
      shard_size: 4,
      missing_indices: [1],
    }) +
    `\r\n--${boundary}\r\n` +
    "Content-Disposition: form-data; name=shard_0; filename=shard_0.bin\r\n" +
    "Content-Type: application/octet-stream\r\n\r\n",
  );
  const shardBytes = Uint8Array.from([0, 1, 2, 3]);
  const closingBoundary = encoder.encode(`\r\n--${boundary}--\r\n`);
  const body = new Uint8Array(metadataHeader.byteLength + shardBytes.byteLength + closingBoundary.byteLength);
  body.set(metadataHeader);
  body.set(shardBytes, metadataHeader.byteLength);
  body.set(closingBoundary, metadataHeader.byteLength + shardBytes.byteLength);
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
  const metadata = JSON.stringify({
    original_count: 1,
    recovery_count: 1,
    shard_size: 4,
    missing_indices: [1],
  });
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
        'Content-Disposition: form-data; name="metadata"\r\n' +
        "Content-Type: application/json\r\n\r\n" +
        metadata,
      pattern: /multipart body terminated before the next boundary/,
    },
    {
      name: "truncated bytes after a boundary marker",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: form-data; name="metadata"\r\n' +
        "Content-Type: application/json\r\n\r\n" +
        `${metadata}\r\n--${boundary}`,
      pattern: /multipart body terminated after a part boundary/,
    },
    {
      name: "invalid separator after a part boundary",
      body:
        `--${boundary}\r\n` +
        'Content-Disposition: form-data; name="metadata"\r\n' +
        "Content-Type: application/json\r\n\r\n" +
        `${metadata}\r\n--${boundary}xx`,
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

      assert.equal(response.status, 400);
      assert.match((await response.json()).error, testCase.pattern);
    });
  }
});

test("accepts a multipart stream that closes immediately with the final boundary", async () => {
  const boundary = "----par3-empty-multipart";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: `--${boundary}--\r\n`,
  });

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata part is required/);
});

test("accepts an immediate final boundary without a trailing CRLF", async () => {
  const boundary = "----par3-empty-multipart-no-crlf";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: `--${boundary}--`,
  });

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata part is required/);
});

test("accepts a final multipart boundary without a trailing CRLF", async () => {
  const boundary = "----par3-no-final-crlf";
  const response = await invokeRawRequest({
    contentType: `multipart/form-data; boundary=${boundary}`,
    body: encodeMultipartBytes(
      [
        {
          name: "metadata",
          contentType: "application/json",
          body: JSON.stringify({
            original_count: 1,
            recovery_count: 1,
            shard_size: 4,
            missing_indices: [1],
          }),
        },
        buildPart("shard_0", Uint8Array.from([0, 1, 2, 3])),
      ],
      boundary,
      { trailingCrlf: false },
    ),
  });

  const repairedParts = await collectResponseParts(response);
  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
});

test("rejects invalid metadata fields", async (t) => {
  const cases = [
    {
      name: "original_count below minimum",
      metadata: { original_count: 0, recovery_count: 1, shard_size: 4, missing_indices: [1] },
      pattern: /original_count must be an integer greater than or equal to 1/,
    },
    {
      name: "recovery_count below minimum",
      metadata: { original_count: 2, recovery_count: -1, shard_size: 4, missing_indices: [1] },
      pattern: /recovery_count must be an integer greater than or equal to 0/,
    },
    {
      name: "odd shard_size",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 3, missing_indices: [1] },
      pattern: /shard_size must be even/,
    },
    {
      name: "missing_indices must be an array",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: "1" },
      pattern: /missing_indices must be an array of integer slot indexes/,
    },
    {
      name: "missing_indices[] below minimum",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: [-1] },
      pattern: /missing_indices\[\] must be an integer greater than or equal to 0/,
    },
    {
      name: "digests must be an array",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: [1], digests: "bad" },
      pattern: /digests must be an array of SHA-256 hex strings or null values/,
    },
    {
      name: "digests must match slot count",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: [1], digests: [null] },
      pattern: /digests must contain exactly 3 entries/,
    },
    {
      name: "digests entries must be SHA-256 hex",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: [1], digests: [null, "abc", null] },
      pattern: /digests\[1\] must be a 64-character hexadecimal SHA-256 digest or null/,
    },
    {
      name: "digests entries must be strings when present",
      metadata: { original_count: 2, recovery_count: 1, shard_size: 4, missing_indices: [1], digests: [null, 123, null] },
      pattern: /digests\[1\] must be a 64-character hexadecimal SHA-256 digest or null/,
    },
  ];

  for (const [offset, testCase] of cases.entries()) {
    await t.test(testCase.name, async () => {
      const response = await invokeWorker([], testCase.metadata, 410 + offset);

      assert.equal(response.status, 400);
      assert.match((await response.json()).error, testCase.pattern);
    });
  }
});

test("rejects duplicate metadata parts", async () => {
  const metadata = JSON.stringify({
    original_count: 2,
    recovery_count: 1,
    shard_size: 4,
    missing_indices: [1],
  });
  const response = await invokeMultipart(
    [
      { name: "metadata", contentType: "application/json", body: metadata },
      { name: "metadata", contentType: "application/json", body: metadata },
    ],
    420,
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /metadata part may only appear once/);
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /exceeds declared shard_size 4/);
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /wrote 2 bytes but shard_size is 4/);
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
  assert.match((await response.json()).error, /metadata must declare recovery_count or total_shard_count/);
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

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /outside declared total_shard_count/);
});

test("rejects POST requests without a body", async () => {
  const response = await worker.fetch(
    new Request("https://example.com/repair", {
      method: "POST",
    }),
    {},
    {},
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /request body is required/);
});

test("releases response resources when the caller cancels a multipart response", async () => {
  const testCase = await createRepairCase(450, "all");
  const response = await invokeWorker(testCase.parts, testCase.metadata, 450);

  assert.equal(response.status, 200);
  await response.body.cancel();
});

test("releases response resources when streaming a shard throws", { concurrency: false }, async (t) => {
  const testCase = await createRepairCase(451, "all");

  t.mock.method(Par3.prototype, "readShard", () => {
    throw new Error("forced shard read failure");
  });

  const response = await invokeWorker(testCase.parts, testCase.metadata, 451);

  assert.equal(response.status, 200);
  await assert.rejects(() => readStream(response.body), /forced shard read failure/);
});

test("rejects oversized metadata parts before buffering them entirely", async () => {
  const response = await invokeMultipart(
    [{
      name: "metadata",
      contentType: "application/json",
      body: JSON.stringify({
        original_count: 2,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
        note: "x".repeat(70 * 1024),
      }),
    }],
    470,
  );

  assert.equal(response.status, 413);
  assert.match((await response.json()).error, /metadata exceeds maximum size of 65536 bytes/);
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

test("rejects shard parts whose digest does not match metadata", async () => {
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
      digests: ["00".repeat(32), null, await sha256Hex(recoveryShards[0])],
    },
    472,
  );

  assert.equal(response.status, 422);
  assert.match((await response.json()).error, /digest mismatch for slot 0/);
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
        /requested missing index \d+ was already provided or is outside the inferred missing set/,
      );
    });
  }
});

test("stops pulling request chunks once the repair threshold is satisfied", async () => {
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
  const parts = [
    {
      name: "metadata",
      contentType: "application/json",
      body: JSON.stringify({
        original_count: 2,
        recovery_count: 1,
        shard_size: 4,
        missing_indices: [1],
      }),
    },
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
      onChunk() {
        pulledChunks += 1;
      },
    },
  );

  const response = await worker.fetch(
    new Request("https://example.com/repair", {
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
  assert.equal(cancellationReason, "repair threshold reached");
  assert.ok(pulledChunksAtResolution < totalPullableChunks);

  await wait(100);

  assert.equal(pulledChunks, pulledChunksAtResolution);
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
