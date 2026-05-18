import assert from "node:assert/strict";
import test from "node:test";

import { MultipartParseError, parseMultipart } from "@mjackson/multipart-parser";

import worker from "./_worker.js";
import * as wasmModule from "./pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode } from "./pkg/par3.js";

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
  const arenaPtr = alloc_shard_arena(slotCount, shardSize);

  try {
    const bytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);
    for (let index = 0; index < originalCount; index += 1) {
      bytes.set(originalShards[index], index * shardSize);
    }

    leopard_encode(originalCount, shardSize, arenaPtr);

    const encodedBytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);

    return Array.from({ length: recoveryCount }, (_, recoveryIndex) =>
      Uint8Array.from(shardAt(encodedBytes, shardSize, originalCount + recoveryIndex)),
    );
  } finally {
    free_shard_arena(arenaPtr);
  }
}

async function collectResponseParts(response) {
  if (response.status === 204) {
    return [];
  }

  const contentType = response.headers.get("content-type");
  assert.match(contentType ?? "", /boundary=/);
  const boundary = contentType.split("boundary=")[1];
  const parts = [];

  for await (const part of parseMultipart(response.body, boundary)) {
    parts.push({
      name: part.name,
      bytes: await readStream(part.body),
    });
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

test("maps multipart parser failures to 400", async () => {
  const response = await worker.fetch(
    new Request("https://example.com/repair", {
      method: "POST",
      headers: {
        "content-type": "multipart/form-data; boundary=broken",
      },
      body: new ReadableStream({
        start(controller) {
          controller.error(new MultipartParseError("forced multipart parse failure"));
        },
      }),
      duplex: "half",
    }),
    {},
    {},
  );

  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /forced multipart parse failure/);
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

test("expands slot count when recovery_count is inferred from streamed shard indexes", async () => {
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
      missing_indices: [1],
    },
    440,
  );

  const repairedParts = await collectResponseParts(response);
  assert.equal(response.status, 200);
  assert.deepEqual(repairedParts.map((part) => part.name), ["shard_1"]);
  assert.deepEqual(repairedParts[0].bytes, originalShards[1]);
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
