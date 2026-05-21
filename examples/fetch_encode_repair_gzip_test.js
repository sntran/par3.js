import assert from "node:assert/strict";
import test from "node:test";
import { gunzipSync } from "node:zlib";

import exampleWorker from "./fetch_encode_repair_gzip.js";

const DEMO_SHARD_SIZE = 64 * 1024;

function buildPayload(byteLength) {
  const bytes = new Uint8Array(byteLength);

  for (let index = 0; index < bytes.byteLength; index += 1) {
    bytes[index] = (index * 29 + 17) % 251;
  }

  return bytes;
}

function createChunkedStream(bytes, chunkSizes) {
  let chunkIndex = 0;
  let offset = 0;

  return new ReadableStream({
    pull(controller) {
      if (offset >= bytes.byteLength) {
        controller.close();
        return;
      }

      const chunkSize = chunkSizes[chunkIndex % chunkSizes.length];
      const nextOffset = Math.min(offset + chunkSize, bytes.byteLength);
      controller.enqueue(bytes.subarray(offset, nextOffset));
      offset = nextOffset;
      chunkIndex += 1;
    },
  });
}

test("fetch/encode/repair/gzip example absorbs closed repair writers during multi-shard repair cutoff", async () => {
  const payload = buildPayload(DEMO_SHARD_SIZE * 4 + 321);
  const sourceUrl = "https://origin.example/video.bin";
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url) => {
    assert.equal(String(url), sourceUrl);

    return new Response(createChunkedStream(payload, [8191, 257, 6553, 131071, 4093]), {
      status: 200,
      headers: {
        "content-length": String(payload.byteLength),
      },
    });
  };

  try {
    const formData = new FormData();
    formData.set("url", sourceUrl);

    const response = await exampleWorker.fetch(
      new Request("https://demo.example/", {
        method: "POST",
        body: formData,
      }),
      {},
      {},
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/gzip");
    assert.equal(response.headers.get("x-demo-dropped-shards"), "3,4");

    const gzipBytes = new Uint8Array(await response.arrayBuffer());
    const decodedBytes = new Uint8Array(gunzipSync(gzipBytes));

    assert.deepEqual(decodedBytes, payload);
  } finally {
    globalThis.fetch = originalFetch;
  }
});