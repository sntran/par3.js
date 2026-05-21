/**
 * Fetch/encode/repair/gzip example Worker.
 *
 * This module is a self-contained example entrypoint that reuses `_worker.js` as an internal
 * microservice. A single upstream download is split into fixed-size shards, streamed into
 * `POST /` to generate recovery shards, then partially replayed into `PATCH /` after the example
 * intentionally omits the last one or two original shards. The repaired tail is written directly
 * into `CompressionStream("gzip")`, so backpressure can flow from the browser all the way back to
 * the upstream fetch.
 *
 * Local run:
 *   npm run build:wasm
 *   npx wrangler dev ./examples/fetch_encode_repair_gzip.js --local
 *
 * Open Wrangler's local URL, submit a remote asset URL that returns `Content-Length`, and the
 * example will stream back a `.gz` file reconstructed through the repair path.
 */
import worker from "#worker";
import {
  encodeBoundary,
  encodeClosingBoundary,
  encodePart,
  getMultipartPartName,
  MultipartDecoder,
  parseMultipartBoundary,
} from "#lib/multipart.js";

const encoder = new TextEncoder();
const DEFAULT_RECOVERY_COUNT = 2;
const DEFAULT_SHARD_SIZE = 64 * 1024;
const DEFAULT_SOURCE_URL = "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4";
const SHARD_PART_NAME = /^shard_(\d+)$/u;
const TRAILING_CRLF = encoder.encode("\r\n");

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

function appendBytes(left, right) {
  const normalizedRight = toUint8Array(right);
  const merged = new Uint8Array(left.byteLength + normalizedRight.byteLength);
  merged.set(left);
  merged.set(normalizedRight, left.byteLength);
  return merged;
}

// Node's internal Request/WritableStream plumbing is happiest here when body chunks are plain
// array-like values instead of borrowed typed-array views.
function toRequestChunk(bytes) {
  return Array.from(toUint8Array(bytes));
}

function shardHeaders(slotIndex) {
  return {
    "Content-Disposition": `form-data; name="shard_${slotIndex}"; filename="shard_${slotIndex}.bin"`,
    "Content-Type": "application/octet-stream",
  };
}

function resolveShardIndex(partName) {
  const match = SHARD_PART_NAME.exec(partName);

  if (!match) {
    throw new Error(`unexpected shard part ${partName}`);
  }

  return Number.parseInt(match[1], 10);
}

// The example forces the repair leg to do real work by omitting the last one or two originals.
function selectDroppedIndices(originalCount, recoveryCount) {
  const dropCount = Math.min(Math.max(1, Math.min(recoveryCount, 2)), originalCount);
  const firstDroppedIndex = originalCount - dropCount;

  return Array.from({ length: dropCount }, (_, offset) => firstDroppedIndex + offset);
}

function normalizeContentLength(rawValue) {
  if (!rawValue) {
    throw new Error("upstream response must include a Content-Length header for the example pipeline");
  }

  const contentLength = Number.parseInt(rawValue, 10);

  if (!Number.isSafeInteger(contentLength) || contentLength <= 0) {
    throw new Error(`invalid upstream Content-Length ${rawValue}`);
  }

  return contentLength;
}

function gzipFileName(sourceUrl) {
  const pathName = new URL(sourceUrl).pathname;
  const fileName = pathName.split("/").filter(Boolean).at(-1) ?? "download.bin";
  return `${fileName}.gz`;
}

function renderForm(sourceUrl = DEFAULT_SOURCE_URL) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>par3 fetch/repair/gzip example</title>
    <style>
      :root {
        color-scheme: light;
        --ink: #18232f;
        --paper: #f7f2e8;
        --accent: #0d7a73;
        --accent-soft: #d7efe7;
        --line: #b8ab96;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: Georgia, "Iowan Old Style", serif;
        background:
          radial-gradient(circle at top left, rgba(13, 122, 115, 0.15), transparent 30%),
          linear-gradient(135deg, #efe6d6, var(--paper));
        color: var(--ink);
        display: grid;
        place-items: center;
        padding: 2rem;
      }

      main {
        width: min(720px, 100%);
        background: rgba(255, 255, 255, 0.82);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 2rem;
        box-shadow: 0 30px 80px rgba(24, 35, 47, 0.12);
        backdrop-filter: blur(10px);
      }

      h1 {
        margin: 0 0 0.75rem;
        font-size: clamp(2rem, 4vw, 3.25rem);
        line-height: 1;
      }

      p {
        margin: 0 0 1rem;
        line-height: 1.6;
      }

      form {
        display: grid;
        gap: 1rem;
        margin-top: 1.5rem;
      }

      label {
        display: grid;
        gap: 0.5rem;
        font-weight: 600;
      }

      input,
      button {
        font: inherit;
      }

      input {
        width: 100%;
        padding: 0.95rem 1rem;
        border: 1px solid var(--line);
        border-radius: 14px;
        background: #fffdf7;
      }

      button {
        border: 0;
        border-radius: 999px;
        padding: 0.9rem 1.4rem;
        background: var(--accent);
        color: white;
        font-weight: 700;
        cursor: pointer;
        justify-self: start;
      }

      code {
        background: var(--accent-soft);
        border-radius: 8px;
        padding: 0.15rem 0.4rem;
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Fetch, Repair, and Gzip Example</h1>
      <p>Fetch a remote file once, stream every original shard into <code>POST /</code>, intentionally omit the last one or two originals from <code>PATCH /</code>, then write the repaired tail directly into gzip as it comes back.</p>
      <form method="post">
        <label>
          Remote URL
          <input type="url" name="url" value="${sourceUrl}" required>
        </label>
        <button type="submit">Run Pipeline</button>
      </form>
    </main>
  </body>
</html>`;
}

// Encode requests can use the higher-level multipart helper because that request runs to normal
// completion and does not need to recover from an early consumer shutdown.
async function writeMultipartPart(writer, boundary, slotIndex, body) {
  await writer.write(toRequestChunk(encodeBoundary(boundary)));

  for await (const chunk of encodePart(shardHeaders(slotIndex), body)) {
    await writer.write(toRequestChunk(chunk));
  }
}

async function closeMultipartWriter(writer, boundary) {
  await writer.write(toRequestChunk(encodeClosingBoundary(boundary)));
  await writer.close();
}

async function abortWriter(writer, error) {
  await writer.abort(error).catch(() => undefined);
}

function encodePartHeaderBlock(headers) {
  const normalizedHeaders = headers instanceof Headers
    ? Array.from(headers.entries())
    : Object.entries(headers ?? {});
  let block = "";

  for (const [name, value] of normalizedHeaders) {
    block += `${name}: ${value}\r\n`;
  }

  return encoder.encode(`${block}\r\n`);
}

// Once the repair request has produced every requested missing shard, the writable side may close
// before the encode leg finishes forwarding all recovery shards. Those terminal states are normal.
function isExpectedRepairCancellation(error) {
  const code = error && typeof error === "object" && "code" in error
    ? String(error.code)
    : "";
  const name = error instanceof Error ? error.name : "";
  const message = error instanceof Error ? error.message : String(error);

  if (name === "AbortError" || code === "ERR_INVALID_STATE") {
    return true;
  }

  return /repair threshold reached|AbortError|WritableStream is closed|already closed|Invalid state/u.test(message)
    || (error instanceof TypeError && /closed|closing|terminal state/u.test(message));
}

async function discardReadableStream(stream) {
  const reader = stream.getReader();

  try {
    while (true) {
      const { done } = await reader.read();

      if (done) {
        break;
      }
    }
  } catch {
    // Ignore drain races after downstream cancellation.
  } finally {
    reader.releaseLock();
  }
}

async function writeRepairMultipartBody(writer, body) {
  if (body === undefined || body === null) {
    return true;
  }

  if (!(body instanceof ReadableStream)) {
    await writer.write(toRequestChunk(body));
    return true;
  }

  // The repair leg is the only place where a downstream early-close is expected. If that happens,
  // we keep draining the source stream so the upstream producer can finish cleanly.
  const reader = body.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        return true;
      }

      try {
        await writer.write(toRequestChunk(value));
      } catch (error) {
        if (!isExpectedRepairCancellation(error)) {
          throw error;
        }

        while (true) {
          const { done: drained } = await reader.read();

          if (drained) {
            break;
          }
        }

        return false;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function writeRepairMultipartPart(writer, boundary, slotIndex, body) {
  try {
    await writer.write(toRequestChunk(encodeBoundary(boundary)));
    await writer.write(toRequestChunk(encodePartHeaderBlock(shardHeaders(slotIndex))));

    const wroteBody = await writeRepairMultipartBody(writer, body);

    if (!wroteBody) {
      return false;
    }

    await writer.write(toRequestChunk(TRAILING_CRLF));
    return true;
  } catch (error) {
    if (isExpectedRepairCancellation(error)) {
      if (body instanceof ReadableStream) {
        await discardReadableStream(body);
      }

      return false;
    }

    throw error;
  }
}

async function closeRepairWriter(writer, boundary) {
  try {
    await closeMultipartWriter(writer, boundary);
    return true;
  } catch (error) {
    if (!isExpectedRepairCancellation(error)) {
      throw error;
    }

    return false;
  }
}

async function writeGzipChunk(writer, chunk, contentLength, emittedBytes) {
  const chunkBytes = toUint8Array(chunk);
  const remainingBytes = contentLength - emittedBytes;

  if (remainingBytes <= 0) {
    return emittedBytes;
  }

  const writeBytes = chunkBytes.byteLength <= remainingBytes
    ? chunkBytes
    : chunkBytes.subarray(0, remainingBytes);

  if (writeBytes.byteLength !== 0) {
    await writer.write(Uint8Array.from(writeBytes));
    return emittedBytes + writeBytes.byteLength;
  }

  return emittedBytes;
}

// Fan the upstream response into both internal requests while immediately sending the intact prefix
// into gzip. The dropped originals are skipped here and reconstructed later by the repair leg.
async function streamSourceIntoPipelines({
  contentLength,
  droppedIndices,
  encodeBoundaryToken,
  encodeWriter,
  gzipWriter,
  originalCount,
  repairBoundaryToken,
  repairWriter,
  shardSize,
  sourceBody,
}) {
  const droppedIndexSet = new Set(droppedIndices);
  let buffer = new Uint8Array(0);
  let emittedBytes = 0;
  let repairInputOpen = true;
  let shardIndex = 0;

  const handleShard = async (shardBytes, currentShardIndex) => {
    // Every original shard always feeds encode so recovery shards can be generated.
    await writeMultipartPart(
      encodeWriter,
      encodeBoundaryToken,
      currentShardIndex,
      Uint8Array.from(shardBytes),
    );

    if (repairInputOpen && !droppedIndexSet.has(currentShardIndex)) {
      // Surviving originals also feed the repair request and can be forwarded straight into gzip.
      repairInputOpen = await writeRepairMultipartPart(
        repairWriter,
        repairBoundaryToken,
        currentShardIndex,
        Uint8Array.from(shardBytes),
      );
      emittedBytes = await writeGzipChunk(gzipWriter, shardBytes, contentLength, emittedBytes);
    } else if (!droppedIndexSet.has(currentShardIndex)) {
      emittedBytes = await writeGzipChunk(gzipWriter, shardBytes, contentLength, emittedBytes);
    }
  };

  for await (const chunk of sourceBody) {
    buffer = appendBytes(buffer, chunk);

    while (buffer.byteLength >= shardSize) {
      await handleShard(buffer.subarray(0, shardSize), shardIndex);
      shardIndex += 1;
      buffer = buffer.subarray(shardSize);
    }
  }

  if (buffer.byteLength > 0 || shardIndex < originalCount) {
    const finalShard = new Uint8Array(shardSize);
    finalShard.set(buffer);
    await handleShard(finalShard, shardIndex);
    shardIndex += 1;
  }

  if (shardIndex !== originalCount) {
    throw new Error(
      `shard stream produced ${shardIndex} shard(s) but expected ${originalCount}`,
    );
  }

  return {
    emittedBytes,
    repairInputOpen,
  };
}

// Relay the recovery shards generated by POST / into PATCH /. This keeps the example fully
// streaming: repair starts consuming recovery bytes as soon as encode produces them.
async function streamRecoveryIntoRepair(encodeResponse, repairWriter, repairBoundaryToken, repairInputOpen) {
  if (!encodeResponse.ok) {
    throw new Error(await encodeResponse.text());
  }

  if (!encodeResponse.body) {
    throw new Error("encode response body is required");
  }

  const boundary = parseMultipartBoundary(encodeResponse.headers.get("content-type"));
  const encodedParts = encodeResponse.body.pipeThrough(MultipartDecoder.create({ boundary }));

  try {
    for await (const part of encodedParts) {
      if (!repairInputOpen) {
        break;
      }

      const partName = getMultipartPartName(part.headers);
      const slotIndex = resolveShardIndex(partName);
      repairInputOpen = await writeRepairMultipartPart(
        repairWriter,
        repairBoundaryToken,
        slotIndex,
        part.body,
      );

      if (!repairInputOpen) {
        break;
      }
    }
  } catch (error) {
    if (!repairInputOpen && isExpectedRepairCancellation(error)) {
      return false;
    }

    throw error;
  }

  return repairInputOpen;
}

// Append the missing tail shards returned by PATCH / to the gzip stream in the exact order the
// example declared in the repair request.
async function streamRepairedTailToGzip({
  contentLength,
  droppedIndices,
  gzipWriter,
  repairResponse,
  startingBytes,
}) {
  if (!repairResponse.ok) {
    throw new Error(await repairResponse.text());
  }

  if (!repairResponse.body) {
    throw new Error("repair response body is required");
  }

  const expectedIndices = droppedIndices.toSorted((left, right) => left - right);
  const boundary = parseMultipartBoundary(repairResponse.headers.get("content-type"));
  const repairedParts = repairResponse.body.pipeThrough(MultipartDecoder.create({ boundary }));
  let emittedBytes = startingBytes;
  let repairedCount = 0;

  for await (const part of repairedParts) {
    const partName = getMultipartPartName(part.headers);
    const slotIndex = resolveShardIndex(partName);
    const expectedIndex = expectedIndices[repairedCount];

    if (slotIndex !== expectedIndex) {
      throw new Error(`expected repaired shard ${expectedIndex} but received ${slotIndex}`);
    }

    for await (const chunk of part.body) {
      emittedBytes = await writeGzipChunk(gzipWriter, toRequestChunk(chunk), contentLength, emittedBytes);
    }

    repairedCount += 1;
  }

  if (repairedCount !== expectedIndices.length) {
    throw new Error(
      `received ${repairedCount} repaired shard(s) but expected ${expectedIndices.length}`,
    );
  }

  return emittedBytes;
}

function createInternalUrl(searchParams) {
  const url = new URL("https://internal.example/");
  url.search = searchParams.toString();
  return url;
}

// Build the three-stream pipeline and return the gzip readable immediately. The caller can start
// consuming the response body while the background promise continues feeding encode and repair.
async function startPipeline(sourceUrl, env, ctx) {
  const upstreamResponse = await fetch(sourceUrl);

  if (!upstreamResponse.ok || !upstreamResponse.body) {
    throw new Error(`Upstream fetch failed with ${upstreamResponse.status}`);
  }

  const contentLength = normalizeContentLength(upstreamResponse.headers.get("content-length"));
  const shardSize = DEFAULT_SHARD_SIZE;
  const originalCount = Math.ceil(contentLength / shardSize);
  const recoveryCount = Math.min(Math.max(DEFAULT_RECOVERY_COUNT, 1), originalCount);
  const droppedIndices = selectDroppedIndices(originalCount, recoveryCount);
  const encodeBoundaryToken = `demo-encode-${crypto.randomUUID()}`;
  const repairBoundaryToken = `demo-repair-${crypto.randomUUID()}`;
  const encodeStream = new TransformStream();
  const repairStream = new TransformStream();
  const compression = new CompressionStream("gzip");
  const encodeWriter = encodeStream.writable.getWriter();
  const repairWriter = repairStream.writable.getWriter();
  const gzipWriter = compression.writable.getWriter();
  const layoutSearch = new URLSearchParams({
    n: String(originalCount),
    r: String(recoveryCount),
    s: String(shardSize),
  });
  // Both internal worker requests are started up front so their readable sides can apply
  // backpressure as soon as the first shard bytes arrive.
  const encodeResponsePromise = worker.fetch(
    new Request(createInternalUrl(layoutSearch), {
      method: "POST",
      headers: {
        "content-type": `multipart/form-data; boundary=${encodeBoundaryToken}`,
      },
      body: encodeStream.readable,
      duplex: "half",
    }),
    env,
    ctx,
  );
  const repairSearch = new URLSearchParams(layoutSearch);

  for (const droppedIndex of droppedIndices) {
    repairSearch.append("i", String(droppedIndex));
  }

  const repairResponsePromise = worker.fetch(
    new Request(createInternalUrl(repairSearch), {
      method: "PATCH",
      headers: {
        "content-type": `multipart/form-data; boundary=${repairBoundaryToken}`,
      },
      body: repairStream.readable,
      duplex: "half",
    }),
    env,
    ctx,
  );

  const pipelinePromise = (async () => {
    try {
      const {
        emittedBytes: emittedPrefixBytes,
        repairInputOpen: repairStillOpenAfterSource,
      } = await streamSourceIntoPipelines({
        contentLength,
        droppedIndices,
        encodeBoundaryToken,
        encodeWriter,
        gzipWriter,
        originalCount,
        repairBoundaryToken,
        repairWriter,
        shardSize,
        sourceBody: upstreamResponse.body,
      });

      await closeMultipartWriter(encodeWriter, encodeBoundaryToken);
      const repairStillOpenAfterRecovery = await streamRecoveryIntoRepair(
        await encodeResponsePromise,
        repairWriter,
        repairBoundaryToken,
        repairStillOpenAfterSource,
      );
      if (repairStillOpenAfterRecovery) {
        await closeRepairWriter(repairWriter, repairBoundaryToken);
      }

      const emittedBytes = await streamRepairedTailToGzip({
        contentLength,
        droppedIndices,
        gzipWriter,
        repairResponse: await repairResponsePromise,
        startingBytes: emittedPrefixBytes,
      });

      if (emittedBytes !== contentLength) {
        throw new Error(`rebuilt ${emittedBytes} bytes but expected ${contentLength}`);
      }

      await gzipWriter.close();
    } catch (error) {
      await Promise.allSettled([
        abortWriter(encodeWriter, error),
        abortWriter(repairWriter, error),
        gzipWriter.abort(error).catch(() => undefined),
      ]);
      throw error;
    } finally {
      encodeWriter.releaseLock();
      repairWriter.releaseLock();
      gzipWriter.releaseLock();
    }
  })();

  return {
    contentLength,
    droppedIndices,
    fileName: gzipFileName(sourceUrl),
    pipelinePromise,
    readable: compression.readable,
  };
}

export default {
  async fetch(request, env, ctx) {
    const requestUrl = new URL(request.url);

    // GET renders a tiny form so the example is easy to try from a browser.
    if (request.method === "GET" && requestUrl.pathname === "/") {
      return new Response(renderForm(), {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // POST starts the streaming pipeline and returns the gzip stream immediately.
    if (request.method !== "POST" || requestUrl.pathname !== "/") {
      return new Response("Not Found", { status: 404 });
    }

    try {
      const formData = await request.formData();
      const sourceUrl = String(formData.get("url") ?? DEFAULT_SOURCE_URL).trim() || DEFAULT_SOURCE_URL;
      const { droppedIndices, fileName, pipelinePromise, readable } = await startPipeline(sourceUrl, env, ctx);

      if (typeof ctx?.waitUntil === "function") {
        ctx.waitUntil(pipelinePromise.catch(() => undefined));
      } else {
        void pipelinePromise.catch(() => undefined);
      }

      return new Response(readable, {
        headers: {
          "cache-control": "no-store",
          "content-disposition": `attachment; filename="${fileName}"`,
          "content-type": "application/gzip",
          "x-demo-dropped-shards": droppedIndices.join(","),
        },
      });
    } catch (error) {
      return new Response(error instanceof Error ? error.message : String(error), { status: 500 });
    }
  },
};