import assert from "node:assert/strict";
import test from "node:test";

import {
  MAX_PART_HEADER_BYTES,
  MultipartParseError,
  MultipartStreamReader,
  parseMultipartBoundary,
} from "./multipart.js";

const encoder = new TextEncoder();

function buildMultipartBytes(parts, boundary, { trailingCrlf = true } = {}) {
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

function streamFromChunks(chunks, { onCancel = () => {} } = {}) {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
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
  const reader = new MultipartStreamReader(stream, boundary);
  const parts = [];
  let hasNextPart = await reader.start();

  try {
    while (hasNextPart) {
      const part = await reader.readHeaders();
      const chunks = [];
      hasNextPart = await reader.readPartBody(async (chunk) => {
        chunks.push(Uint8Array.from(chunk));
      });
      parts.push({
        name: part.name,
        body: Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))),
      });
    }
  } finally {
    reader.release();
  }

  return parts;
}

test("parseMultipartBoundary accepts quoted boundaries and rejects invalid content types", () => {
  assert.equal(parseMultipartBoundary('multipart/form-data; boundary="abc123"'), "abc123");
  assert.equal(parseMultipartBoundary("multipart/form-data; charset=utf-8; boundary=plain"), "plain");

  assert.throws(() => parseMultipartBoundary(null), MultipartParseError);
  assert.throws(() => parseMultipartBoundary("text/plain"), /multipart\/form-data/);
  assert.throws(() => parseMultipartBoundary("multipart/form-data"), /boundary/);
});

test("MultipartStreamReader reads multipart parts across chunk boundaries", async () => {
  const boundary = "--parts";
  const bytes = buildMultipartBytes(
    [
      { name: "metadata", contentType: "application/json", body: '{"ok":true}' },
      { name: "shard_0", filename: "shard_0.bin", contentType: "application/octet-stream", body: Uint8Array.from([1, 2, 3, 4]) },
    ],
    boundary,
  );

  const chunks = [
    bytes.subarray(0, 7),
    bytes.subarray(7, 19),
    bytes.subarray(19, 41),
    bytes.subarray(41),
  ];
  const parts = await readAllParts(streamFromChunks(chunks), boundary);

  assert.deepEqual(
    parts.map((part) => part.name),
    ["metadata", "shard_0"],
  );
  assert.deepEqual(parts[0].body, encoder.encode('{"ok":true}'));
  assert.deepEqual(parts[1].body, Uint8Array.from([1, 2, 3, 4]));
});

test("MultipartStreamReader can skip a part body and continue parsing", async () => {
  const boundary = "--skip";
  const bytes = buildMultipartBytes(
    [
      { name: "metadata", contentType: "application/json", body: '{"skip":true}' },
      { name: "shard_1", filename: "shard_1.bin", contentType: "application/octet-stream", body: Uint8Array.from([9, 8, 7, 6]) },
    ],
    boundary,
  );
  const reader = new MultipartStreamReader(streamFromChunks([bytes]), boundary);
  const names = [];

  try {
    let hasNextPart = await reader.start();
    while (hasNextPart) {
      const part = await reader.readHeaders();
      names.push(part.name);
      hasNextPart = await reader.readPartBody(async () => {});
    }
  } finally {
    reader.release();
  }

  assert.deepEqual(names, ["metadata", "shard_1"]);
});

test("MultipartStreamReader cancel delegates to the underlying stream", async () => {
  const boundary = "--cancel";
  let cancelledWith = null;
  const reader = new MultipartStreamReader(streamFromChunks([], {
    onCancel(reason) {
      cancelledWith = reason;
    },
  }), boundary);

  try {
    await reader.cancel("stop early");
  } finally {
    reader.release();
  }

  assert.equal(cancelledWith, "stop early");
});

test("MultipartStreamReader rejects malformed multipart state", async () => {
  const malformedHeader = streamFromChunks([
    encoder.encode("----bad\r\nContent-Disposition\r\n\r\n{}\r\n----bad--\r\n"),
  ]);
  const malformedReader = new MultipartStreamReader(malformedHeader, "--bad");

  await assert.rejects(async () => {
    await malformedReader.start();
    await malformedReader.readHeaders();
  }, /multipart header is malformed/);
  malformedReader.release();

  const oversizedHeader = streamFromChunks([
    encoder.encode(`----huge\r\nX-Test: ${"x".repeat(MAX_PART_HEADER_BYTES + 64)}`),
  ]);
  const oversizedReader = new MultipartStreamReader(oversizedHeader, "--huge");

  await assert.rejects(async () => {
    await oversizedReader.start();
    await oversizedReader.readHeaders();
  }, /multipart headers exceed maximum size/);
  oversizedReader.release();

  const truncatedBoundary = streamFromChunks([
    encoder.encode("----edge\r\nContent-Disposition: form-data; name=metadata\r\n\r\n{}\r\n----edge"),
  ]);
  const truncatedReader = new MultipartStreamReader(truncatedBoundary, "--edge");

  await assert.rejects(async () => {
    await truncatedReader.start();
    await truncatedReader.readHeaders();
    await truncatedReader.readPartBody(async () => {});
  }, /multipart body terminated after a part boundary/);
  truncatedReader.release();
});