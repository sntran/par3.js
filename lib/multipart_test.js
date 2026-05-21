import assert from "node:assert/strict";
import test from "node:test";

import {
  encodeBoundary,
  encodeClosingBoundary,
  encodePart,
  getMultipartPartName,
  MAX_PART_HEADER_BYTES,
  MultipartDecoder,
  MultipartParseError,
  parseHeaderParameters,
  parseMultipartBoundary,
  parseMultipartPartHeaders,
} from "./multipart.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

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

function streamFromChunks(chunks) {
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
  });
}

async function readStream(stream) {
  const chunks = [];
  let totalLength = 0;

  for await (const chunk of stream) {
    const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    chunks.push(bytes);
    totalLength += bytes.byteLength;
  }

  const merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return merged;
}

async function collectDecodedParts(stream, boundary) {
  const parts = [];
  const decodedStream = stream.pipeThrough(MultipartDecoder.create({ boundary }));

  for await (const part of decodedStream) {
    parts.push({
      headers: Object.fromEntries(part.headers.entries()),
      name: getMultipartPartName(part.headers),
      bytes: new Uint8Array(await part.arrayBuffer()),
    });
  }

  return parts;
}

async function collectEncodedPart(iterable) {
  const chunks = [];
  let totalLength = 0;

  for await (const chunk of iterable) {
    chunks.push(chunk);
    totalLength += chunk.byteLength;
  }

  const merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return merged;
}

test("parseMultipartBoundary accepts quoted boundaries and rejects invalid content types", () => {
  assert.equal(parseMultipartBoundary('multipart/form-data; boundary="abc123"'), "abc123");
  assert.equal(parseMultipartBoundary("multipart/form-data; charset=utf-8; boundary=plain"), "plain");

  assert.throws(() => parseMultipartBoundary(null), MultipartParseError);
  assert.throws(() => parseMultipartBoundary("text/plain"), /multipart\/form-data/);
  assert.throws(() => parseMultipartBoundary("multipart/form-data"), /boundary/);
  assert.throws(() => parseMultipartBoundary('multipart/form-data; boundary=""'), /boundary/);
});

test("parseHeaderParameters normalizes keys and strips quotes", () => {
  const parsed = parseHeaderParameters('form-data; ignored; name="alpha"; filename=beta.bin');

  assert.equal(parsed.type, "form-data");
  assert.equal(parsed.parameters.get("name"), "alpha");
  assert.equal(parsed.parameters.get("filename"), "beta.bin");
});

test("MultipartDecoder yields Response parts across chunk boundaries", async () => {
  const boundary = "generic-parts";
  const bytes = buildMultipartBytes([
    {
      name: "text",
      contentType: "text/plain",
      body: "hello",
    },
    {
      name: "file",
      filename: "file.bin",
      contentType: "application/octet-stream",
      body: Uint8Array.from([1, 2, 3, 4]),
    },
  ], boundary);
  const parts = await collectDecodedParts(streamFromChunks([
    bytes.subarray(0, 5).buffer,
    bytes.subarray(5, 23),
    bytes.subarray(23, 51),
    bytes.subarray(51),
  ]), boundary);

  assert.deepEqual(parts.map((part) => part.name), ["text", "file"]);
  assert.deepEqual(parts[0].headers, {
    "content-disposition": 'form-data; name="text"',
    "content-type": "text/plain",
  });
  assert.equal(decoder.decode(parts[0].bytes), "hello");
  assert.deepEqual(parts[1].bytes, Uint8Array.from([1, 2, 3, 4]));
});

test("MultipartDecoder accepts unquoted content-disposition parameters", async () => {
  const boundary = "unquoted-params";
  const body = encoder.encode(
    `--${boundary}\r\n`
    + "Content-Disposition: form-data; ignored=value; name=alpha; filename=alpha.txt\r\n"
    + "Content-Type: text/plain\r\n\r\n"
    + "ok\r\n"
    + `--${boundary}--\r\n`,
  );
  const parts = await collectDecodedParts(streamFromChunks([body]), boundary);

  assert.deepEqual(parts.map((part) => part.name), ["alpha"]);
  assert.equal(decoder.decode(parts[0].bytes), "ok");
});

test("MultipartDecoder accepts split opening boundaries and ArrayBufferView chunks", async () => {
  const boundary = "split-boundary";
  const initialBoundaryLength = encoder.encode(`--${boundary}`).byteLength;
  const bytes = buildMultipartBytes([
    {
      name: "alpha",
      contentType: "text/plain",
      body: "ok",
    },
  ], boundary);
  const parts = await collectDecodedParts(streamFromChunks([
    new DataView(bytes.buffer, 0, 3),
    bytes.subarray(3, initialBoundaryLength),
    bytes.subarray(initialBoundaryLength),
  ]), boundary);

  assert.deepEqual(parts.map((part) => part.name), ["alpha"]);
  assert.equal(decoder.decode(parts[0].bytes), "ok");
});

test("MultipartDecoder accepts array-like chunks written directly to its writable side", async () => {
  const boundary = "array-like-input";
  const bytes = buildMultipartBytes([
    {
      name: "alpha",
      contentType: "text/plain",
      body: "fallback",
    },
  ], boundary);
  const parts = [];
  const decoded = streamFromChunks([Array.from(bytes)]).pipeThrough(MultipartDecoder.create({ boundary }));

  for await (const part of decoded) {
    parts.push({
      body: await part.text(),
      name: getMultipartPartName(part.headers),
    });
  }

  assert.deepEqual(parts, [{ name: "alpha", body: "fallback" }]);
});

test("MultipartDecoder accepts an immediate final boundary with and without a trailing CRLF", async () => {
  const withCrlf = await collectDecodedParts(
    streamFromChunks([encoder.encode("--empty--\r\n")]),
    "empty",
  );
  const withoutCrlf = await collectDecodedParts(
    streamFromChunks([encoder.encode("--empty-no-crlf--")]),
    "empty-no-crlf",
  );

  assert.deepEqual(withCrlf, []);
  assert.deepEqual(withoutCrlf, []);
});

test("MultipartDecoder preserves zero-length part bodies", async () => {
  const boundary = "empty-part";
  const parts = await collectDecodedParts(streamFromChunks([
    encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="empty"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + `\r\n--${boundary}--\r\n`,
    ),
  ]), boundary);

  assert.deepEqual(parts, [{
    headers: {
      "content-disposition": 'form-data; name="empty"',
      "content-type": "text/plain",
    },
    name: "empty",
    bytes: new Uint8Array(0),
  }]);
});

test("MultipartDecoder accepts a final boundary without a trailing CRLF after a part body", async () => {
  const boundary = "final-no-crlf";
  const parts = await collectDecodedParts(streamFromChunks([
    buildMultipartBytes([
      {
        name: "alpha",
        contentType: "text/plain",
        body: "ok",
      },
    ], boundary, { trailingCrlf: false }),
  ]), boundary);

  assert.deepEqual(parts.map((part) => part.name), ["alpha"]);
  assert.equal(decoder.decode(parts[0].bytes), "ok");
});

test("MultipartDecoder rejects malformed multipart state", async (t) => {
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
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=part\r\nContent-Type: text/plain\r\n\r\nok\r\n----edge")],
      boundary: "--edge",
      pattern: /multipart body terminated after a part boundary/,
    },
    {
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=part\r\nContent-Type: text/plain\r\n\r\nok\r\n----edge--oops")],
      boundary: "--edge",
      pattern: /multipart boundary must end with CRLF/,
    },
    {
      body: [encoder.encode("----edge\r\nContent-Disposition: form-data; name=part")],
      boundary: "--edge",
      pattern: /multipart body terminated in part headers/,
    },
    {
      body: [encoder.encode("----edge--oops")],
      boundary: "--edge",
      pattern: /multipart boundary must end with CRLF/,
    },
    {
      body: [encoder.encode("not-a-boundary")],
      boundary: "edge",
      pattern: /multipart body does not start with the declared boundary/,
    },
    {
      body: [encoder.encode("--edge\r\nContent-Type: text/plain\r\n\r\nok\r\n--edge--\r\n")],
      boundary: "edge",
      pattern: /multipart part is missing content-disposition/,
    },
    {
      body: [encoder.encode("--edge\r\nContent-Disposition: attachment; name=part\r\n\r\nok\r\n--edge--\r\n")],
      boundary: "edge",
      pattern: /multipart content-disposition must be form-data/,
    },
    {
      body: [encoder.encode("--edge\r\nContent-Disposition: form-data; ignored\r\n\r\nok\r\n--edge--\r\n")],
      boundary: "edge",
      pattern: /multipart part is missing a name/,
    },
  ];

  for (const [index, testCase] of cases.entries()) {
    await t.test(`case ${index + 1}`, async () => {
      await assert.rejects(
        () => collectDecodedParts(streamFromChunks(testCase.body), testCase.boundary),
        testCase.pattern,
      );
    });
  }
});

test("MultipartDecoder rejects truncated part bodies and malformed opening boundaries", async (t) => {
  const cases = [
    {
      name: "missing next boundary",
      body: encoder.encode(
        "--truncated\r\n"
        + "Content-Disposition: form-data; name=part\r\n"
        + "Content-Type: text/plain\r\n\r\n"
        + "ok",
      ),
      pattern: /multipart body terminated before the next boundary/,
    },
    {
      name: "opening boundary without CRLF",
      body: encoder.encode("--truncatedxx"),
      pattern: /multipart boundary must end with CRLF/,
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.name, async () => {
      await assert.rejects(
        () => collectDecodedParts(streamFromChunks([testCase.body]), "truncated"),
        testCase.pattern,
      );
    });
  }
});

test("MultipartDecoder tolerates canceled part bodies before surfacing parse failures", async () => {
  const boundary = "cancel-race";
  const bytes = encoder.encode(
    `--${boundary}\r\n`
    + 'Content-Disposition: form-data; name="part"\r\n'
    + 'Content-Type: text/plain\r\n\r\n'
    + 'hello',
  );
  const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
  const iterator = decoded.values({ preventCancel: true });
  const { done, value } = await iterator.next();

  assert.equal(done, false);
  await value.body.cancel("stop");
  await assert.rejects(() => iterator.next(), /multipart body terminated before the next boundary/);
});

test("MultipartDecoder ignores rejected part aborts while surfacing parse failures", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.reject(new Error("forced abort failure"));
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
    const boundary = "abort-reject";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello',
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(() => iterator.next(), /multipart body terminated before the next boundary/);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder ignores rejected part closes from already-cancelled consumers", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                const error = new TypeError("Invalid state: WritableStream is closed");
                error.code = "ERR_INVALID_STATE";
                return Promise.reject(error);
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
    const boundary = "close-race";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello',
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(() => iterator.next(), /multipart body terminated before the next boundary/);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder also ignores close races reported only by TypeError messages", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject(new TypeError("WritableStream is closed"));
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
    const boundary = "close-race-message";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello',
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(() => iterator.next(), /multipart body terminated before the next boundary/);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder also ignores close races surfaced as non-Error objects", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject({ code: "ERR_INVALID_STATE" });
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
    const boundary = "close-race-object";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello',
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(() => iterator.next(), /multipart body terminated before the next boundary/);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder ignores ERR_INVALID_STATE close races on normal part completion", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                const error = new TypeError("Invalid state: WritableStream is closed");
                error.code = "ERR_INVALID_STATE";
                return Promise.reject(error);
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
    const boundary = "close-race-complete-code";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello\r\n'
      + `--${boundary}--\r\n`,
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const first = await iterator.next();
    const second = await iterator.next();

    assert.equal(first.done, false);
    assert.equal(second.done, true);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder ignores TypeError close races on normal part completion", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject(new TypeError("WritableStream is closed"));
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
    const boundary = "close-race-complete-message";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello\r\n'
      + `--${boundary}--\r\n`,
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const first = await iterator.next();
    const second = await iterator.next();

    assert.equal(first.done, false);
    assert.equal(second.done, true);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder still surfaces unexpected part close failures", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject(new Error("forced close failure"));
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
    const boundary = "close-race-failure";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello\r\n'
      + `--${boundary}--\r\n`,
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(() => iterator.next(), /forced close failure/);
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder rethrows non-object part close failures", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject("forced close string");
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
    const boundary = "close-race-string";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello\r\n'
      + `--${boundary}--\r\n`,
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(
      () => iterator.next(),
      (error) => error === "forced close string",
    );
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("MultipartDecoder rethrows falsy part close failures", async () => {
  const originalTransformStream = globalThis.TransformStream;

  globalThis.TransformStream = class MockTransformStream {
    constructor() {
      return {
        readable: new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
        writable: {
          getWriter() {
            return {
              abort() {
                return Promise.resolve();
              },
              close() {
                return Promise.reject(null);
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
    const boundary = "close-race-null";
    const bytes = encoder.encode(
      `--${boundary}\r\n`
      + 'Content-Disposition: form-data; name="part"\r\n'
      + 'Content-Type: text/plain\r\n\r\n'
      + 'hello\r\n'
      + `--${boundary}--\r\n`,
    );
    const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));
    const iterator = decoded.values({ preventCancel: true });
    const { done } = await iterator.next();

    assert.equal(done, false);
    await assert.rejects(
      () => iterator.next(),
      (error) => error === null,
    );
  } finally {
    globalThis.TransformStream = originalTransformStream;
  }
});

test("parseMultipartPartHeaders validates multipart form-data requirements", () => {
  const headers = parseMultipartPartHeaders(
    "Content-Disposition: form-data; name=alpha\r\nContent-Type: text/plain",
  );

  assert.equal(getMultipartPartName(headers), "alpha");
  assert.equal(headers.get("content-type"), "text/plain");
});

test("getMultipartPartName accepts plain object header bags", () => {
  assert.equal(getMultipartPartName({
    "content-disposition": 'form-data; name="plain"',
  }), "plain");
});

test("encodeBoundary, encodePart, and encodeClosingBoundary generate multipart bytes", async () => {
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(Uint8Array.from([1, 2]));
      controller.enqueue(Uint8Array.from([3, 4]));
      controller.close();
    },
  });
  const encodedPart = await collectEncodedPart(encodePart({
    "content-disposition": 'form-data; name="file"; filename="file.bin"',
    "content-type": "application/octet-stream",
  }, body));
  const bytes = new Uint8Array(
    encodeBoundary("encoded").byteLength
    + encodedPart.byteLength
    + encodeClosingBoundary("encoded").byteLength,
  );
  let offset = 0;
  for (const chunk of [encodeBoundary("encoded"), encodedPart, encodeClosingBoundary("encoded")]) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  assert.equal(
    decoder.decode(bytes),
    "--encoded\r\n"
    + 'content-disposition: form-data; name="file"; filename="file.bin"\r\n'
    + "content-type: application/octet-stream\r\n\r\n"
    + "\u0001\u0002\u0003\u0004\r\n"
    + "--encoded--\r\n",
  );
});

test("encodePart accepts byte bodies and empty bodies", async () => {
  const bytesPart = await collectEncodedPart(encodePart({
    "content-disposition": 'form-data; name="bytes"',
  }, Uint8Array.from([7, 8]).buffer));
  const emptyPart = await collectEncodedPart(encodePart({
    "content-disposition": 'form-data; name="empty"',
  }));

  assert.equal(
    decoder.decode(bytesPart),
    'content-disposition: form-data; name="bytes"\r\n\r\n\u0007\b\r\n',
  );
  assert.equal(
    decoder.decode(emptyPart),
    'content-disposition: form-data; name="empty"\r\n\r\n\r\n',
  );
  assert.equal(decoder.decode(encodeClosingBoundary("encoded", { trailingCrlf: false })), "--encoded--");
});

test("encodePart accepts Headers instances", async () => {
  const part = await collectEncodedPart(encodePart(new Headers({
    "content-disposition": 'form-data; name="headers"',
  })));

  assert.equal(
    decoder.decode(part),
    'content-disposition: form-data; name="headers"\r\n\r\n\r\n',
  );
});

test("encodePart accepts missing headers", async () => {
  const part = await collectEncodedPart(encodePart(undefined));

  assert.equal(decoder.decode(part), "\r\n\r\n");
});

test("MultipartDecoder exposes part bodies as standard readable streams", async () => {
  const boundary = "stream-body";
  const bytes = buildMultipartBytes([
    {
      name: "alpha",
      contentType: "text/plain",
      body: "streamed",
    },
  ], boundary);
  const decoded = streamFromChunks([bytes]).pipeThrough(MultipartDecoder.create({ boundary }));

  for await (const part of decoded) {
    assert.ok(part instanceof Response);
    assert.equal(getMultipartPartName(part.headers), "alpha");
    assert.equal(decoder.decode(await readStream(part.body)), "streamed");
  }
});