const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CRLF = encoder.encode("\r\n");
const DOUBLE_CRLF = encoder.encode("\r\n\r\n");
const DASH_DASH = encoder.encode("--");

export const MAX_PART_HEADER_BYTES = 8 * 1024;

export class MultipartParseError extends Error {
  constructor(message) {
    super(message);
    this.name = "MultipartParseError";
  }
}

function invalidMultipart(message) {
  return new MultipartParseError(message);
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

function appendBytes(left, right) {
  const normalizedRight = toUint8Array(right);
  if (left.byteLength === 0) {
    return normalizedRight;
  }

  const merged = new Uint8Array(left.byteLength + normalizedRight.byteLength);
  merged.set(left);
  merged.set(normalizedRight, left.byteLength);
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

export function parseHeaderParameters(value) {
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

export function getMultipartPartName(headers) {
  const normalizedHeaders = headers instanceof Headers ? headers : new Headers(headers);
  const contentDisposition = normalizedHeaders.get("content-disposition");
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

  return name;
}

export function parseMultipartPartHeaders(rawHeaders) {
  const headers = new Headers();

  for (const line of rawHeaders.split("\r\n")) {
    const separator = line.indexOf(":");
    if (separator <= 0) {
      throw invalidMultipart(`multipart header is malformed: ${line}`);
    }

    headers.append(line.slice(0, separator).trim().toLowerCase(), line.slice(separator + 1).trim());
  }

  getMultipartPartName(headers);
  return headers;
}

function encodeHeaderBlock(headers) {
  const normalizedHeaders = headers instanceof Headers
    ? Array.from(headers.entries())
    : Object.entries(headers ?? {});
  let block = "";

  for (const [name, value] of normalizedHeaders) {
    block += `${name}: ${value}\r\n`;
  }

  return encoder.encode(`${block}\r\n`);
}

/**
 * Encode an opening multipart boundary line.
 *
 * @param {string} boundary Multipart boundary token without the leading `--`.
 * @returns {Uint8Array} Boundary bytes terminated with CRLF.
 */
export function encodeBoundary(boundary) {
  return encoder.encode(`--${boundary}\r\n`);
}

export function encodeClosingBoundary(boundary, { trailingCrlf = true } = {}) {
  return encoder.encode(`--${boundary}--${trailingCrlf ? "\r\n" : ""}`);
}

/**
 * Encode a single multipart part after its enclosing boundary line has been written.
 *
 * @param {Headers | Record<string, string>} headers Part headers to serialize.
 * @param {ReadableStream<Uint8Array> | ArrayBuffer | ArrayBufferView | ArrayLike<number> | null | undefined} [body]
 * Optional part payload.
 * @returns {AsyncGenerator<Uint8Array, void, void>} Encoded header, body, and trailing CRLF bytes.
 */
export async function* encodePart(headers, body = undefined) {
  yield encodeHeaderBlock(headers);

  if (body instanceof ReadableStream) {
    for await (const chunk of body) {
      yield toUint8Array(chunk);
    }
  } else if (body !== undefined && body !== null) {
    yield toUint8Array(body);
  }

  yield CRLF;
}

function createMultipartDecoderTransformer({ boundary, maxPartHeaderBytes }) {
  let buffer = new Uint8Array(0);
  let currentPartWriter = null;
  let inputClosed = false;
  let phase = "start";

  const initialBoundary = encoder.encode(`--${boundary}`);
  const partBoundary = encoder.encode(`\r\n--${boundary}`);

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

  const openPart = (headers, controller) => {
    const channel = new TransformStream();
    currentPartWriter = channel.writable.getWriter();
    controller.enqueue(new Response(channel.readable, { headers }));
  };

  const closeCurrentPart = async () => {
    const writer = currentPartWriter;
    currentPartWriter = null;

    try {
      await writer.close();
    } finally {
      writer.releaseLock();
    }
  };

  const abortCurrentPart = async (error) => {
    if (currentPartWriter === null) {
      return;
    }

    const writer = currentPartWriter;
    currentPartWriter = null;

    try {
      await writer.abort(error);
    } catch {
      // Ignore abort races from already-cancelled part consumers.
    } finally {
      writer.releaseLock();
    }
  };

  const writeCurrentPartChunk = async (chunk) => {
    await currentPartWriter.write(chunk);
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

  const tryReadHeaders = (controller) => {
    const delimiterIndex = indexOfBytes(buffer, DOUBLE_CRLF);
    if (delimiterIndex !== -1) {
      if (delimiterIndex > maxPartHeaderBytes) {
        throw invalidMultipart(`multipart headers exceed maximum size of ${maxPartHeaderBytes} bytes`);
      }

      const headerBytes = consume(delimiterIndex);
      consume(DOUBLE_CRLF.byteLength);
      openPart(parseMultipartPartHeaders(decoder.decode(headerBytes)), controller);
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
        await closeCurrentPart();
        phase = "done";
        return true;
      }

      if (!startsWithBytes(trailing, CRLF)) {
        throw invalidMultipart("multipart boundary must end with CRLF");
      }

      consume(partBoundary.byteLength + CRLF.byteLength);
      await closeCurrentPart();
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

  const drain = async (controller) => {
    while (phase !== "done") {
      if (phase === "start") {
        if (!tryConsumeInitialBoundary()) {
          return;
        }
        continue;
      }

      if (phase === "headers") {
        if (!tryReadHeaders(controller)) {
          return;
        }
        continue;
      }

      if (phase === "body") {
        if (!(await tryReadPartBody())) {
          return;
        }
      }
    }
  };

  return {
    async transform(chunk, controller) {
      try {
        buffer = appendBytes(buffer, chunk);
        await drain(controller);
      } catch (error) {
        await abortCurrentPart(error);
        throw error;
      }
    },
    async flush(controller) {
      try {
        inputClosed = true;
        await drain(controller);
      } catch (error) {
        await abortCurrentPart(error);
        throw error;
      }
    },
  };
}

/**
 * Transform stream that decodes multipart/form-data into standard `Response` objects.
 *
 * Each chunk emitted by the readable side is a `Response` whose headers mirror the multipart part
 * headers and whose body stream yields that part's bytes.
 */
export class MultipartDecoder extends TransformStream {
  /**
   * Create a multipart decoder stream for a known boundary.
   *
   * The returned decoder emits standard `Response` objects representing multipart parts.
   *
   * @param {{ boundary: string, maxPartHeaderBytes?: number }} options Decoder configuration.
   * @returns {MultipartDecoder} Decoder whose readable side yields multipart parts as `Response`s.
   */
  static create(options) {
    return new MultipartDecoder(options);
  }

  /**
   * @param {{ boundary: string, maxPartHeaderBytes?: number }} options Decoder configuration.
   */
  constructor({ boundary, maxPartHeaderBytes = MAX_PART_HEADER_BYTES }) {
    super(
      createMultipartDecoderTransformer({ boundary, maxPartHeaderBytes }),
      undefined,
      { highWaterMark: 16 },
    );
  }
}
