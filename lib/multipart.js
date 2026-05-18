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

export class MultipartStreamReader {
  constructor(stream, boundary, { maxPartHeaderBytes = MAX_PART_HEADER_BYTES } = {}) {
    this.buffer = new Uint8Array(0);
    this.done = false;
    this.maxPartHeaderBytes = maxPartHeaderBytes;
    this.reader = stream.getReader();
    this.initialBoundary = encoder.encode(`--${boundary}`);
    this.partBoundary = encoder.encode(`\r\n--${boundary}`);
  }

  async cancel(reason = "multipart parsing stopped") {
    try {
      await this.reader.cancel(reason);
    } catch {
      // Ignore cancellation failures after stream errors; callers receive the original failure.
    }
  }

  release() {
    this.reader.releaseLock();
  }

  consume(byteLength) {
    const chunk = this.buffer.subarray(0, byteLength);
    this.buffer = this.buffer.subarray(byteLength);
    return chunk;
  }

  async ensureBytes(byteLength) {
    while (this.buffer.byteLength < byteLength && !this.done) {
      await this.readMore();
    }

    return this.buffer.byteLength >= byteLength;
  }

  async readHeaders() {
    while (true) {
      const delimiterIndex = indexOfBytes(this.buffer, DOUBLE_CRLF);
      if (delimiterIndex !== -1) {
        if (delimiterIndex > this.maxPartHeaderBytes) {
          throw invalidMultipart(`multipart headers exceed maximum size of ${this.maxPartHeaderBytes} bytes`);
        }

        const headerBytes = this.consume(delimiterIndex);
        this.consume(DOUBLE_CRLF.byteLength);
        return parsePartHeaders(decoder.decode(headerBytes));
      }

      if (this.buffer.byteLength > this.maxPartHeaderBytes) {
        throw invalidMultipart(`multipart headers exceed maximum size of ${this.maxPartHeaderBytes} bytes`);
      }

      if (!(await this.readMore())) {
        throw invalidMultipart("multipart body terminated in part headers");
      }
    }
  }

  async readMore() {
    let result;
    try {
      result = await this.reader.read();
    } catch (error) {
      throw invalidMultipart(error instanceof Error ? error.message : String(error));
    }

    if (result.done) {
      this.done = true;
      return false;
    }

    this.buffer = appendBytes(this.buffer, result.value);
    return true;
  }

  async readPartBody(onChunk) {
    while (true) {
      const boundaryIndex = indexOfBytes(this.buffer, this.partBoundary);
      if (boundaryIndex !== -1) {
        const requiredBytes = boundaryIndex + this.partBoundary.byteLength + 2;
        if (!(await this.ensureBytes(requiredBytes))) {
          throw invalidMultipart("multipart body terminated after a part boundary");
        }

        if (boundaryIndex > 0) {
          await onChunk(this.consume(boundaryIndex));
        }

        this.consume(this.partBoundary.byteLength);
        if (startsWithBytes(this.buffer, DASH_DASH)) {
          this.consume(DASH_DASH.byteLength);
          await this.ensureBytes(CRLF.byteLength);
          if (startsWithBytes(this.buffer, CRLF)) {
            this.consume(CRLF.byteLength);
          }
          return false;
        }

        if (!startsWithBytes(this.buffer, CRLF)) {
          throw invalidMultipart("multipart boundary must end with CRLF");
        }

        this.consume(CRLF.byteLength);
        return true;
      }

      const safeByteLength = this.buffer.byteLength - (this.partBoundary.byteLength - 1);
      if (safeByteLength > 0) {
        await onChunk(this.consume(safeByteLength));
        continue;
      }

      if (!(await this.readMore())) {
        throw invalidMultipart("multipart body terminated before the next boundary");
      }
    }
  }

  async start() {
    if (!(await this.ensureBytes(this.initialBoundary.byteLength + CRLF.byteLength))) {
      throw invalidMultipart("multipart body terminated before the first boundary");
    }

    if (!startsWithBytes(this.buffer, this.initialBoundary)) {
      throw invalidMultipart("multipart body does not start with the declared boundary");
    }

    this.consume(this.initialBoundary.byteLength);
    if (startsWithBytes(this.buffer, DASH_DASH)) {
      this.consume(DASH_DASH.byteLength);
      await this.ensureBytes(CRLF.byteLength);
      if (startsWithBytes(this.buffer, CRLF)) {
        this.consume(CRLF.byteLength);
      }
      return false;
    }

    if (!startsWithBytes(this.buffer, CRLF)) {
      throw invalidMultipart("multipart boundary must end with CRLF");
    }

    this.consume(CRLF.byteLength);
    return true;
  }
}