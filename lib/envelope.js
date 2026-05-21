import { createHash } from "node:crypto";

const encoder = new TextEncoder();
const PACKET_MAGIC_BYTES = 8;
const PACKET_TYPE_BYTES = 16;

export const PACKET_ALIGNMENT_BYTES = 4;
export const PACKET_HEADER_BYTES = 64;
export const PACKET_DIALECTS = Object.freeze({
  par2: createPacketDialect("par2", "2"),
  par3: createPacketDialect("par3", "3"),
});
export const PACKET_MAGIC = PACKET_DIALECTS.par2.magic;
export const PACKET_TYPES = PACKET_DIALECTS.par2.types;

function createPacketDialect(format, versionDigit) {
  return Object.freeze({
    format,
    magic: encoder.encode(`PAR${versionDigit}\0PKT`),
    packetTypePrefix: encoder.encode(`PAR ${versionDigit}.0\0`),
    types: Object.freeze({
      creator: encoder.encode(`PAR ${versionDigit}.0\0Creator\0`),
      fileDescription: encoder.encode(`PAR ${versionDigit}.0\0FileDesc`),
      main: encoder.encode(`PAR ${versionDigit}.0\0Main\0\0\0\0`),
      recoverySlice: encoder.encode(`PAR ${versionDigit}.0\0RecvSlic`),
    }),
  });
}

function resolvePacketDialect(format, field = "format") {
  const dialect = PACKET_DIALECTS[format];

  if (!dialect) {
    throw new Error(`${field} must be either par2 or par3`);
  }

  return dialect;
}

function resolvePacketDialectFromPacketType(packetType) {
  return Object.values(PACKET_DIALECTS).find((dialect) =>
    bytesEqual(packetType.subarray(0, dialect.packetTypePrefix.byteLength), dialect.packetTypePrefix),
  ) ?? null;
}

function resolvePacketDialectFromMagic(magic) {
  const normalized = normalizeFixedBytes(magic, PACKET_MAGIC_BYTES, "packet magic");

  if (
    normalized[0] !== 0x50
    || normalized[1] !== 0x41
    || normalized[2] !== 0x52
    || normalized[4] !== 0x00
    || normalized[5] !== 0x50
    || normalized[6] !== 0x4b
    || normalized[7] !== 0x54
  ) {
    return null;
  }

  if (normalized[3] === 0x32) {
    return PACKET_DIALECTS.par2;
  }

  if (normalized[3] === 0x33) {
    return PACKET_DIALECTS.par3;
  }

  return null;
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

function bytesEqual(left, right) {
  return left.byteLength === right.byteLength && Buffer.from(left).equals(Buffer.from(right));
}

function compareBytes(left, right) {
  return Buffer.from(left).compare(Buffer.from(right));
}

function assertSafeInteger(value, field, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${field} must be an integer greater than or equal to ${minimum}`);
  }

  return value;
}

function encodeAscii(value, field) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty ASCII string`);
  }

  const bytes = new Uint8Array(value.length);

  for (let index = 0; index < value.length; index += 1) {
    const codePoint = value.charCodeAt(index);

    if (codePoint === 0 || codePoint > 0x7f) {
      throw new Error(`${field} must be ASCII without embedded NUL bytes`);
    }

    bytes[index] = codePoint;
  }

  return bytes;
}

function decodeAscii(bytes, field) {
  let value = "";

  for (const byte of bytes) {
    if (byte > 0x7f) {
      throw new Error(`${field} must contain only ASCII bytes`);
    }

    value += String.fromCharCode(byte);
  }

  return value;
}

function alignByteLength(byteLength) {
  return (byteLength + (PACKET_ALIGNMENT_BYTES - 1)) & ~(PACKET_ALIGNMENT_BYTES - 1);
}

function alignBody(bodyBytes) {
  const alignedLength = alignByteLength(bodyBytes.byteLength);

  if (alignedLength === bodyBytes.byteLength) {
    return bodyBytes;
  }

  const aligned = new Uint8Array(alignedLength);
  aligned.set(bodyBytes);
  return aligned;
}

function concatBytes(chunks) {
  const normalizedChunks = chunks.map((chunk) => toUint8Array(chunk));
  const totalLength = normalizedChunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const merged = new Uint8Array(totalLength);
  let offset = 0;

  for (const chunk of normalizedChunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return merged;
}

function md5Bytes(chunks) {
  const hash = createHash("md5");

  for (const chunk of chunks) {
    hash.update(toUint8Array(chunk));
  }

  return new Uint8Array(hash.digest());
}

function uint64Bytes(value) {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return bytes;
}

function trimTrailingZeros(bytes) {
  let end = bytes.byteLength;

  while (end > 0 && bytes[end - 1] === 0) {
    end -= 1;
  }

  return bytes.subarray(0, end);
}

function bytesKey(bytes) {
  return Buffer.from(bytes).toString("hex");
}

function packetTypeLabel(packetType) {
  return Array.from(packetType, (byte) => (byte === 0 ? "\\0" : String.fromCharCode(byte))).join("");
}

function normalizeFixedBytes(bytes, expectedLength, field) {
  const normalized = toUint8Array(bytes);

  if (normalized.byteLength !== expectedLength) {
    throw new Error(`${field} must be exactly ${expectedLength} bytes`);
  }

  return normalized;
}

function normalizeFileRecord(file) {
  const nameBytes = encodeAscii(file.name, "file name");
  const size = assertSafeInteger(file.size, "file size", 0);
  const fullMd5 = normalizeFixedBytes(file.fullMd5, 16, "file fullMd5");
  const headMd5 = normalizeFixedBytes(file.headMd5, 16, "file headMd5");
  const fileId = file.fileId
    ? normalizeFixedBytes(file.fileId, 16, "file fileId")
    : computeFileId({
      headMd5,
      name: file.name,
      size,
    });

  return {
    fileId,
    fullMd5,
    headMd5,
    name: file.name,
    nameBytes,
    size,
  };
}

function buildMainBody({ sliceSize, files }) {
  const body = new Uint8Array(12 + files.length * 16);
  const view = new DataView(body.buffer);

  view.setBigUint64(0, BigInt(assertSafeInteger(sliceSize, "slice size", 4)), true);
  view.setUint32(8, files.length, true);

  for (const [index, file] of files.entries()) {
    body.set(file.fileId, 12 + index * 16);
  }

  return body;
}

function buildFileDescriptionBody(file) {
  const body = new Uint8Array(56 + file.nameBytes.byteLength);
  const view = new DataView(body.buffer);

  body.set(file.fileId, 0);
  body.set(file.fullMd5, 16);
  body.set(file.headMd5, 32);
  view.setBigUint64(48, BigInt(file.size), true);
  body.set(file.nameBytes, 56);

  return body;
}

function parseMainBody(body) {
  if (body.byteLength < 12) {
    throw new Error("main packet body is truncated");
  }

  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const sliceSize = Number(view.getBigUint64(0, true));
  const fileCount = view.getUint32(8, true);
  const expectedLength = 12 + fileCount * 16;

  if (!Number.isSafeInteger(sliceSize) || sliceSize < 4 || sliceSize % 4 !== 0) {
    throw new Error("main packet slice size must be a safe integer multiple of 4");
  }

  if (body.byteLength !== expectedLength) {
    throw new Error("main packet body length does not match the declared file count");
  }

  const fileIds = Array.from({ length: fileCount }, (_, index) =>
    Uint8Array.from(body.subarray(12 + index * 16, 12 + (index + 1) * 16)),
  );

  return {
    fileIds,
    sliceSize,
  };
}

function parseFileDescriptionBody(body) {
  if (body.byteLength < 56) {
    throw new Error("file description packet body is truncated");
  }

  const fileId = Uint8Array.from(body.subarray(0, 16));
  const fullMd5 = Uint8Array.from(body.subarray(16, 32));
  const headMd5 = Uint8Array.from(body.subarray(32, 48));
  const size = Number(new DataView(body.buffer, body.byteOffset, body.byteLength).getBigUint64(48, true));

  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error("file description packet size must be a safe integer");
  }

  const nameBytes = trimTrailingZeros(body.subarray(56));
  const name = decodeAscii(nameBytes, "file description name");
  const computedFileId = computeFileId({ headMd5, name, size });

  if (!bytesEqual(fileId, computedFileId)) {
    throw new Error(`file description packet id mismatch for ${name}`);
  }

  return {
    fileId,
    fullMd5,
    headMd5,
    name,
    size,
  };
}

function parseRecoverySliceBody(body, sliceSize) {
  if (body.byteLength < 4 + sliceSize) {
    throw new Error("recovery slice packet body is truncated");
  }

  const exponent = new DataView(body.buffer, body.byteOffset, body.byteLength).getUint32(0, true);
  const shardBytes = Uint8Array.from(body.subarray(4, 4 + sliceSize));
  const padding = body.subarray(4 + sliceSize);

  for (const byte of padding) {
    if (byte !== 0) {
      throw new Error("recovery slice packet padding must be zero-filled");
    }
  }

  return {
    exponent,
    bytes: shardBytes,
  };
}

export function computeFileDigests(bytes) {
  const normalized = toUint8Array(bytes);

  return {
    fullMd5: md5Bytes([normalized]),
    headMd5: md5Bytes([normalized.subarray(0, Math.min(normalized.byteLength, 16 * 1024))]),
  };
}

export function computeFileId({ headMd5, name, size }) {
  const normalizedHeadMd5 = normalizeFixedBytes(headMd5, 16, "file headMd5");
  const nameBytes = encodeAscii(name, "file name");

  return md5Bytes([
    normalizedHeadMd5,
    uint64Bytes(assertSafeInteger(size, "file size", 0)),
    nameBytes,
  ]);
}

export function createPacket({ format, packetType, recoverySetId, body }) {
  const normalizedPacketType = normalizeFixedBytes(packetType, PACKET_TYPE_BYTES, "packet type");
  const packetTypeDialect = resolvePacketDialectFromPacketType(normalizedPacketType);
  const dialect = format === undefined
    ? packetTypeDialect ?? PACKET_DIALECTS.par2
    : resolvePacketDialect(format, "packet format");

  if (!packetTypeDialect || packetTypeDialect.format !== dialect.format) {
    throw new Error(`packet type ${packetTypeLabel(normalizedPacketType)} does not match ${dialect.format} packet prefix`);
  }

  const normalizedRecoverySetId = normalizeFixedBytes(recoverySetId, 16, "recovery set id");
  const alignedBody = alignBody(toUint8Array(body));
  const packet = new Uint8Array(PACKET_HEADER_BYTES + alignedBody.byteLength);
  const view = new DataView(packet.buffer);

  packet.set(dialect.magic, 0);
  view.setBigUint64(8, BigInt(packet.byteLength), true);
  packet.set(normalizedRecoverySetId, 32);
  packet.set(normalizedPacketType, 48);
  packet.set(alignedBody, PACKET_HEADER_BYTES);
  packet.set(md5Bytes([packet.subarray(32)]), 16);

  return packet;
}

export function packEnvelopeSet({ creator = "par3", files, format = "par2", recoverySlices = [], sliceSize }) {
  const dialect = resolvePacketDialect(format);
  const normalizedFiles = files.map((file) => normalizeFileRecord(file)).toSorted((left, right) =>
    compareBytes(left.fileId, right.fileId),
  );
  const normalizedSliceSize = assertSafeInteger(sliceSize, "slice size", 4);

  if (normalizedSliceSize % 4 !== 0) {
    throw new Error("slice size must be a multiple of 4");
  }

  const mainBody = buildMainBody({ files: normalizedFiles, sliceSize: normalizedSliceSize });
  const recoverySetId = md5Bytes([alignBody(mainBody)]);
  const manifestPackets = [
    createPacket({
      body: mainBody,
      format: dialect.format,
      packetType: dialect.types.main,
      recoverySetId,
    }),
    createPacket({
      body: encodeAscii(creator, "creator"),
      format: dialect.format,
      packetType: dialect.types.creator,
      recoverySetId,
    }),
    ...normalizedFiles.map((file) =>
      createPacket({
        body: buildFileDescriptionBody(file),
        format: dialect.format,
        packetType: dialect.types.fileDescription,
        recoverySetId,
      })
    ),
  ];
  const normalizedRecoverySlices = recoverySlices
    .map((recoverySlice) => {
      const exponent = assertSafeInteger(recoverySlice.exponent, "recovery slice exponent", 0);
      const recoveryBytes = toUint8Array(recoverySlice.bytes);

      if (recoveryBytes.byteLength !== normalizedSliceSize) {
        throw new Error(
          `recovery slice ${exponent} is ${recoveryBytes.byteLength} bytes but slice size is ${normalizedSliceSize}`,
        );
      }

      return {
        bytes: recoveryBytes,
        exponent,
      };
    })
    .toSorted((left, right) => left.exponent - right.exponent);
  const seenExponents = new Set();

  for (const recoverySlice of normalizedRecoverySlices) {
    if (seenExponents.has(recoverySlice.exponent)) {
      throw new Error(`duplicate recovery slice exponent ${recoverySlice.exponent}`);
    }

    seenExponents.add(recoverySlice.exponent);
  }

  const recoveryPackets = normalizedRecoverySlices.map((recoverySlice) => {
    const body = new Uint8Array(4 + recoverySlice.bytes.byteLength);
    new DataView(body.buffer).setUint32(0, recoverySlice.exponent, true);
    body.set(recoverySlice.bytes, 4);

    return createPacket({
      body,
      format: dialect.format,
      packetType: dialect.types.recoverySlice,
      recoverySetId,
    });
  });

  return {
    files: normalizedFiles,
    format: dialect.format,
    manifestBytes: concatBytes(manifestPackets),
    recoverySetId,
    volumeBytes: concatBytes(recoveryPackets),
  };
}

export function parsePacketFile(bytes) {
  const normalized = toUint8Array(bytes);
  const packets = [];

  for (let offset = 0; offset < normalized.byteLength; ) {
    if (offset + PACKET_HEADER_BYTES > normalized.byteLength) {
      throw new Error("packet header is truncated");
    }

    const magic = normalized.subarray(offset, offset + PACKET_MAGIC_BYTES);
    const dialect = resolvePacketDialectFromMagic(magic);

    if (!dialect) {
      throw new Error(`packet magic mismatch at byte offset ${offset}`);
    }

    const headerView = new DataView(normalized.buffer, normalized.byteOffset + offset, PACKET_HEADER_BYTES);
    const packetLength = Number(headerView.getBigUint64(8, true));

    if (!Number.isSafeInteger(packetLength) || packetLength < PACKET_HEADER_BYTES) {
      throw new Error(`packet length ${packetLength} at byte offset ${offset} is invalid`);
    }

    if (packetLength % PACKET_ALIGNMENT_BYTES !== 0) {
      throw new Error(`packet length ${packetLength} at byte offset ${offset} is not 4-byte aligned`);
    }

    if (offset + packetLength > normalized.byteLength) {
      throw new Error(`packet at byte offset ${offset} overruns the envelope file`);
    }

    const packetBytes = normalized.subarray(offset, offset + packetLength);
    const packetHash = packetBytes.subarray(16, 32);
    const expectedHash = md5Bytes([packetBytes.subarray(32)]);

    if (!bytesEqual(packetHash, expectedHash)) {
      throw new Error(`packet checksum mismatch at byte offset ${offset}`);
    }

    const packetType = Uint8Array.from(packetBytes.subarray(48, 64));
    if (!bytesEqual(packetType.subarray(0, dialect.packetTypePrefix.byteLength), dialect.packetTypePrefix)) {
      throw new Error(`packet type ${packetTypeLabel(packetType)} does not match ${dialect.format} packet prefix at byte offset ${offset}`);
    }

    packets.push({
      body: Uint8Array.from(packetBytes.subarray(PACKET_HEADER_BYTES)),
      format: dialect.format,
      packetLength,
      packetType,
      recoverySetId: Uint8Array.from(packetBytes.subarray(32, 48)),
    });
    offset += packetLength;
  }

  return packets;
}

export function parseEnvelopeSet(packetFiles) {
  const packets = packetFiles.flatMap((packetFile) => parsePacketFile(packetFile.bytes));
  let mainPacket = null;

  for (const packet of packets) {
    if (bytesEqual(packet.packetType, PACKET_DIALECTS[packet.format].types.main)) {
      if (mainPacket === null) {
        mainPacket = packet;
        continue;
      }

      if (!bytesEqual(packet.recoverySetId, mainPacket.recoverySetId) || !bytesEqual(packet.body, mainPacket.body)) {
        throw new Error("envelope set contains conflicting main packets");
      }
    }
  }

  if (mainPacket === null) {
    throw new Error("envelope set does not contain a main packet");
  }

  const recoverySetKey = bytesKey(mainPacket.recoverySetId);
  const dialect = PACKET_DIALECTS[mainPacket.format];
  const { fileIds, sliceSize } = parseMainBody(mainPacket.body);
  const filesById = new Map();
  const recoverySlices = new Map();

  for (const packet of packets) {
    if (packet.format !== mainPacket.format) {
      throw new Error("envelope set contains mixed packet formats");
    }

    if (bytesKey(packet.recoverySetId) !== recoverySetKey) {
      throw new Error("envelope set contains mixed recovery set ids");
    }

    if (bytesEqual(packet.packetType, dialect.types.fileDescription)) {
      const file = parseFileDescriptionBody(packet.body);
      const key = bytesKey(file.fileId);
      const previous = filesById.get(key);

      if (previous && (
        previous.name !== file.name
        || previous.size !== file.size
        || !bytesEqual(previous.fullMd5, file.fullMd5)
        || !bytesEqual(previous.headMd5, file.headMd5)
      )) {
        throw new Error(`envelope set contains conflicting file descriptions for ${file.name}`);
      }

      filesById.set(key, file);
      continue;
    }

    if (bytesEqual(packet.packetType, dialect.types.recoverySlice)) {
      const recoverySlice = parseRecoverySliceBody(packet.body, sliceSize);
      const previous = recoverySlices.get(recoverySlice.exponent);

      if (previous && !bytesEqual(previous.bytes, recoverySlice.bytes)) {
        throw new Error(`envelope set contains conflicting recovery slice ${recoverySlice.exponent}`);
      }

      recoverySlices.set(recoverySlice.exponent, recoverySlice);
    }
  }

  const orderedFiles = fileIds.map((fileId) => {
    const file = filesById.get(bytesKey(fileId));

    if (!file) {
      throw new Error(`missing file description for file id ${bytesKey(fileId)}`);
    }

    return file;
  });

  return {
    files: orderedFiles,
    format: mainPacket.format,
    recoverySetId: Uint8Array.from(mainPacket.recoverySetId),
    recoverySlices: Array.from(recoverySlices.values()).toSorted((left, right) => left.exponent - right.exponent),
    sliceSize,
  };
}

export function describePacketType(packetType) {
  return packetTypeLabel(normalizeFixedBytes(packetType, 16, "packet type"));
}