import assert from "node:assert/strict";
import test from "node:test";

import {
  PACKET_DIALECTS,
  PACKET_MAGIC,
  PACKET_TYPES,
  computeFileDigests,
  computeFileId,
  createPacket,
  describePacketType,
  packEnvelopeSet,
  parseEnvelopeSet,
  parsePacketFile,
} from "./envelope.js";

const encoder = new TextEncoder();

function splitPacketBytes(bytes) {
  const packets = parsePacketFile(bytes);
  const normalized = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const slices = [];
  let offset = 0;

  for (const packet of packets) {
    slices.push(Uint8Array.from(normalized.subarray(offset, offset + packet.packetLength)));
    offset += packet.packetLength;
  }

  return slices;
}

function setPacketLength(packetBytes, packetLength) {
  const mutated = Uint8Array.from(packetBytes);
  new DataView(mutated.buffer).setBigUint64(8, BigInt(packetLength), true);
  return mutated;
}

function buildFileDescriptionBody(file, { fileId = file.fileId, fullMd5 = file.fullMd5, name = file.name } = {}) {
  const nameBytes = encoder.encode(name);
  const body = new Uint8Array(56 + nameBytes.byteLength);

  body.set(fileId, 0);
  body.set(fullMd5, 16);
  body.set(file.headMd5, 32);
  new DataView(body.buffer).setBigUint64(48, BigInt(file.size), true);
  body.set(nameBytes, 56);

  return body;
}

function buildMainBody({ sliceSize, fileCount, fileIds = [] }) {
  const body = new Uint8Array(12 + fileIds.length * 16);
  const view = new DataView(body.buffer);

  view.setBigUint64(0, BigInt(sliceSize), true);
  view.setUint32(8, fileCount, true);

  for (const [index, fileId] of fileIds.entries()) {
    body.set(fileId, 12 + index * 16);
  }

  return body;
}

function buildRecoveryBody(exponent, sliceBytes, extraPadding = []) {
  const body = new Uint8Array(4 + sliceBytes.byteLength + extraPadding.length);

  new DataView(body.buffer).setUint32(0, exponent, true);
  body.set(sliceBytes, 4);
  body.set(extraPadding, 4 + sliceBytes.byteLength);

  return body;
}

test("packEnvelopeSet round-trips strict PAR2 packet envelopes", () => {
  const alphaBytes = encoder.encode("alpha-body");
  const bravoBytes = encoder.encode("bravo-body");
  const alphaDigests = computeFileDigests(alphaBytes.buffer);
  const bravoDigests = computeFileDigests([98, 114, 97, 118, 111]);
  const bravoFileId = computeFileId({
    headMd5: bravoDigests.headMd5,
    name: "bravo.bin",
    size: 5,
  });
  const packed = packEnvelopeSet({
    creator: "par3",
    files: [
      {
        name: "alpha.bin",
        size: alphaBytes.byteLength,
        ...alphaDigests,
      },
      {
        fileId: bravoFileId,
        name: "bravo.bin",
        size: 5,
        ...bravoDigests,
      },
    ],
    recoverySlices: [
      { exponent: 1, bytes: Uint8Array.from([9, 8, 7, 6, 5, 4, 3, 2]).buffer },
      { exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) },
    ],
    sliceSize: 8,
  });

  assert.deepEqual(Array.from(packed.manifestBytes.subarray(0, 8)), Array.from(PACKET_MAGIC));

  const manifestPackets = parsePacketFile(new DataView(
    packed.manifestBytes.buffer,
    packed.manifestBytes.byteOffset,
    packed.manifestBytes.byteLength,
  ));

  assert.equal(manifestPackets.length, 4);
  assert.equal(describePacketType(manifestPackets[0].packetType), "PAR 2.0\\0Main\\0\\0\\0\\0");

  const parsed = parseEnvelopeSet([
    { name: "set.par2", bytes: packed.manifestBytes },
    { name: "set-copy.par2", bytes: packed.manifestBytes },
    { name: "set.vol.par2", bytes: packed.volumeBytes },
    { name: "set.vol.copy.par2", bytes: packed.volumeBytes },
  ]);

  assert.deepEqual(parsed.files.map((file) => file.name), packed.files.map((file) => file.name));
  assert.equal(parsed.format, "par2");
  assert.equal(parsed.sliceSize, 8);
  assert.deepEqual(parsed.recoverySlices.map((recoverySlice) => recoverySlice.exponent), [0, 1]);
  assert.deepEqual(parsed.recoverySlices[0].bytes, Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]));
  assert.deepEqual(parsed.recoverySetId, packed.recoverySetId);
});

test("packEnvelopeSet round-trips strict PAR3 packet envelopes", () => {
  const alphaBytes = encoder.encode("alpha-body");
  const alphaDigests = computeFileDigests(alphaBytes);
  const packed = packEnvelopeSet({
    creator: "par3",
    files: [{ name: "alpha.bin", size: alphaBytes.byteLength, ...alphaDigests }],
    format: "par3",
    recoverySlices: [{ exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) }],
    sliceSize: 8,
  });

  assert.deepEqual(Array.from(packed.manifestBytes.subarray(0, 8)), Array.from(PACKET_DIALECTS.par3.magic));

  const manifestPackets = parsePacketFile(packed.manifestBytes);
  assert.equal(manifestPackets.length, 3);
  assert.equal(describePacketType(manifestPackets[0].packetType), "PAR 3.0\\0Main\\0\\0\\0\\0");

  const parsed = parseEnvelopeSet([
    { name: "set.par3", bytes: packed.manifestBytes },
    { name: "set.vol.par3", bytes: packed.volumeBytes },
  ]);

  assert.equal(parsed.format, "par3");
  assert.equal(parsed.sliceSize, 8);
  assert.deepEqual(parsed.recoverySlices.map((recoverySlice) => recoverySlice.exponent), [0]);
});

test("helper APIs validate fixed-width fields and ASCII constraints", () => {
  assert.deepEqual(
    computeFileDigests(new Uint16Array([0x6162, 0x6364])).fullMd5,
    computeFileDigests(Uint8Array.from([0x62, 0x61, 0x64, 0x63])).fullMd5,
  );

  assert.throws(
    () => computeFileId({
      headMd5: Uint8Array.from([1, 2, 3]),
      name: "alpha.bin",
      size: 1,
    }),
    /file headMd5 must be exactly 16 bytes/,
  );
  assert.throws(
    () => computeFileId({
      headMd5: new Uint8Array(16),
      name: "naïve.bin",
      size: 1,
    }),
    /file name must be ASCII/,
  );
  assert.throws(
    () => createPacket({
      packetType: Uint8Array.from([1]),
      recoverySetId: new Uint8Array(16),
      body: [],
    }),
    /packet type must be exactly 16 bytes/,
  );
  assert.throws(
    () => createPacket({
      packetType: PACKET_TYPES.creator,
      recoverySetId: Uint8Array.from([1]),
      body: [],
    }),
    /recovery set id must be exactly 16 bytes/,
  );
  assert.throws(
    () => createPacket({
      format: "par3",
      packetType: PACKET_TYPES.creator,
      recoverySetId: new Uint8Array(16),
      body: [],
    }),
    /packet type PAR 2.0\\0Creator\\0 does not match par3 packet prefix/,
  );
  assert.throws(
    () => createPacket({
      packetType: new Uint8Array(16).fill(1),
      recoverySetId: new Uint8Array(16),
      body: [],
    }),
    /does not match par2 packet prefix/,
  );
  assert.throws(
    () => packEnvelopeSet({
      files: [{ name: "alpha.bin", size: 1, ...computeFileDigests(Uint8Array.from([1])) }],
      format: "zip",
      sliceSize: 8,
    }),
    /format must be either par2 or par3/,
  );
});

test("packEnvelopeSet validates slice size and recovery slice layout", () => {
  const fileBytes = encoder.encode("payload");
  const digests = computeFileDigests(fileBytes);

  assert.throws(
    () => packEnvelopeSet({
      creator: "",
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      sliceSize: 8,
    }),
    /creator must be a non-empty ASCII string/,
  );
  assert.throws(
    () => packEnvelopeSet({
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      sliceSize: 4.5,
    }),
    /slice size must be an integer greater than or equal to 4/,
  );
  assert.throws(
    () => packEnvelopeSet({
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      sliceSize: 6,
    }),
    /slice size must be a multiple of 4/,
  );
  assert.throws(
    () => packEnvelopeSet({
      creator: "créateur",
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      sliceSize: 8,
    }),
    /creator must be ASCII/,
  );
  assert.throws(
    () => packEnvelopeSet({
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      recoverySlices: [
        { exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) },
        { exponent: 0, bytes: Uint8Array.from([8, 7, 6, 5, 4, 3, 2, 1]) },
      ],
      sliceSize: 8,
    }),
    /duplicate recovery slice exponent 0/,
  );
  assert.throws(
    () => packEnvelopeSet({
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      recoverySlices: [{ exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4]) }],
      sliceSize: 8,
    }),
    /recovery slice 0 is 4 bytes but slice size is 8/,
  );
});

test("parsePacketFile rejects framing and checksum issues", async (t) => {
  const packet = createPacket({
    packetType: PACKET_TYPES.creator,
    recoverySetId: new Uint8Array(16),
    body: encoder.encode("par3"),
  });

  await t.test("truncated header", () => {
    assert.throws(() => parsePacketFile(packet.subarray(0, 63)), /packet header is truncated/);
  });

  await t.test("magic mismatch", () => {
    const mutated = Uint8Array.from(packet);
    mutated[0] = 0;
    assert.throws(() => parsePacketFile(mutated), /packet magic mismatch/);
  });

  await t.test("invalid short packet length", () => {
    assert.throws(() => parsePacketFile(setPacketLength(packet, 60)), /packet length 60 .* invalid/);
  });

  await t.test("invalid unaligned packet length", () => {
    assert.throws(() => parsePacketFile(setPacketLength(packet, 66)), /not 4-byte aligned/);
  });

  await t.test("overrunning packet length", () => {
    assert.throws(() => parsePacketFile(setPacketLength(packet, 128)), /overruns the envelope file/);
  });

  await t.test("checksum mismatch", () => {
    const mutated = Uint8Array.from(packet);
    mutated[64] ^= 0xff;
    assert.throws(() => parsePacketFile(mutated), /packet checksum mismatch/);
  });

  await t.test("packet type prefix must match the magic dialect", () => {
    const mutated = Uint8Array.from(packet);
    mutated[3] = 0x33;
    assert.throws(() => parsePacketFile(mutated), /does not match par3 packet prefix/);
  });

  await t.test("unsupported packet version digits are rejected", () => {
    const mutated = Uint8Array.from(packet);
    mutated[3] = 0x58;
    assert.throws(() => parsePacketFile(mutated), /packet magic mismatch/);
  });
});

test("parseEnvelopeSet rejects inconsistent manifests and packets", async (t) => {
  const fileBytes = encoder.encode("alpha-body");
  const digests = computeFileDigests(fileBytes);
  const packed = packEnvelopeSet({
    files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
    recoverySlices: [{ exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) }],
    sliceSize: 8,
  });
  const [mainPacket, creatorPacket, fileDescPacket] = splitPacketBytes(packed.manifestBytes);
  const [recoveryPacket] = splitPacketBytes(packed.volumeBytes);
  const conflictingSet = packEnvelopeSet({
    files: [{ name: "bravo.bin", size: fileBytes.byteLength, ...digests }],
    recoverySlices: [{ exponent: 0, bytes: Uint8Array.from([8, 7, 6, 5, 4, 3, 2, 1]) }],
    sliceSize: 8,
  });

  await t.test("missing main packet", () => {
    assert.throws(
      () => parseEnvelopeSet([{ name: "set.vol.par2", bytes: packed.volumeBytes }]),
      /does not contain a main packet/,
    );
  });

  await t.test("conflicting main packets", () => {
    assert.throws(
      () => parseEnvelopeSet([
        { name: "a.par2", bytes: packed.manifestBytes },
        { name: "b.par2", bytes: conflictingSet.manifestBytes },
      ]),
      /conflicting main packets/,
    );
  });

  await t.test("mixed recovery set ids", () => {
    assert.throws(
      () => parseEnvelopeSet([
        { name: "a.par2", bytes: packed.manifestBytes },
        { name: "b.vol.par2", bytes: conflictingSet.volumeBytes },
      ]),
      /mixed recovery set ids/,
    );
  });

  await t.test("mixed packet formats", () => {
    const par3Set = packEnvelopeSet({
      files: [{ name: "alpha.bin", size: fileBytes.byteLength, ...digests }],
      format: "par3",
      recoverySlices: [{ exponent: 0, bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) }],
      sliceSize: 8,
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "a.par2", bytes: packed.manifestBytes },
        { name: "b.vol.par3", bytes: par3Set.volumeBytes },
      ]),
      /mixed packet formats/,
    );
  });

  await t.test("missing file descriptions", () => {
    assert.throws(
      () => parseEnvelopeSet([{ name: "broken.par2", bytes: Buffer.concat([mainPacket, creatorPacket]) }]),
      /missing file description/,
    );
  });

  await t.test("conflicting file descriptions", () => {
    const conflictingFileDesc = createPacket({
      packetType: PACKET_TYPES.fileDescription,
      recoverySetId: packed.recoverySetId,
      body: buildFileDescriptionBody(packed.files[0], {
        fullMd5: Uint8Array.from(new Uint8Array(16).fill(9)),
      }),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "set.par2", bytes: packed.manifestBytes },
        { name: "conflict.par2", bytes: conflictingFileDesc },
      ]),
      /conflicting file descriptions/,
    );
  });

  await t.test("conflicting recovery slices", () => {
    const conflictingRecovery = createPacket({
      packetType: PACKET_TYPES.recoverySlice,
      recoverySetId: packed.recoverySetId,
      body: buildRecoveryBody(0, Uint8Array.from([8, 7, 6, 5, 4, 3, 2, 1])),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "set.par2", bytes: packed.manifestBytes },
        { name: "set.vol.par2", bytes: packed.volumeBytes },
        { name: "conflict.vol.par2", bytes: conflictingRecovery },
      ]),
      /conflicting recovery slice 0/,
    );
  });

  await t.test("recovery slice padding must stay zero", () => {
    const malformedRecovery = createPacket({
      packetType: PACKET_TYPES.recoverySlice,
      recoverySetId: packed.recoverySetId,
      body: buildRecoveryBody(1, Uint8Array.from([1, 1, 1, 1, 1, 1, 1, 1]), [9]),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "set.par2", bytes: packed.manifestBytes },
        { name: "malformed.vol.par2", bytes: malformedRecovery },
      ]),
      /padding must be zero-filled/,
    );
  });

  await t.test("file description ids are recomputed", () => {
    const malformedFileDesc = createPacket({
      packetType: PACKET_TYPES.fileDescription,
      recoverySetId: packed.recoverySetId,
      body: buildFileDescriptionBody(packed.files[0], {
        fileId: Uint8Array.from(new Uint8Array(16).fill(1)),
      }),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "main.par2", bytes: Buffer.concat([mainPacket, creatorPacket]) },
        { name: "desc.par2", bytes: malformedFileDesc },
      ]),
      /file description packet id mismatch/,
    );
  });

  await t.test("file description names must be ASCII", () => {
    const malformedFileDesc = createPacket({
      packetType: PACKET_TYPES.fileDescription,
      recoverySetId: packed.recoverySetId,
      body: buildFileDescriptionBody(packed.files[0], {
        name: "alpha.binÿ",
      }),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "main.par2", bytes: Buffer.concat([mainPacket, creatorPacket]) },
        { name: "desc.par2", bytes: malformedFileDesc },
      ]),
      /file description name must contain only ASCII bytes/,
    );
  });

  await t.test("main packet slice size must be a multiple of 4", () => {
    const malformedMain = createPacket({
      packetType: PACKET_TYPES.main,
      recoverySetId: packed.recoverySetId,
      body: buildMainBody({ sliceSize: 6, fileCount: 1, fileIds: [packed.files[0].fileId] }),
    });

    assert.throws(
      () => parseEnvelopeSet([{ name: "main.par2", bytes: malformedMain }]),
      /slice size must be a safe integer multiple of 4/,
    );
  });

  await t.test("main packet body length must match the declared file count", () => {
    const malformedMain = createPacket({
      packetType: PACKET_TYPES.main,
      recoverySetId: packed.recoverySetId,
      body: buildMainBody({ sliceSize: 8, fileCount: 1 }),
    });

    assert.throws(
      () => parseEnvelopeSet([{ name: "main.par2", bytes: malformedMain }]),
      /body length does not match the declared file count/,
    );
  });

  await t.test("main packet bodies must include the fixed header payload", () => {
    const malformedMain = createPacket({
      packetType: PACKET_TYPES.main,
      recoverySetId: packed.recoverySetId,
      body: Uint8Array.from([1, 2, 3, 4]),
    });

    assert.throws(
      () => parseEnvelopeSet([{ name: "main.par2", bytes: malformedMain }]),
      /main packet body is truncated/,
    );
  });

  await t.test("file description packets must include the fixed metadata header", () => {
    const malformedFileDesc = createPacket({
      packetType: PACKET_TYPES.fileDescription,
      recoverySetId: packed.recoverySetId,
      body: new Uint8Array(52),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "main.par2", bytes: Buffer.concat([mainPacket, creatorPacket]) },
        { name: "desc.par2", bytes: malformedFileDesc },
      ]),
      /file description packet body is truncated/,
    );
  });

  await t.test("file description packet sizes must stay in the safe integer range", () => {
    const malformedBody = buildFileDescriptionBody(packed.files[0]);
    new DataView(malformedBody.buffer).setBigUint64(48, 2n ** 60n, true);
    const malformedFileDesc = createPacket({
      packetType: PACKET_TYPES.fileDescription,
      recoverySetId: packed.recoverySetId,
      body: malformedBody,
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "main.par2", bytes: Buffer.concat([mainPacket, creatorPacket]) },
        { name: "desc.par2", bytes: malformedFileDesc },
      ]),
      /file description packet size must be a safe integer/,
    );
  });

  await t.test("recovery slices must contain the full shard payload", () => {
    const malformedRecovery = createPacket({
      packetType: PACKET_TYPES.recoverySlice,
      recoverySetId: packed.recoverySetId,
      body: buildRecoveryBody(0, Uint8Array.from([1, 2, 3, 4])),
    });

    assert.throws(
      () => parseEnvelopeSet([
        { name: "set.par2", bytes: packed.manifestBytes },
        { name: "short.vol.par2", bytes: malformedRecovery },
      ]),
      /recovery slice packet body is truncated/,
    );
  });

  assert.ok(fileDescPacket.byteLength > 0);
  assert.ok(recoveryPacket.byteLength > 0);
});