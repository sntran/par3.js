import * as wasmModule from "../pkg/par3_bg.wasm";
import {
  alloc_shard_arena,
  alloc_u32_buffer,
  free_shard_arena,
  free_u32_buffer,
  leopard_repair,
} from "../pkg/par3.js";

const { memory } = wasmModule;

function defaultCreateError(message) {
  return new Error(message);
}

function sortNumeric(values) {
  return values.toSorted((left, right) => left - right);
}

export class Par3 {
  static assertInteger(value, field, minimum = 0, createError = defaultCreateError) {
    if (!Number.isInteger(value) || value < minimum) {
      throw createError(`${field} must be an integer greater than or equal to ${minimum}`);
    }

    return value;
  }

  static normalizeIndices(
    input,
    { field = "indices", minimum = 0, createError = defaultCreateError } = {},
  ) {
    if (!Array.isArray(input)) {
      throw createError(`${field} must be an array of integer slot indexes`);
    }

    const unique = new Set();
    for (const rawIndex of input) {
      unique.add(Par3.assertInteger(rawIndex, `${field}[]`, minimum, createError));
    }

    return sortNumeric(Array.from(unique));
  }

  static resolveLayout({
    originalCount,
    shardSize,
    recoveryCount = null,
    totalShardCount,
    requestedMissingIndices = [],
    createError = defaultCreateError,
    fieldNames = {},
  }) {
    const names = {
      missingIndices: fieldNames.missingIndices ?? "missing_indices",
      originalCount: fieldNames.originalCount ?? "original_count",
      recoveryCount: fieldNames.recoveryCount ?? "recovery_count",
      shardSize: fieldNames.shardSize ?? "shard_size",
      totalShardCount: fieldNames.totalShardCount ?? "total_shard_count",
    };

    const normalizedRequestedMissingIndices = Par3.normalizeIndices(requestedMissingIndices, {
      field: names.missingIndices,
      createError,
    });
    const normalizedOriginalCount = Par3.assertInteger(
      originalCount,
      names.originalCount,
      1,
      createError,
    );
    const normalizedShardSize = Par3.assertInteger(
      shardSize,
      names.shardSize,
      2,
      createError,
    );

    if (normalizedShardSize % 2 !== 0) {
      throw createError(`${names.shardSize} must be even`);
    }

    const normalizedRecoveryCount = recoveryCount === null || recoveryCount === undefined
      ? null
      : Par3.assertInteger(recoveryCount, names.recoveryCount, 0, createError);
    const hasExplicitSlotCount = totalShardCount !== null && totalShardCount !== undefined;
    const hasLockedSlotCount = hasExplicitSlotCount || normalizedRecoveryCount !== null;
    const normalizedSlotCount = !hasExplicitSlotCount
      ? (normalizedRecoveryCount === null
        ? Math.max(normalizedOriginalCount, (normalizedRequestedMissingIndices.at(-1) ?? 0) + 1)
        : normalizedOriginalCount + normalizedRecoveryCount)
      : Par3.assertInteger(totalShardCount, names.totalShardCount, normalizedOriginalCount, createError);

    return {
      originalCount: normalizedOriginalCount,
      requestedMissingIndices: normalizedRequestedMissingIndices,
      shardSize: normalizedShardSize,
      slotCount: normalizedSlotCount,
      slotCountLocked: hasLockedSlotCount,
    };
  }

  constructor(
    { originalCount, requestedMissingIndices = [], shardSize, slotCount, slotCountLocked = true },
    { createError = defaultCreateError } = {},
  ) {
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
    this.createError = createError;
    this.originalCount = originalCount;
    this.receivedIndices = new Set();
    this.requestedMissingSet = new Set(requestedMissingIndices);
    this.shardSize = shardSize;
    this.slotCount = slotCount;
    this.slotCountLocked = slotCountLocked;
  }

  memoryView(pointer, byteLength) {
    return new Uint8Array(memory.buffer, pointer, byteLength);
  }

  ensureCapacity(requiredSlotCount, { overflowMessage } = {}) {
    if (requiredSlotCount > this.slotCount) {
      if (this.slotCountLocked) {
        throw this.createError(
          overflowMessage ?? `encountered slot index outside declared total_shard_count (${this.slotCount})`,
        );
      }

      this.slotCount = requiredSlotCount;
    }

    if (!this.arenaPtr) {
      this.arenaPtr = alloc_shard_arena(this.slotCount, this.shardSize);
      this.arenaSlotCount = this.slotCount;
      return;
    }

    if (this.slotCount <= this.arenaSlotCount) {
      return;
    }

    // Wasm arenas cannot grow in place, so expansion takes a snapshot before swapping pointers.
    const previousByteLength = this.arenaSlotCount * this.shardSize;
    const snapshot = Uint8Array.from(this.memoryView(this.arenaPtr, previousByteLength));
    const nextPtr = alloc_shard_arena(this.slotCount, this.shardSize);
    this.memoryView(nextPtr, this.slotCount * this.shardSize).set(snapshot);
    free_shard_arena(this.arenaPtr);
    this.arenaPtr = nextPtr;
    this.arenaSlotCount = this.slotCount;
  }

  prepareWritableShard(slotIndex, { duplicateMessage, overflowMessage } = {}) {
    Par3.assertInteger(slotIndex, "slot_index", 0, this.createError);
    this.ensureCapacity(slotIndex + 1, { overflowMessage });

    if (this.receivedIndices.has(slotIndex)) {
      throw this.createError(duplicateMessage ?? `duplicate shard received for slot ${slotIndex}`);
    }

    return this.memoryView(this.arenaPtr + slotIndex * this.shardSize, this.shardSize);
  }

  commitShard(slotIndex) {
    this.requestedMissingSet.delete(slotIndex);
    this.receivedIndices.add(slotIndex);
  }

  writeShard(slotIndex, bytes, { duplicateMessage, overflowMessage, sizeMessage } = {}) {
    const target = this.prepareWritableShard(slotIndex, { duplicateMessage, overflowMessage });
    const normalizedBytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);

    if (normalizedBytes.byteLength !== this.shardSize) {
      throw this.createError(
        sizeMessage ?? `slot ${slotIndex} is ${normalizedBytes.byteLength} bytes but shard_size is ${this.shardSize}`,
      );
    }

    target.set(normalizedBytes);
    this.commitShard(slotIndex);
  }

  thresholdReached() {
    return this.receivedIndices.size >= this.originalCount;
  }

  currentRequestedMissingIndices() {
    return sortNumeric(Array.from(this.requestedMissingSet));
  }

  inferMissingIndices() {
    const missing = [];

    for (let index = 0; index < this.slotCount; index += 1) {
      if (!this.receivedIndices.has(index)) {
        missing.push(index);
      }
    }

    return missing;
  }

  selectOutputIndices(requestedMissingIndices = this.currentRequestedMissingIndices()) {
    if (requestedMissingIndices.length === 0) {
      return this.inferMissingIndices();
    }

    const repairSet = new Set(this.inferMissingIndices());
    for (const index of requestedMissingIndices) {
      if (!repairSet.has(index)) {
        throw this.createError(
          `requested missing index ${index} was already provided or is outside the inferred missing set`,
        );
      }
    }

    return sortNumeric(Array.from(requestedMissingIndices));
  }

  repair() {
    const missingIndices = this.inferMissingIndices();
    if (missingIndices.length === 0) {
      return missingIndices;
    }

    const missingPtr = alloc_u32_buffer(missingIndices.length);
    try {
      new Uint32Array(memory.buffer, missingPtr, missingIndices.length).set(missingIndices);
      leopard_repair(
        this.originalCount,
        this.shardSize,
        missingPtr,
        missingIndices.length,
        this.arenaPtr,
      );
    } finally {
      free_u32_buffer(missingPtr);
    }

    return missingIndices;
  }

  readShard(slotIndex) {
    return Uint8Array.from(this.memoryView(this.arenaPtr + slotIndex * this.shardSize, this.shardSize));
  }

  free() {
    if (!this.arenaPtr) {
      return;
    }

    free_shard_arena(this.arenaPtr);
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
  }
}
