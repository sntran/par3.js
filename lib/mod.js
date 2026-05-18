import * as wasmModule from "../pkg/par3_bg.wasm";
import {
  alloc_shard_arena,
  alloc_u32_buffer,
  free_shard_arena,
  free_u32_buffer,
  leopard_repair,
  shard_arena_ptr,
  u32_buffer_ptr,
} from "../pkg/par3.js";

const { memory } = wasmModule;
const NULL_HANDLE = 0n;

export const MAX_SLOT_COUNT = 32_768;
export const MAX_SHARD_SIZE = 10 * 1024 * 1024;
export const MAX_ARENA_BYTES = 128 * 1024 * 1024;

function defaultCreateError(message) {
  return new Error(message);
}

function sortNumeric(values) {
  return values.toSorted((left, right) => left - right);
}

export class Par3 {
  static assertInteger(value, field, minimum = 0, createError = defaultCreateError) {
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw createError(`${field} must be an integer greater than or equal to ${minimum}`);
    }

    return value;
  }

  static validateCapacity(
    slotCount,
    shardSize,
    { createError = defaultCreateError, fieldNames = {} } = {},
  ) {
    const slotCountField = fieldNames.totalShardCount ?? "total_shard_count";
    const shardSizeField = fieldNames.shardSize ?? "shard_size";

    if (slotCount > MAX_SLOT_COUNT) {
      throw createError(`${slotCountField} must be less than or equal to ${MAX_SLOT_COUNT}`);
    }

    if (shardSize > MAX_SHARD_SIZE) {
      throw createError(`${shardSizeField} must be less than or equal to ${MAX_SHARD_SIZE}`);
    }

    if (slotCount > Math.floor(MAX_ARENA_BYTES / shardSize)) {
      throw createError(
        `slot layout exceeds maximum arena size of ${MAX_ARENA_BYTES} bytes`,
      );
    }
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

    Par3.validateCapacity(normalizedSlotCount, normalizedShardSize, {
      createError,
      fieldNames: names,
    });

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
    this.arenaHandle = NULL_HANDLE;
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

      Par3.validateCapacity(requiredSlotCount, this.shardSize, {
        createError: this.createError,
      });
      this.slotCount = requiredSlotCount;
    }

    if (this.arenaHandle === NULL_HANDLE) {
      this.arenaHandle = alloc_shard_arena(this.slotCount, this.shardSize);
      this.arenaPtr = shard_arena_ptr(this.arenaHandle);
      this.arenaSlotCount = this.slotCount;
      return;
    }

    if (this.slotCount <= this.arenaSlotCount) {
      return;
    }

    // Wasm arenas cannot grow in place, so expansion takes a snapshot before swapping pointers.
    const previousByteLength = this.arenaSlotCount * this.shardSize;
    const snapshot = Uint8Array.from(this.memoryView(this.arenaPtr, previousByteLength));
    const nextHandle = alloc_shard_arena(this.slotCount, this.shardSize);
    const nextPtr = shard_arena_ptr(nextHandle);
    this.memoryView(nextPtr, this.slotCount * this.shardSize).set(snapshot);
    free_shard_arena(this.arenaHandle);
    this.arenaHandle = nextHandle;
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

    const missingHandle = alloc_u32_buffer(missingIndices.length);
    try {
      const missingPtr = u32_buffer_ptr(missingHandle);
      new Uint32Array(memory.buffer, missingPtr, missingIndices.length).set(missingIndices);
      leopard_repair(
        this.originalCount,
        this.shardSize,
        missingHandle,
        missingIndices.length,
        this.arenaHandle,
      );
    } finally {
      free_u32_buffer(missingHandle);
    }

    return missingIndices;
  }

  readShard(slotIndex) {
    Par3.assertInteger(slotIndex, "slot_index", 0, this.createError);
    if (slotIndex >= this.slotCount) {
      throw this.createError(`slot index ${slotIndex} is outside slot_count ${this.slotCount}`);
    }

    return Uint8Array.from(this.memoryView(this.arenaPtr + slotIndex * this.shardSize, this.shardSize));
  }

  free() {
    if (this.arenaHandle === NULL_HANDLE) {
      return;
    }

    free_shard_arena(this.arenaHandle);
    this.arenaHandle = NULL_HANDLE;
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
  }
}
