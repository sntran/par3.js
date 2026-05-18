import * as wasmModule from "../pkg/par3_bg.wasm";
import * as par3Bindings from "../pkg/par3.js";

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

function splitDelimitedValues(rawValues) {
  const values = [];

  for (const rawValue of rawValues) {
    for (const value of rawValue.split(",")) {
      values.push(value.trim());
    }
  }

  return values;
}

function toIntegerCandidate(rawValue) {
  if (rawValue === undefined) {
    return undefined;
  }

  if (typeof rawValue === "string" && rawValue.trim().length === 0) {
    return Number.NaN;
  }

  return Number(rawValue);
}

export class Par3 {
  static SEARCH_PARAM_ALIASES = Object.freeze({
    missingIndices: ["i", "missing_indices"],
    originalCount: ["n", "original_count"],
    recoveryCount: ["r", "recovery_count"],
    shardSize: ["s", "shard_size"],
    totalShardCount: ["total_shard_count"],
  });

  static bindings = {
    alloc_shard_arena: par3Bindings.alloc_shard_arena,
    alloc_u32_buffer: par3Bindings.alloc_u32_buffer,
    free_shard_arena: par3Bindings.free_shard_arena,
    free_u32_buffer: par3Bindings.free_u32_buffer,
    leopard_encode: par3Bindings.leopard_encode,
    leopard_repair: par3Bindings.leopard_repair,
    shard_arena_ptr: par3Bindings.shard_arena_ptr,
    u32_buffer_ptr: par3Bindings.u32_buffer_ptr,
  };

  static assertInteger(value, field, minimum = 0, createError = defaultCreateError) {
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw createError(`${field} must be an integer greater than or equal to ${minimum}`);
    }

    return value;
  }

  static isResolvedLayout(value) {
    return Boolean(value)
      && !Par3.isSearchParams(value)
      && Number.isSafeInteger(value.originalCount)
      && Array.isArray(value.requestedMissingIndices)
      && Number.isSafeInteger(value.shardSize)
      && Number.isSafeInteger(value.slotCount)
      && typeof value.slotCountLocked === "boolean";
  }

  static isSearchParams(value) {
    return value instanceof URLSearchParams
      || (
        value !== null
        && typeof value === "object"
        && typeof value.get === "function"
        && typeof value.getAll === "function"
        && typeof value.has === "function"
      );
  }

  static readSearchParam(searchParams, aliases, field, createError = defaultCreateError) {
    const values = [];

    for (const alias of aliases) {
      for (const value of searchParams.getAll(alias)) {
        values.push(value);
      }
    }

    if (values.length === 0) {
      return undefined;
    }

    if (values.length > 1) {
      throw createError(`${field} may only be provided once`);
    }

    return values[0];
  }

  static readSearchParamIndices(
    searchParams,
    aliases,
    field,
    createError = defaultCreateError,
  ) {
    const values = [];

    for (const alias of aliases) {
      values.push(...searchParams.getAll(alias));
    }

    if (values.length === 0) {
      return [];
    }

    return Par3.normalizeIndices(splitDelimitedValues(values).map((value) => {
      if (value.length === 0) {
        return Number.NaN;
      }

      return toIntegerCandidate(value);
    }), {
      field,
      createError,
    });
  }

  static resolveLayoutFromSearchParams(
    searchParams,
    { createError = defaultCreateError, fieldNames = {} } = {},
  ) {
    const names = {
      missingIndices: fieldNames.missingIndices ?? "i",
      originalCount: fieldNames.originalCount ?? "n",
      recoveryCount: fieldNames.recoveryCount ?? "r",
      shardSize: fieldNames.shardSize ?? "s",
      totalShardCount: fieldNames.totalShardCount ?? "total_shard_count",
    };

    return Par3.resolveObjectLayout({
      createError,
      fieldNames: names,
      originalCount: toIntegerCandidate(Par3.readSearchParam(
        searchParams,
        Par3.SEARCH_PARAM_ALIASES.originalCount,
        names.originalCount,
        createError,
      )),
      recoveryCount: toIntegerCandidate(Par3.readSearchParam(
        searchParams,
        Par3.SEARCH_PARAM_ALIASES.recoveryCount,
        names.recoveryCount,
        createError,
      )),
      requestedMissingIndices: Par3.readSearchParamIndices(
        searchParams,
        Par3.SEARCH_PARAM_ALIASES.missingIndices,
        names.missingIndices,
        createError,
      ),
      shardSize: toIntegerCandidate(Par3.readSearchParam(
        searchParams,
        Par3.SEARCH_PARAM_ALIASES.shardSize,
        names.shardSize,
        createError,
      )),
      totalShardCount: toIntegerCandidate(Par3.readSearchParam(
        searchParams,
        Par3.SEARCH_PARAM_ALIASES.totalShardCount,
        names.totalShardCount,
        createError,
      )),
    });
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

  static validateRequestedMissingIndices(
    requestedMissingIndices,
    slotCount,
    {
      field = "missing_indices",
      slotCountField = "total_shard_count",
      createError = defaultCreateError,
    } = {},
  ) {
    for (const index of requestedMissingIndices) {
      if (index >= slotCount) {
        throw createError(`${field}[] must be less than ${slotCountField} ${slotCount}`);
      }
    }
  }

  static resolveObjectLayout({
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
    Par3.validateRequestedMissingIndices(normalizedRequestedMissingIndices, normalizedSlotCount, {
      field: names.missingIndices,
      slotCountField: names.totalShardCount,
      createError,
    });

    return {
      originalCount: normalizedOriginalCount,
      requestedMissingIndices: normalizedRequestedMissingIndices,
      shardSize: normalizedShardSize,
      slotCount: normalizedSlotCount,
      slotCountLocked: hasLockedSlotCount,
    };
  }

  static resolveLayout(layout, options = {}) {
    if (Par3.isSearchParams(layout)) {
      return Par3.resolveLayoutFromSearchParams(layout, options);
    }

    return Par3.resolveObjectLayout(layout, options);
  }

  constructor(
    layout,
    { createError = defaultCreateError } = {},
  ) {
    const resolvedLayout = Par3.isResolvedLayout(layout)
      ? layout
      : Par3.resolveLayout(layout, { createError });

    this.arenaHandle = NULL_HANDLE;
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
    this.createError = createError;
    this.originalCount = resolvedLayout.originalCount;
    this.receivedIndices = new Set();
    this.requestedMissingSet = new Set(resolvedLayout.requestedMissingIndices);
    this.shardSize = resolvedLayout.shardSize;
    this.slotCount = resolvedLayout.slotCount;
    this.slotCountLocked = resolvedLayout.slotCountLocked;
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
      this.arenaHandle = Par3.bindings.alloc_shard_arena(this.slotCount, this.shardSize);
      this.arenaPtr = Par3.bindings.shard_arena_ptr(this.arenaHandle);
      this.arenaSlotCount = this.slotCount;
      return;
    }

    if (this.slotCount <= this.arenaSlotCount) {
      return;
    }

    // Wasm arenas cannot grow in place, so expansion takes a snapshot before swapping pointers.
    const previousByteLength = this.arenaSlotCount * this.shardSize;
    const snapshot = Uint8Array.from(this.memoryView(this.arenaPtr, previousByteLength));
    const nextHandle = Par3.bindings.alloc_shard_arena(this.slotCount, this.shardSize);
    const nextPtr = Par3.bindings.shard_arena_ptr(nextHandle);
    this.memoryView(nextPtr, this.slotCount * this.shardSize).set(snapshot);
    Par3.bindings.free_shard_arena(this.arenaHandle);
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

  recoveryIndices() {
    return Array.from(
      { length: Math.max(0, this.slotCount - this.originalCount) },
      (_, index) => this.originalCount + index,
    );
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

  shardView(slotIndex) {
    Par3.assertInteger(slotIndex, "slot_index", 0, this.createError);
    if (slotIndex >= this.slotCount) {
      throw this.createError(`slot index ${slotIndex} is outside slot_count ${this.slotCount}`);
    }

    return this.memoryView(this.arenaPtr + slotIndex * this.shardSize, this.shardSize);
  }

  repair() {
    const missingIndices = this.inferMissingIndices();
    if (missingIndices.length === 0) {
      return missingIndices;
    }

    if (!this.thresholdReached() || this.arenaHandle === NULL_HANDLE) {
      throw this.createError("Insufficient shards provided for repair");
    }

    const missingHandle = Par3.bindings.alloc_u32_buffer(missingIndices.length);
    try {
      const missingPtr = Par3.bindings.u32_buffer_ptr(missingHandle);
      new Uint32Array(memory.buffer, missingPtr, missingIndices.length).set(missingIndices);
      Par3.bindings.leopard_repair(
        this.originalCount,
        this.shardSize,
        missingHandle,
        missingIndices.length,
        this.arenaHandle,
      );
    } finally {
      Par3.bindings.free_u32_buffer(missingHandle);
    }

    return missingIndices;
  }

  encode() {
    const recoveryIndices = this.recoveryIndices();
    if (recoveryIndices.length === 0) {
      return recoveryIndices;
    }

    for (let index = 0; index < this.originalCount; index += 1) {
      if (!this.receivedIndices.has(index)) {
        throw this.createError(`original shard ${index} must be written before encode`);
      }
    }

    Par3.bindings.leopard_encode(this.originalCount, this.shardSize, this.arenaHandle);

    for (const index of recoveryIndices) {
      this.requestedMissingSet.delete(index);
      this.receivedIndices.add(index);
    }

    return recoveryIndices;
  }

  readShard(slotIndex) {
    return Uint8Array.from(this.shardView(slotIndex));
  }

  free() {
    if (this.arenaHandle === NULL_HANDLE) {
      return;
    }

    Par3.bindings.free_shard_arena(this.arenaHandle);
    this.arenaHandle = NULL_HANDLE;
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
  }
}
