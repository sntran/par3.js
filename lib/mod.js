import * as importedWasmModule from "#pkg/par3_bg.wasm";
import * as par3Bindings from "#pkg/par3_bg.js";

function instantiateCompiledWasm(module, bindings = par3Bindings) {
  const instance = new WebAssembly.Instance(module, {
    "./par3_bg.js": {
      __wbindgen_cast_0000000000000001: bindings.__wbindgen_cast_0000000000000001,
      __wbindgen_init_externref_table: bindings.__wbindgen_init_externref_table,
    },
  });

  return instance.exports;
}

export function resolveWasmExports(moduleNamespace, bindings = par3Bindings) {
  if (moduleNamespace && typeof moduleNamespace === "object") {
    if (typeof moduleNamespace.__wbindgen_start === "function") {
      return moduleNamespace;
    }

    if (moduleNamespace.default instanceof WebAssembly.Module) {
      return instantiateCompiledWasm(moduleNamespace.default, bindings);
    }

    if (moduleNamespace.default && typeof moduleNamespace.default.__wbindgen_start === "function") {
      return moduleNamespace.default;
    }
  }

  if (moduleNamespace instanceof WebAssembly.Module) {
    return instantiateCompiledWasm(moduleNamespace, bindings);
  }

  return moduleNamespace;
}

export function initializeWasmBindings(moduleNamespace, bindings = par3Bindings) {
  const wasmModule = resolveWasmExports(moduleNamespace, bindings);

  bindings.__wbg_set_wasm(wasmModule);
  wasmModule.__wbindgen_start();

  return wasmModule;
}

const wasmModule = initializeWasmBindings(importedWasmModule);
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

/**
 * Shared JavaScript facade over the wasm PAR codec.
 *
 * A `Par3` instance validates shard layouts, owns shard storage inside wasm linear memory, and
 * tracks which slots were provided versus which repaired outputs should be returned to the caller.
 */
export class Par3 {
  static CODEC_CHUNK_BYTES = 1024 * 1024;
  static CHUNK_ALIGNMENT_BYTES = 64;

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

  static async yieldToEventLoop() {
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  }

  static assertChunkAlignment(createError = defaultCreateError) {
    if (Par3.CODEC_CHUNK_BYTES % Par3.CHUNK_ALIGNMENT_BYTES !== 0) {
      throw createError(
        `CODEC_CHUNK_BYTES must be a multiple of ${Par3.CHUNK_ALIGNMENT_BYTES} bytes, got ${Par3.CODEC_CHUNK_BYTES}`,
      );
    }
  }

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

  /**
   * Create a codec wrapper for a shard layout.
   *
   * Side effects: stores normalized layout metadata immediately, but defers wasm arena allocation
   * until the first shard write or capacity expansion.
   *
   * @param {URLSearchParams | {
   *   originalCount: number,
   *   shardSize: number,
   *   recoveryCount?: number | null,
   *   totalShardCount?: number,
   *   requestedMissingIndices?: number[],
   *   slotCount?: number,
   *   slotCountLocked?: boolean
   * }} layout Layout input accepted by `Par3.resolveLayout()`.
   * @param {{ createError?: (message: string) => Error }} [options] Custom error factory used for
   * validation and runtime failures.
   */
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

  /**
   * Expose a zero-copy shard view backed directly by wasm linear memory.
   *
   * The returned `Uint8Array` aliases the live wasm arena, so later writes can change its contents
   * and `free()` invalidates the view.
   *
   * @param {number} slotIndex Zero-based shard slot index.
   * @returns {Uint8Array} View over the shard bytes currently stored in wasm memory.
   */
  shardView(slotIndex) {
    Par3.assertInteger(slotIndex, "slot_index", 0, this.createError);
    if (slotIndex >= this.slotCount) {
      throw this.createError(`slot index ${slotIndex} is outside slot_count ${this.slotCount}`);
    }

    return this.memoryView(this.arenaPtr + slotIndex * this.shardSize, this.shardSize);
  }

  async processShardChunks(runChunk) {
    Par3.assertChunkAlignment(this.createError);

    for (let chunkOffset = 0; chunkOffset < this.shardSize; ) {
      const remainingBytes = this.shardSize - chunkOffset;
      const chunkLength = remainingBytes > Par3.CODEC_CHUNK_BYTES
        ? Par3.CODEC_CHUNK_BYTES
        : remainingBytes;

      runChunk(chunkOffset, chunkLength);
      chunkOffset += chunkLength;

      if (chunkOffset < this.shardSize) {
        await Par3.yieldToEventLoop();
      }
    }
  }

  /**
   * Repair every currently missing slot in place inside the wasm arena.
   *
   * Side effects: overwrites missing shard slots in wasm memory and leaves the repaired bytes in
   * place for later zero-copy reads or copied exports.
   *
   * @returns {Promise<number[]>} Zero-based slot indexes that were repaired.
   * @throws {Error} If too few shards have been received to satisfy the repair threshold.
   */
  async repair() {
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
      await this.processShardChunks((chunkOffset, chunkLength) => {
        Par3.bindings.leopard_repair(
          this.originalCount,
          this.shardSize,
          chunkOffset,
          chunkLength,
          missingHandle,
          missingIndices.length,
          this.arenaHandle,
        );
      });
    } finally {
      Par3.bindings.free_u32_buffer(missingHandle);
    }

    return missingIndices;
  }

  /**
   * Encode recovery shards in place inside the wasm arena.
   *
   * Side effects: mutates recovery slots in wasm memory, marks the generated recovery shards as
   * received, and removes them from the requested-missing set.
   *
   * @returns {Promise<number[]>} Zero-based recovery slot indexes written into the arena.
   * @throws {Error} If any original shard is still missing from the arena.
   */
  async encode() {
    const recoveryIndices = this.recoveryIndices();
    if (recoveryIndices.length === 0) {
      return recoveryIndices;
    }

    for (let index = 0; index < this.originalCount; index += 1) {
      if (!this.receivedIndices.has(index)) {
        throw this.createError(`original shard ${index} must be written before encode`);
      }
    }

    await this.processShardChunks((chunkOffset, chunkLength) => {
      Par3.bindings.leopard_encode(
        this.originalCount,
        this.shardSize,
        chunkOffset,
        chunkLength,
        this.arenaHandle,
      );
    });

    for (const index of recoveryIndices) {
      this.requestedMissingSet.delete(index);
      this.receivedIndices.add(index);
    }

    return recoveryIndices;
  }

  /**
   * Copy a shard out of the wasm arena.
   *
   * This method returns an owned copy, so the resulting `Uint8Array` remains valid after `free()`.
   * For zero-copy access, use `shardView()`, which aliases wasm memory and becomes invalid once
   * the arena is released.
   *
   * @param {number} slotIndex Zero-based shard slot index.
   * @returns {Uint8Array} Copied shard bytes for the requested slot.
   */
  readShard(slotIndex) {
    return Uint8Array.from(this.shardView(slotIndex));
  }

  /**
   * Release the wasm shard arena owned by this codec.
   *
   * Side effects: frees wasm linear memory for shard storage, resets local arena bookkeeping, and
   * invalidates any zero-copy views previously returned by `shardView()`.
   *
   * @returns {void}
   */
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
