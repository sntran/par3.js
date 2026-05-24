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

export class BasePar3 {
  static CODEC_CHUNK_BYTES = 1024 * 1024;
  static CHUNK_ALIGNMENT_BYTES = 64;
  static MAX_SLOT_COUNT = MAX_SLOT_COUNT;
  static MAX_SHARD_SIZE = MAX_SHARD_SIZE;
  static MAX_ARENA_BYTES = MAX_ARENA_BYTES;

  static SEARCH_PARAM_ALIASES = Object.freeze({
    missingIndices: ["i", "missing_indices"],
    originalCount: ["n", "original_count"],
    recoveryCount: ["r", "recovery_count"],
    shardSize: ["s", "shard_size"],
    totalShardCount: ["total_shard_count"],
  });

  static async yieldToEventLoop() {
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  }

  static assertChunkAlignment(createError = defaultCreateError) {
    if (this.CODEC_CHUNK_BYTES % this.CHUNK_ALIGNMENT_BYTES !== 0) {
      throw createError(
        `CODEC_CHUNK_BYTES must be a multiple of ${this.CHUNK_ALIGNMENT_BYTES} bytes, got ${this.CODEC_CHUNK_BYTES}`,
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
      && !this.isSearchParams(value)
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

    return this.normalizeIndices(splitDelimitedValues(values).map((value) => {
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

    return this.resolveObjectLayout({
      createError,
      fieldNames: names,
      originalCount: toIntegerCandidate(this.readSearchParam(
        searchParams,
        this.SEARCH_PARAM_ALIASES.originalCount,
        names.originalCount,
        createError,
      )),
      recoveryCount: toIntegerCandidate(this.readSearchParam(
        searchParams,
        this.SEARCH_PARAM_ALIASES.recoveryCount,
        names.recoveryCount,
        createError,
      )),
      requestedMissingIndices: this.readSearchParamIndices(
        searchParams,
        this.SEARCH_PARAM_ALIASES.missingIndices,
        names.missingIndices,
        createError,
      ),
      shardSize: toIntegerCandidate(this.readSearchParam(
        searchParams,
        this.SEARCH_PARAM_ALIASES.shardSize,
        names.shardSize,
        createError,
      )),
      totalShardCount: toIntegerCandidate(this.readSearchParam(
        searchParams,
        this.SEARCH_PARAM_ALIASES.totalShardCount,
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

    if (slotCount > this.MAX_SLOT_COUNT) {
      throw createError(`${slotCountField} must be less than or equal to ${this.MAX_SLOT_COUNT}`);
    }

    if (shardSize > this.MAX_SHARD_SIZE) {
      throw createError(`${shardSizeField} must be less than or equal to ${this.MAX_SHARD_SIZE}`);
    }

    if (slotCount > Math.floor(this.MAX_ARENA_BYTES / shardSize)) {
      throw createError(
        `slot layout exceeds maximum arena size of ${this.MAX_ARENA_BYTES} bytes`,
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
      unique.add(this.assertInteger(rawIndex, `${field}[]`, minimum, createError));
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

    const normalizedRequestedMissingIndices = this.normalizeIndices(requestedMissingIndices, {
      field: names.missingIndices,
      createError,
    });
    const normalizedOriginalCount = this.assertInteger(
      originalCount,
      names.originalCount,
      1,
      createError,
    );
    const normalizedShardSize = this.assertInteger(
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
      : this.assertInteger(recoveryCount, names.recoveryCount, 0, createError);
    const hasExplicitSlotCount = totalShardCount !== null && totalShardCount !== undefined;
    const hasLockedSlotCount = hasExplicitSlotCount || normalizedRecoveryCount !== null;
    const normalizedSlotCount = !hasExplicitSlotCount
      ? (normalizedRecoveryCount === null
        ? Math.max(normalizedOriginalCount, (normalizedRequestedMissingIndices.at(-1) ?? 0) + 1)
        : normalizedOriginalCount + normalizedRecoveryCount)
      : this.assertInteger(totalShardCount, names.totalShardCount, normalizedOriginalCount, createError);

    this.validateCapacity(normalizedSlotCount, normalizedShardSize, {
      createError,
      fieldNames: names,
    });
    this.validateRequestedMissingIndices(normalizedRequestedMissingIndices, normalizedSlotCount, {
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
    if (this.isSearchParams(layout)) {
      return this.resolveLayoutFromSearchParams(layout, options);
    }

    return this.resolveObjectLayout(layout, options);
  }

  constructor(
    layout,
    { createError = defaultCreateError } = {},
  ) {
    const Par3Class = this.constructor;
    const resolvedLayout = Par3Class.isResolvedLayout(layout)
      ? layout
      : Par3Class.resolveLayout(layout, { createError });

    this.arenaSlotCount = 0;
    this.createError = createError;
    this.originalCount = resolvedLayout.originalCount;
    this.receivedIndices = new Set();
    this.requestedMissingSet = new Set(resolvedLayout.requestedMissingIndices);
    this.shardSize = resolvedLayout.shardSize;
    this.slotCount = resolvedLayout.slotCount;
    this.slotCountLocked = resolvedLayout.slotCountLocked;
  }

  arenaView() {
    throw new Error("Subclass must implement arenaView()");
  }

  ensureCapacity() {
    throw new Error("Subclass must implement ensureCapacity()");
  }

  prepareWritableShard(slotIndex, { duplicateMessage, overflowMessage } = {}) {
    const Par3Class = this.constructor;
    Par3Class.assertInteger(slotIndex, "slot_index", 0, this.createError);
    this.ensureCapacity(slotIndex + 1, { overflowMessage });

    if (this.receivedIndices.has(slotIndex)) {
      throw this.createError(duplicateMessage ?? `duplicate shard received for slot ${slotIndex}`);
    }

    return this.arenaView(slotIndex * this.shardSize, this.shardSize);
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
    const Par3Class = this.constructor;
    Par3Class.assertInteger(slotIndex, "slot_index", 0, this.createError);
    if (slotIndex >= this.slotCount) {
      throw this.createError(`slot index ${slotIndex} is outside slot_count ${this.slotCount}`);
    }

    return this.arenaView(slotIndex * this.shardSize, this.shardSize);
  }

  readShard(slotIndex) {
    return Uint8Array.from(this.shardView(slotIndex));
  }
}
