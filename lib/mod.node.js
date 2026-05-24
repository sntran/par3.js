import { createRequire } from "node:module";

import { BasePar3, MAX_SHARD_SIZE, MAX_SLOT_COUNT } from "./par3-core.js";

const require = createRequire(import.meta.url);
const MAX_ARENA_BYTES = 512 * 1024 * 1024;
const EMPTY_VIEW_BUFFER = new ArrayBuffer(0);
const UNHANDLED_VIEW_PROPERTY = Symbol("unhandledViewProperty");
const VIEW_INVALIDATION_BACKGROUND = "background";
const VIEW_INVALIDATION_RESIZED = "resized";
const VIEW_INVALIDATION_FREED = "freed";
const VIEW_INVALIDATION_MESSAGES = {
  [VIEW_INVALIDATION_BACKGROUND]: "Par3 zero-copy view was invalidated before background processing began",
  [VIEW_INVALIDATION_RESIZED]: "Par3 zero-copy view was invalidated by arena growth",
  [VIEW_INVALIDATION_FREED]: "Par3 zero-copy view was invalidated by free()",
};

function loadNativeBindings() {
  return require("./par3.node");
}

const nativeBindings = loadNativeBindings();

export { MAX_ARENA_BYTES, MAX_SHARD_SIZE, MAX_SLOT_COUNT };

export class Par3 extends BasePar3 {
  static MAX_ARENA_BYTES = MAX_ARENA_BYTES;
  // The Rust arena is never exposed as a public field. Every borrowed view leaves through a Proxy
  // so the facade can revoke it before a background N-API task gets mutable access to the backing
  // store.
  #arena = null;
  #isProcessing = false;
  #issuedViewStates = new Set();

  constructor(layout, options = {}) {
    super(layout, options);
  }

  #assertIdle() {
    if (this.#isProcessing) {
      throw this.createError("Par3 codec is currently processing on a background thread");
    }
  }

  #ensureArena() {
    if (this.#arena === null) {
      this.#arena = nativeBindings.allocate_aligned_arena(this.slotCount * this.shardSize);
      this.arenaSlotCount = this.slotCount;
    }

    return this.#arena;
  }

  #arenaSlice(offset, byteLength) {
    const arena = this.#ensureArena();
    return arena.subarray(offset, offset + byteLength);
  }

  #createInvalidatedViewError(reason) {
    return this.createError(VIEW_INVALIDATION_MESSAGES[reason]);
  }

  #invalidateIssuedViews(reason) {
    for (const state of this.#issuedViewStates) {
      state.invalidationReason = reason;
    }

    this.#issuedViewStates.clear();
  }

  #viewStructuralMetadata(target, property, invalidationReason) {
    // `.buffer` is the escape hatch that would let callers rewrap the whole external ArrayBuffer
    // and keep mutating bytes after this Proxy is invalidated. Return an inert buffer and controlled
    // offsets instead; stale background views report zero length before any data access can occur.
    if (property === "buffer" || property === "parent") {
      return EMPTY_VIEW_BUFFER;
    }

    if (property === "byteLength") {
      return invalidationReason === VIEW_INVALIDATION_BACKGROUND
        ? 0
        : Reflect.get(target, property, target);
    }

    if (property === "byteOffset" || property === "offset") {
      return invalidationReason === VIEW_INVALIDATION_BACKGROUND
        ? 0
        : Reflect.get(target, property, target);
    }

    return UNHANDLED_VIEW_PROPERTY;
  }

  #issueIterator(iterator, viewState) {
    let proxy;

    proxy = new Proxy(iterator, {
      get: (target, property) => {
        if (viewState.invalidationReason !== null) {
          throw this.#createInvalidatedViewError(viewState.invalidationReason);
        }

        this.#assertIdle();
        const value = Reflect.get(target, property, target);
        if (typeof value !== "function") {
          return value;
        }

        return (...args) => {
          if (viewState.invalidationReason !== null) {
            throw this.#createInvalidatedViewError(viewState.invalidationReason);
          }

          this.#assertIdle();
          const result = Reflect.apply(value, target, args);
          return result === target ? proxy : result;
        };
      },
    });

    return proxy;
  }

  #issueView(view) {
    // All shard aliases run through these traps. Direct reads and writes check the processing lock,
    // derived Buffer views are wrapped again, and iterators stay tied to the same invalidation
    // state as the source view.
    const viewState = {
      invalidationReason: null,
    };
    let proxy;
    const issuedProxy = new Proxy(view, {
      get: (target, property) => {
        const structuralValue = this.#viewStructuralMetadata(
          target,
          property,
          viewState.invalidationReason,
        );
        if (structuralValue !== UNHANDLED_VIEW_PROPERTY) {
          if (viewState.invalidationReason !== null && viewState.invalidationReason !== VIEW_INVALIDATION_BACKGROUND) {
            throw this.#createInvalidatedViewError(viewState.invalidationReason);
          }

          if (viewState.invalidationReason === null) {
            this.#assertIdle();
          }

          return structuralValue;
        }

        if (viewState.invalidationReason !== null) {
          throw this.#createInvalidatedViewError(viewState.invalidationReason);
        }

        this.#assertIdle();
        const value = Reflect.get(target, property, target);

        if (typeof value !== "function") {
          return value;
        }

        if (property === "valueOf") {
          return (...args) => {
            this.#assertIdle();
            return Buffer.from(Reflect.apply(value, target, args));
          };
        }

        return (...args) => {
          if (viewState.invalidationReason !== null) {
            throw this.#createInvalidatedViewError(viewState.invalidationReason);
          }

          this.#assertIdle();
          const result = Reflect.apply(value, target, args);

          if (result === target) {
            return proxy;
          }

          if (Buffer.isBuffer(result)) {
            return this.#issueView(result);
          }

          if (result && typeof result === "object" && typeof result.next === "function") {
            return this.#issueIterator(result, viewState);
          }

          return result;
        };
      },
      set: (target, property, value) => {
        if (viewState.invalidationReason !== null) {
          throw this.#createInvalidatedViewError(viewState.invalidationReason);
        }

        this.#assertIdle();
        return Reflect.set(target, property, value, target);
      },
    });

    proxy = issuedProxy;

    this.#issuedViewStates.add(viewState);
    return issuedProxy;
  }

  async #runWhileLocked(operation) {
    this.#assertIdle();
    // Logical invalidation happens before Rust receives the Buffer. At that point any previously
    // issued JS view is dead, which blocks concurrent mutation while Rayon owns the arena.
    this.#invalidateIssuedViews(VIEW_INVALIDATION_BACKGROUND);
    this.#isProcessing = true;

    try {
      return await operation();
    } finally {
      this.#isProcessing = false;
    }
  }

  arenaView(offset, byteLength) {
    this.#assertIdle();
    return this.#issueView(this.#arenaSlice(offset, byteLength));
  }

  memoryView(offset, byteLength) {
    this.#assertIdle();
    return this.#issueView(this.#arenaSlice(offset, byteLength));
  }

  ensureCapacity(requiredSlotCount, { overflowMessage } = {}) {
    this.#assertIdle();
    const Par3Class = this.constructor;

    if (requiredSlotCount > this.slotCount) {
      if (this.slotCountLocked) {
        throw this.createError(
          overflowMessage ?? `encountered slot index outside declared total_shard_count (${this.slotCount})`,
        );
      }

      Par3Class.validateCapacity(requiredSlotCount, this.shardSize, {
        createError: this.createError,
      });
      this.slotCount = requiredSlotCount;
    }

    if (this.#arena === null) {
      this.#arena = nativeBindings.allocate_aligned_arena(this.slotCount * this.shardSize);
      this.arenaSlotCount = this.slotCount;
      return;
    }

    if (this.slotCount <= this.arenaSlotCount) {
      return;
    }

    this.#invalidateIssuedViews(VIEW_INVALIDATION_RESIZED);
    const nextArena = nativeBindings.allocate_aligned_arena(this.slotCount * this.shardSize);
    this.#arena.copy(nextArena, 0, 0, this.arenaSlotCount * this.shardSize);
    this.#arena = nextArena;
    this.arenaSlotCount = this.slotCount;
  }

  writeShard(slotIndex, bytes, { duplicateMessage, overflowMessage, sizeMessage } = {}) {
    this.#assertIdle();
    const Par3Class = this.constructor;
    Par3Class.assertInteger(slotIndex, "slot_index", 0, this.createError);
    this.ensureCapacity(slotIndex + 1, { overflowMessage });

    if (this.receivedIndices.has(slotIndex)) {
      throw this.createError(duplicateMessage ?? `duplicate shard received for slot ${slotIndex}`);
    }

    const normalizedBytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    if (normalizedBytes.byteLength !== this.shardSize) {
      throw this.createError(
        sizeMessage ?? `slot ${slotIndex} is ${normalizedBytes.byteLength} bytes but shard_size is ${this.shardSize}`,
      );
    }

    this.#arenaSlice(slotIndex * this.shardSize, this.shardSize).set(normalizedBytes);
    this.commitShard(slotIndex);
  }

  shardView(slotIndex) {
    this.#assertIdle();
    return super.shardView(slotIndex);
  }

  readShard(slotIndex) {
    this.#assertIdle();
    const Par3Class = this.constructor;
    Par3Class.assertInteger(slotIndex, "slot_index", 0, this.createError);
    if (slotIndex >= this.slotCount) {
      throw this.createError(`slot index ${slotIndex} is outside slot_count ${this.slotCount}`);
    }

    return Uint8Array.from(this.#arenaSlice(slotIndex * this.shardSize, this.shardSize));
  }

  async repair() {
    const missingIndices = this.inferMissingIndices();
    if (missingIndices.length === 0) {
      return missingIndices;
    }

    if (!this.thresholdReached()) {
      throw this.createError("Insufficient shards provided for repair");
    }

    return this.#runWhileLocked(async () => {
      await nativeBindings.leopard_repair(
        this.originalCount,
        this.shardSize,
        missingIndices,
        this.#arena,
      );
      return missingIndices;
    });
  }

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

    return this.#runWhileLocked(async () => {
      await nativeBindings.leopard_encode(this.originalCount, this.shardSize, this.#arena);

      for (const index of recoveryIndices) {
        this.requestedMissingSet.delete(index);
        this.receivedIndices.add(index);
      }

      return recoveryIndices;
    });
  }

  free() {
    this.#assertIdle();
    // Drop JS reachability, not native memory directly. V8 will sweep the Buffer and run the Rust
    // finalizer that owns physical deallocation.
    this.#invalidateIssuedViews(VIEW_INVALIDATION_FREED);
    this.#arena = null;
    this.arenaSlotCount = 0;
  }
}
