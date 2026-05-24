import * as importedWasmModule from "#pkg/par3_bg.wasm";
import * as par3Bindings from "#pkg/par3_bg.js";
import { BasePar3, MAX_ARENA_BYTES, MAX_SHARD_SIZE, MAX_SLOT_COUNT } from "./par3-core.js";

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

export { MAX_ARENA_BYTES, MAX_SHARD_SIZE, MAX_SLOT_COUNT };

export class Par3 extends BasePar3 {
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

  constructor(layout, options = {}) {
    super(layout, options);
    this.arenaHandle = NULL_HANDLE;
    this.arenaPtr = 0;
  }

  memoryView(pointer, byteLength) {
    return new Uint8Array(memory.buffer, pointer, byteLength);
  }

  arenaView(offset, byteLength) {
    return new Uint8Array(memory.buffer, this.arenaPtr + offset, byteLength);
  }

  ensureCapacity(requiredSlotCount, { overflowMessage } = {}) {
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

    if (this.arenaHandle === NULL_HANDLE) {
      this.arenaHandle = Par3Class.bindings.alloc_shard_arena(this.slotCount, this.shardSize);
      this.arenaPtr = Par3Class.bindings.shard_arena_ptr(this.arenaHandle);
      this.arenaSlotCount = this.slotCount;
      return;
    }

    if (this.slotCount <= this.arenaSlotCount) {
      return;
    }

    const previousByteLength = this.arenaSlotCount * this.shardSize;
  const snapshot = Uint8Array.from(this.arenaView(0, previousByteLength));
    const nextHandle = Par3Class.bindings.alloc_shard_arena(this.slotCount, this.shardSize);
    const nextPtr = Par3Class.bindings.shard_arena_ptr(nextHandle);
    new Uint8Array(memory.buffer, nextPtr, this.slotCount * this.shardSize).set(snapshot);
    Par3Class.bindings.free_shard_arena(this.arenaHandle);
    this.arenaHandle = nextHandle;
    this.arenaPtr = nextPtr;
    this.arenaSlotCount = this.slotCount;
  }

  async processShardChunks(runChunk) {
    const Par3Class = this.constructor;
    Par3Class.assertChunkAlignment(this.createError);

    for (let chunkOffset = 0; chunkOffset < this.shardSize; ) {
      const remainingBytes = this.shardSize - chunkOffset;
      const chunkLength = remainingBytes > Par3Class.CODEC_CHUNK_BYTES
        ? Par3Class.CODEC_CHUNK_BYTES
        : remainingBytes;

      runChunk(chunkOffset, chunkLength);
      chunkOffset += chunkLength;

      if (chunkOffset < this.shardSize) {
        await Par3Class.yieldToEventLoop();
      }
    }
  }

  async repair() {
    const Par3Class = this.constructor;
    const missingIndices = this.inferMissingIndices();
    if (missingIndices.length === 0) {
      return missingIndices;
    }

    if (!this.thresholdReached() || this.arenaHandle === NULL_HANDLE) {
      throw this.createError("Insufficient shards provided for repair");
    }

    const missingHandle = Par3Class.bindings.alloc_u32_buffer(missingIndices.length);
    try {
      const missingPtr = Par3Class.bindings.u32_buffer_ptr(missingHandle);
      new Uint32Array(memory.buffer, missingPtr, missingIndices.length).set(missingIndices);
      await this.processShardChunks((chunkOffset, chunkLength) => {
        Par3Class.bindings.leopard_repair(
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
      Par3Class.bindings.free_u32_buffer(missingHandle);
    }

    return missingIndices;
  }

  async encode() {
    const Par3Class = this.constructor;
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
      Par3Class.bindings.leopard_encode(
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

  free() {
    const Par3Class = this.constructor;
    if (this.arenaHandle === NULL_HANDLE) {
      return;
    }

    Par3Class.bindings.free_shard_arena(this.arenaHandle);
    this.arenaHandle = NULL_HANDLE;
    this.arenaPtr = 0;
    this.arenaSlotCount = 0;
  }
}
