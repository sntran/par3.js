//! WebAssembly-facing PAR repair and encoding primitives.
//!
//! The Rust core owns two low-level responsibilities:
//! 1. managing typed buffers inside linear memory so JavaScript can stream bytes in place,
//! 2. delegating encoding and repair work to `reed-solomon-simd` without copying more than necessary.
//!
//! The exported API deliberately works in terms of generation-tagged allocation handles plus
//! explicit pointer lookups because the Cloudflare Worker and the local CLI both need to fill shard
//! arenas directly from Web Streams or filesystem reads. Tests exercise the exact same low-level
//! entrypoints so that the native suite stays close to production behavior.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::sync::{Mutex, OnceLock};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, Copy)]
struct ShardArenaMeta {
    byte_len: usize,
    slot_count: usize,
    shard_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct U32BufferMeta {
    len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocationHandle(u64);

impl AllocationHandle {
    fn new(slot_id: u32, generation: u32) -> Self {
        Self((u64::from(generation) << 32) | u64::from(slot_id))
    }

    fn null() -> Self {
        Self(0)
    }

    fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    fn into_raw(self) -> u64 {
        self.0
    }

    fn is_null(self) -> bool {
        self.0 == 0
    }

    fn slot_index(self) -> Option<usize> {
        let slot_id = self.0 as u32;
        if slot_id == 0 {
            None
        } else {
            Some((slot_id - 1) as usize)
        }
    }

    fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

#[derive(Debug, Clone, Copy)]
enum Allocation {
    ShardArena(ShardArenaMeta),
    U32Buffer(U32BufferMeta),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Errors produced while allocating arenas or running repair/encode operations.
///
/// The variants are intentionally explicit because JavaScript callers receive these as strings.
/// Keeping the failure modes stable makes the Worker and CLI easier to diagnose when shard input
/// is incomplete or malformed.
pub enum RepairError {
    AllocationTooLarge { slot_count: usize, shard_size: usize },
    ZeroOriginalCount,
    InvalidShardSize { shard_size: usize },
    NullHandle { target: &'static str },
    UnknownHandle { target: &'static str, handle: u64 },
    AllocationTypeMismatch { target: &'static str, handle: u64 },
    ArenaShardSizeMismatch { expected: usize, got: usize },
    InvalidSlotLayout { original_count: usize, slot_count: usize },
    UnsupportedShardCount { original_count: usize, recovery_count: usize },
    MissingIndexOutOfRange { index: usize, slot_count: usize },
    DuplicateMissingIndex { index: usize },
    NotEnoughAvailableShards { original_count: usize, available_count: usize },
    MissingOriginalShard { index: usize },
    MissingRecoveryShard { index: usize },
    Codec(String),
}

impl Display for RepairError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocationTooLarge {
                slot_count,
                shard_size,
            } => write!(
                f,
                "requested arena is too large for allocation: slot_count={}, shard_size={}",
                slot_count, shard_size
            ),
            Self::ZeroOriginalCount => f.write_str("original_count must be greater than zero"),
            Self::InvalidShardSize { shard_size } => write!(
                f,
                "shard_size must be non-zero and even, got {}",
                shard_size
            ),
            Self::NullHandle { target } => write!(f, "null handle for {}", target),
            Self::UnknownHandle { target, handle } => {
                write!(f, "unknown {} handle: {}", target, handle)
            }
            Self::AllocationTypeMismatch { target, handle } => write!(
                f,
                "handle {} is not registered as the requested {} allocation",
                handle, target
            ),
            Self::ArenaShardSizeMismatch { expected, got } => write!(
                f,
                "registered shard arena expects shard_size {}, got {}",
                expected, got
            ),
            Self::InvalidSlotLayout {
                original_count,
                slot_count,
            } => write!(
                f,
                "slot layout is invalid: original_count={} exceeds slot_count={}",
                original_count, slot_count
            ),
            Self::UnsupportedShardCount {
                original_count,
                recovery_count,
            } => write!(
                f,
                "reed-solomon configuration is unsupported: original_count={}, recovery_count={}",
                original_count, recovery_count
            ),
            Self::MissingIndexOutOfRange { index, slot_count } => write!(
                f,
                "missing index {} is outside slot_count {}",
                index, slot_count
            ),
            Self::DuplicateMissingIndex { index } => {
                write!(f, "duplicate missing index {}", index)
            }
            Self::NotEnoughAvailableShards {
                original_count,
                available_count,
            } => write!(
                f,
                "not enough shards to repair: original_count={}, available_count={}",
                original_count, available_count
            ),
            Self::MissingOriginalShard { index } => {
                write!(f, "decoder did not restore original shard {}", index)
            }
            Self::MissingRecoveryShard { index } => {
                write!(f, "encoder did not regenerate recovery shard {}", index)
            }
            Self::Codec(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RepairError {}

impl From<reed_solomon_simd::Error> for RepairError {
    fn from(error: reed_solomon_simd::Error) -> Self {
        Self::Codec(error.to_string())
    }
}

#[derive(Debug, Clone, Copy)]
struct RegistryEntry {
    ptr: usize,
    allocation: Allocation,
}

#[derive(Debug, Clone)]
struct RegistrySlot {
    generation: u32,
    entry: Option<RegistryEntry>,
}

#[derive(Debug, Default)]
struct AllocationRegistry {
    slots: Vec<RegistrySlot>,
    reusable_slot_ids: Vec<u32>,
}

impl AllocationRegistry {
    fn next_generation(current: u32) -> u32 {
        let next = current.wrapping_add(1);
        if next == 0 { 1 } else { next }
    }

    fn register(&mut self, ptr: usize, allocation: Allocation) -> AllocationHandle {
        if let Some(reused_slot_id) = self.reusable_slot_ids.pop() {
            let slot = &mut self.slots[(reused_slot_id - 1) as usize];
            slot.generation = Self::next_generation(slot.generation);
            slot.entry = Some(RegistryEntry { ptr, allocation });
            return AllocationHandle::new(reused_slot_id, slot.generation);
        }

        let slot_id = (self.slots.len() + 1) as u32;
        self.slots.push(RegistrySlot {
            generation: 1,
            entry: Some(RegistryEntry { ptr, allocation }),
        });
        AllocationHandle::new(slot_id, 1)
    }

    fn lookup(&self, handle: AllocationHandle, target: &'static str) -> Result<RegistryEntry, RepairError> {
        if handle.is_null() {
            return Err(RepairError::NullHandle { target });
        }

        let Some(slot_index) = handle.slot_index() else {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        };

        let Some(slot) = self.slots.get(slot_index) else {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        };

        if slot.generation != handle.generation() {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        }

        slot.entry.ok_or(RepairError::UnknownHandle {
            target,
            handle: handle.into_raw(),
        })
    }

    fn remove(&mut self, handle: AllocationHandle, target: &'static str) -> Result<RegistryEntry, RepairError> {
        if handle.is_null() {
            return Err(RepairError::NullHandle { target });
        }

        let Some(slot_index) = handle.slot_index() else {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        };

        let Some(slot) = self.slots.get_mut(slot_index) else {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        };

        if slot.generation != handle.generation() {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        }

        let Some(entry) = slot.entry.take() else {
            return Err(RepairError::UnknownHandle {
                target,
                handle: handle.into_raw(),
            });
        };

        self.reusable_slot_ids.push((slot_index + 1) as u32);
        Ok(entry)
    }
}

type Registry = Mutex<AllocationRegistry>;

fn allocation_registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(AllocationRegistry::default()))
}

fn register_allocation(ptr: usize, allocation: Allocation) -> AllocationHandle {
    allocation_registry()
        .lock()
        .expect("allocation registry poisoned")
        .register(ptr, allocation)
}

fn lookup_allocation(handle: AllocationHandle, target: &'static str) -> Result<RegistryEntry, RepairError> {
    allocation_registry()
        .lock()
        .expect("allocation registry poisoned")
        .lookup(handle, target)
}

fn remove_allocation(handle: AllocationHandle, target: &'static str) -> Result<RegistryEntry, RepairError> {
    allocation_registry()
        .lock()
        .expect("allocation registry poisoned")
        .remove(handle, target)
}

fn alloc_shard_arena_inner(slot_count: usize, shard_size: usize) -> Result<AllocationHandle, RepairError> {
    if shard_size == 0 || shard_size % 2 != 0 {
        return Err(RepairError::InvalidShardSize { shard_size });
    }

    let byte_len = slot_count
        .checked_mul(shard_size)
        .ok_or(RepairError::AllocationTooLarge {
            slot_count,
            shard_size,
        })?;

    let mut arena = vec![0_u8; byte_len];
    let ptr = arena.as_mut_ptr();

    let handle = register_allocation(
        ptr as usize,
        Allocation::ShardArena(ShardArenaMeta {
            byte_len,
            slot_count,
            shard_size,
        }),
    );

    std::mem::forget(arena);

    Ok(handle)
}

fn free_shard_arena_inner(handle: AllocationHandle) -> Result<(), RepairError> {
    match remove_allocation(handle, "shard arena")? {
        RegistryEntry {
            ptr,
            allocation: Allocation::ShardArena(meta),
        } => {
            unsafe {
                drop(Vec::from_raw_parts(ptr as *mut u8, meta.byte_len, meta.byte_len));
            }
            Ok(())
        }
        RegistryEntry { .. } => Err(RepairError::AllocationTypeMismatch {
            target: "shard arena",
            handle: handle.into_raw(),
        }),
    }
}

fn alloc_u32_buffer_inner(len: usize) -> AllocationHandle {
    if len == 0 {
        return AllocationHandle::null();
    }

    let mut buffer = vec![0_u32; len];
    let ptr = buffer.as_mut_ptr();
    let handle = register_allocation(ptr as usize, Allocation::U32Buffer(U32BufferMeta { len }));
    std::mem::forget(buffer);
    handle
}

fn free_u32_buffer_inner(handle: AllocationHandle) -> Result<(), RepairError> {
    if handle.is_null() {
        return Ok(());
    }

    match remove_allocation(handle, "u32 buffer")? {
        RegistryEntry {
            ptr,
            allocation: Allocation::U32Buffer(meta),
        } => {
            unsafe {
                drop(Vec::from_raw_parts(ptr as *mut u32, meta.len, meta.len));
            }
            Ok(())
        }
        RegistryEntry { .. } => Err(RepairError::AllocationTypeMismatch {
            target: "u32 buffer",
            handle: handle.into_raw(),
        }),
    }
}

fn with_registered_arena_mut<T>(
    handle: AllocationHandle,
    f: impl FnOnce(ShardArenaMeta, &mut [u8]) -> Result<T, RepairError>,
) -> Result<T, RepairError> {
    let entry = lookup_allocation(handle, "shard arena")?;
    let meta = match entry.allocation {
        Allocation::ShardArena(meta) => meta,
        _ => {
            return Err(RepairError::AllocationTypeMismatch {
                target: "shard arena",
                handle: handle.into_raw(),
            })
        }
    };

    let bytes = unsafe { std::slice::from_raw_parts_mut(entry.ptr as *mut u8, meta.byte_len) };
    f(meta, bytes)
}

fn with_registered_u32_slice<T>(
    handle: AllocationHandle,
    len: usize,
    f: impl FnOnce(&[u32]) -> Result<T, RepairError>,
) -> Result<T, RepairError> {
    if len == 0 {
        return f(&[]);
    }

    let entry = lookup_allocation(handle, "u32 buffer")?;
    let meta = match entry.allocation {
        Allocation::U32Buffer(meta) => meta,
        _ => {
            return Err(RepairError::AllocationTypeMismatch {
                target: "u32 buffer",
                handle: handle.into_raw(),
            })
        }
    };

    if len > meta.len {
        return Err(RepairError::Codec(format!(
            "missing index buffer length {} exceeds registered length {}",
            len, meta.len
        )));
    }

    let slice = unsafe { std::slice::from_raw_parts(entry.ptr as *const u32, len) };
    f(slice)
}

fn shard_arena_ptr_inner(handle: AllocationHandle) -> Result<*mut u8, RepairError> {
    let entry = lookup_allocation(handle, "shard arena")?;
    match entry.allocation {
        Allocation::ShardArena(_) => Ok(entry.ptr as *mut u8),
        _ => Err(RepairError::AllocationTypeMismatch {
            target: "shard arena",
            handle: handle.into_raw(),
        }),
    }
}

fn u32_buffer_ptr_inner(handle: AllocationHandle) -> Result<*mut u32, RepairError> {
    let entry = lookup_allocation(handle, "u32 buffer")?;
    match entry.allocation {
        Allocation::U32Buffer(_) => Ok(entry.ptr as *mut u32),
        _ => Err(RepairError::AllocationTypeMismatch {
            target: "u32 buffer",
            handle: handle.into_raw(),
        }),
    }
}

fn shard_slice(bytes: &[u8], shard_size: usize, slot_index: usize) -> &[u8] {
    let start = slot_index * shard_size;
    &bytes[start..start + shard_size]
}

fn shard_slice_mut(bytes: &mut [u8], shard_size: usize, slot_index: usize) -> &mut [u8] {
    let start = slot_index * shard_size;
    &mut bytes[start..start + shard_size]
}

fn validate_missing_indices(
    missing_indices: &[u32],
    slot_count: usize,
) -> Result<BTreeSet<usize>, RepairError> {
    let mut missing_set = BTreeSet::new();

    for &raw_index in missing_indices {
        let index = raw_index as usize;
        if index >= slot_count {
            return Err(RepairError::MissingIndexOutOfRange { index, slot_count });
        }
        if !missing_set.insert(index) {
            return Err(RepairError::DuplicateMissingIndex { index });
        }
    }

    Ok(missing_set)
}

fn repair_in_place(
    original_count: usize,
    shard_size: usize,
    missing_indices: &[u32],
    slot_count: usize,
    bytes: &mut [u8],
) -> Result<usize, RepairError> {
    if original_count == 0 {
        return Err(RepairError::ZeroOriginalCount);
    }

    if shard_size == 0 || shard_size % 2 != 0 {
        return Err(RepairError::InvalidShardSize { shard_size });
    }

    if slot_count < original_count {
        return Err(RepairError::InvalidSlotLayout {
            original_count,
            slot_count,
        });
    }

    let recovery_count = slot_count - original_count;
    if !ReedSolomonEncoder::supports(original_count, recovery_count)
        || !ReedSolomonDecoder::supports(original_count, recovery_count)
    {
        return Err(RepairError::UnsupportedShardCount {
            original_count,
            recovery_count,
        });
    }

    let missing_set = validate_missing_indices(missing_indices, slot_count)?;
    if missing_set.is_empty() {
        return Ok(0);
    }

    let available_count = slot_count - missing_set.len();
    if available_count < original_count {
        return Err(RepairError::NotEnoughAvailableShards {
            original_count,
            available_count,
        });
    }

    let missing_original_indices: Vec<usize> = missing_set
        .iter()
        .copied()
        .filter(|index| *index < original_count)
        .collect();

    if !missing_original_indices.is_empty() {
        let mut decoder = ReedSolomonDecoder::new(original_count, recovery_count, shard_size)?;

        for slot_index in 0..original_count {
            if missing_set.contains(&slot_index) {
                continue;
            }
            decoder.add_original_shard(slot_index, shard_slice(bytes, shard_size, slot_index))?;
        }

        for recovery_index in 0..recovery_count {
            let slot_index = original_count + recovery_index;
            if missing_set.contains(&slot_index) {
                continue;
            }
            decoder.add_recovery_shard(
                recovery_index,
                shard_slice(bytes, shard_size, slot_index),
            )?;
        }

        let restored_originals = {
            let result = decoder.decode()?;
            let mut restored = Vec::with_capacity(missing_original_indices.len());

            for &index in &missing_original_indices {
                let shard = result
                    .restored_original(index)
                    .ok_or(RepairError::MissingOriginalShard { index })?;
                restored.push((index, shard.to_vec()));
            }

            restored
        };

        for (index, shard) in restored_originals {
            shard_slice_mut(bytes, shard_size, index).copy_from_slice(&shard);
        }
    }

    let missing_recovery_indices: Vec<usize> = missing_set
        .iter()
        .copied()
        .filter(|index| *index >= original_count)
        .map(|index| index - original_count)
        .collect();

    if !missing_recovery_indices.is_empty() {
        let mut encoder = ReedSolomonEncoder::new(original_count, recovery_count, shard_size)?;

        for slot_index in 0..original_count {
            encoder.add_original_shard(shard_slice(bytes, shard_size, slot_index))?;
        }

        let regenerated_recovery = {
            let result = encoder.encode()?;
            let mut regenerated = Vec::with_capacity(missing_recovery_indices.len());

            for &index in &missing_recovery_indices {
                let shard = result
                    .recovery(index)
                    .ok_or(RepairError::MissingRecoveryShard { index })?;
                regenerated.push((index, shard.to_vec()));
            }

            regenerated
        };

        for (recovery_index, shard) in regenerated_recovery {
            let slot_index = original_count + recovery_index;
            shard_slice_mut(bytes, shard_size, slot_index).copy_from_slice(&shard);
        }
    }

    Ok(missing_set.len())
}

fn encode_in_place(
    original_count: usize,
    shard_size: usize,
    slot_count: usize,
    bytes: &mut [u8],
) -> Result<usize, RepairError> {
    if original_count == 0 {
        return Err(RepairError::ZeroOriginalCount);
    }

    if shard_size == 0 || shard_size % 2 != 0 {
        return Err(RepairError::InvalidShardSize { shard_size });
    }

    if slot_count < original_count {
        return Err(RepairError::InvalidSlotLayout {
            original_count,
            slot_count,
        });
    }

    let recovery_count = slot_count - original_count;
    if !ReedSolomonEncoder::supports(original_count, recovery_count) {
        return Err(RepairError::UnsupportedShardCount {
            original_count,
            recovery_count,
        });
    }

    let mut encoder = ReedSolomonEncoder::new(original_count, recovery_count, shard_size)?;

    for slot_index in 0..original_count {
        encoder.add_original_shard(shard_slice(bytes, shard_size, slot_index))?;
    }

    let generated_recovery = {
        let result = encoder.encode()?;
        let mut generated = Vec::with_capacity(recovery_count);

        for index in 0..recovery_count {
            let shard = result
                .recovery(index)
                .ok_or(RepairError::MissingRecoveryShard { index })?;
            generated.push((index, shard.to_vec()));
        }

        generated
    };

    for (recovery_index, shard) in generated_recovery {
        let slot_index = original_count + recovery_index;
        shard_slice_mut(bytes, shard_size, slot_index).copy_from_slice(&shard);
    }

    Ok(recovery_count)
}

pub fn alloc_shard_arena(slot_count: usize, shard_size: usize) -> Result<AllocationHandle, RepairError> {
    alloc_shard_arena_inner(slot_count, shard_size)
}

/// Release a shard arena previously created by [`alloc_shard_arena`].
pub fn free_shard_arena(handle: AllocationHandle) -> Result<(), RepairError> {
    free_shard_arena_inner(handle)
}

/// Resolve a live shard arena handle to its current linear-memory pointer.
pub fn shard_arena_ptr(handle: AllocationHandle) -> Result<*mut u8, RepairError> {
    shard_arena_ptr_inner(handle)
}

/// Allocate a `u32` buffer inside linear memory for missing-index lists.
///
/// A zero-length request returns a null pointer so callers can naturally model the "no missing
/// indices" case without paying for an unnecessary allocation.
pub fn alloc_u32_buffer(len: usize) -> AllocationHandle {
    alloc_u32_buffer_inner(len)
}

/// Release a buffer previously created by [`alloc_u32_buffer`].
pub fn free_u32_buffer(handle: AllocationHandle) -> Result<(), RepairError> {
    free_u32_buffer_inner(handle)
}

/// Resolve a live `u32` buffer handle to its current linear-memory pointer.
pub fn u32_buffer_ptr(handle: AllocationHandle) -> Result<*mut u32, RepairError> {
    u32_buffer_ptr_inner(handle)
}

/// Repair the missing shard slots recorded in `missing_indices`.
///
/// The caller is responsible for populating every present shard slot in the registered arena. Any
/// slot not listed in `missing_indices` is treated as authoritative input. The return value is the
/// number of repaired slots.
pub fn leopard_repair(
    original_count: usize,
    shard_size: usize,
    missing_indices: &[u32],
    shard_handle: AllocationHandle,
) -> Result<usize, RepairError> {
    with_registered_arena_mut(shard_handle, |meta, bytes| {
        if meta.shard_size != shard_size {
            return Err(RepairError::ArenaShardSizeMismatch {
                expected: meta.shard_size,
                got: shard_size,
            });
        }

        repair_in_place(
            original_count,
            shard_size,
            missing_indices,
            meta.slot_count,
            bytes,
        )
    })
}

/// Encode recovery shards in place after the original slots have been populated.
///
/// Recovery bytes are written into the trailing `slot_count - original_count` slots of the arena.
/// The return value is the number of generated recovery shards.
pub fn leopard_encode(
    original_count: usize,
    shard_size: usize,
    shard_handle: AllocationHandle,
) -> Result<usize, RepairError> {
    with_registered_arena_mut(shard_handle, |meta, bytes| {
        if meta.shard_size != shard_size {
            return Err(RepairError::ArenaShardSizeMismatch {
                expected: meta.shard_size,
                got: shard_size,
            });
        }

        encode_in_place(original_count, shard_size, meta.slot_count, bytes)
    })
}

#[wasm_bindgen(js_name = alloc_shard_arena)]
/// JavaScript/Wasm binding for [`alloc_shard_arena`].
pub fn alloc_shard_arena_js(slot_count: usize, shard_size: usize) -> Result<u64, JsValue> {
    alloc_shard_arena(slot_count, shard_size)
        .map(AllocationHandle::into_raw)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = free_shard_arena)]
/// JavaScript/Wasm binding for [`free_shard_arena`].
pub fn free_shard_arena_js(handle: u64) -> Result<(), JsValue> {
    free_shard_arena(AllocationHandle::from_raw(handle))
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = shard_arena_ptr)]
/// JavaScript/Wasm binding for [`shard_arena_ptr`].
pub fn shard_arena_ptr_js(handle: u64) -> Result<usize, JsValue> {
    shard_arena_ptr(AllocationHandle::from_raw(handle))
        .map(|ptr| ptr as usize)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = alloc_u32_buffer)]
/// JavaScript/Wasm binding for [`alloc_u32_buffer`].
pub fn alloc_u32_buffer_js(len: usize) -> u64 {
    alloc_u32_buffer(len).into_raw()
}

#[wasm_bindgen(js_name = free_u32_buffer)]
/// JavaScript/Wasm binding for [`free_u32_buffer`].
pub fn free_u32_buffer_js(handle: u64) -> Result<(), JsValue> {
    free_u32_buffer(AllocationHandle::from_raw(handle))
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = u32_buffer_ptr)]
/// JavaScript/Wasm binding for [`u32_buffer_ptr`].
pub fn u32_buffer_ptr_js(handle: u64) -> Result<usize, JsValue> {
    u32_buffer_ptr(AllocationHandle::from_raw(handle))
        .map(|ptr| ptr as usize)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = leopard_repair)]
/// JavaScript/Wasm binding for [`leopard_repair`].
pub fn leopard_repair_js(
    original_count: usize,
    shard_size: usize,
    missing_indices_handle: u64,
    missing_indices_len: usize,
    shard_handle: u64,
) -> Result<usize, JsValue> {
    with_registered_u32_slice(
        AllocationHandle::from_raw(missing_indices_handle),
        missing_indices_len,
        |missing_indices| {
            leopard_repair(
                original_count,
                shard_size,
                missing_indices,
                AllocationHandle::from_raw(shard_handle),
            )
        },
    )
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = leopard_encode)]
/// JavaScript/Wasm binding for [`leopard_encode`].
pub fn leopard_encode_js(
    original_count: usize,
    shard_size: usize,
    shard_handle: u64,
) -> Result<usize, JsValue> {
    leopard_encode(original_count, shard_size, AllocationHandle::from_raw(shard_handle))
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    struct EncodeCase {
        original_count: usize,
        recovery_count: usize,
        shard_size: usize,
        original_shards: Vec<Vec<u8>>,
    }

    impl EncodeCase {
        fn slot_count(&self) -> usize {
            self.original_count + self.recovery_count
        }

        fn byte_len(&self) -> usize {
            self.slot_count() * self.shard_size
        }
    }

    #[derive(Debug, Clone)]
    struct RepairCase {
        original_count: usize,
        recovery_count: usize,
        shard_size: usize,
        original_shards: Vec<Vec<u8>>,
        missing_indices: Vec<usize>,
    }

    struct OwnedShardArena {
        handle: AllocationHandle,
        ptr: *mut u8,
    }

    impl OwnedShardArena {
        fn new(slot_count: usize, shard_size: usize) -> Self {
            let handle = alloc_shard_arena(slot_count, shard_size).expect("arena allocation failed");
            Self {
                ptr: shard_arena_ptr(handle).expect("arena pointer lookup failed"),
                handle,
            }
        }

        fn bytes_mut(&mut self, byte_len: usize) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, byte_len) }
        }
    }

    impl Drop for OwnedShardArena {
        fn drop(&mut self) {
            if !self.handle.is_null() {
                free_shard_arena(self.handle).expect("arena free failed");
            }
        }
    }

    struct OwnedU32Buffer {
        handle: AllocationHandle,
        ptr: *mut u32,
        len: usize,
    }

    impl OwnedU32Buffer {
        fn new(values: &[u32]) -> Self {
            let handle = alloc_u32_buffer(values.len());
            let ptr = if handle.is_null() {
                std::ptr::null_mut()
            } else {
                u32_buffer_ptr(handle).expect("u32 buffer pointer lookup failed")
            };
            if !ptr.is_null() {
                unsafe {
                    std::slice::from_raw_parts_mut(ptr, values.len()).copy_from_slice(values);
                }
            }
            Self {
                handle,
                ptr,
                len: values.len(),
            }
        }
    }

    impl Drop for OwnedU32Buffer {
        fn drop(&mut self) {
            if self.len > 0 {
                free_u32_buffer(self.handle).expect("u32 buffer free failed");
            }
        }
    }

    fn write_slots(arena: &mut OwnedShardArena, shard_size: usize, slots: &[Vec<u8>]) {
        let bytes = arena.bytes_mut(slots.len() * shard_size);

        for (slot_index, shard) in slots.iter().enumerate() {
            let start = slot_index * shard_size;
            bytes[start..start + shard_size].copy_from_slice(shard);
        }
    }

    fn read_slots(arena: &mut OwnedShardArena, slot_count: usize, shard_size: usize) -> Vec<Vec<u8>> {
        let bytes = arena.bytes_mut(slot_count * shard_size);

        (0..slot_count)
            .map(|slot_index| {
                let start = slot_index * shard_size;
                bytes[start..start + shard_size].to_vec()
            })
            .collect()
    }

    fn encode_reference_recovery(
        original_shards: &[Vec<u8>],
        recovery_count: usize,
    ) -> Vec<Vec<u8>> {
        let original_count = original_shards.len();
        let original_refs: Vec<&[u8]> = original_shards.iter().map(Vec::as_slice).collect();
        reed_solomon_simd::encode(original_count, recovery_count, original_refs)
            .expect("reference recovery encoding should succeed")
    }

    fn reference_slots(original_shards: &[Vec<u8>], recovery_shards: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut slots = Vec::with_capacity(original_shards.len() + recovery_shards.len());
        slots.extend(original_shards.iter().cloned());
        slots.extend(recovery_shards.iter().cloned());
        slots
    }

    fn run_encode_case(case: &EncodeCase) -> Vec<Vec<u8>> {
        let slot_count = case.slot_count();
        let mut arena = OwnedShardArena::new(slot_count, case.shard_size);

        {
            let bytes = arena.bytes_mut(case.byte_len());

            for (slot_index, shard) in case.original_shards.iter().enumerate() {
                let start = slot_index * case.shard_size;
                bytes[start..start + case.shard_size].copy_from_slice(shard);
            }
        }

        let encoded_count =
            leopard_encode(case.original_count, case.shard_size, arena.handle).expect("encoding should succeed");

        assert_eq!(encoded_count, case.recovery_count);

        read_slots(&mut arena, slot_count, case.shard_size)
    }

    fn run_repair_case(case: &RepairCase) -> Vec<Vec<u8>> {
        let recovery_shards = encode_reference_recovery(&case.original_shards, case.recovery_count);
        let reference = reference_slots(&case.original_shards, &recovery_shards);
        let slot_count = case.original_count + case.recovery_count;
        let byte_len = slot_count * case.shard_size;
        let missing_u32: Vec<u32> = case
            .missing_indices
            .iter()
            .map(|index| *index as u32)
            .collect();

        let mut arena = OwnedShardArena::new(slot_count, case.shard_size);
        let missing = OwnedU32Buffer::new(&missing_u32);

        {
            let bytes = arena.bytes_mut(byte_len);

            for (slot_index, shard) in reference.iter().enumerate() {
                if case.missing_indices.contains(&slot_index) {
                    continue;
                }

                let start = slot_index * case.shard_size;
                bytes[start..start + case.shard_size].copy_from_slice(shard);
            }
        }

        let repaired_count = leopard_repair(
            case.original_count,
            case.shard_size,
            unsafe { std::slice::from_raw_parts(missing.ptr, missing.len) },
            arena.handle,
        )
        .expect("repair should succeed");

        assert_eq!(repaired_count, case.missing_indices.len());

        read_slots(&mut arena, slot_count, case.shard_size)
    }

    #[test]
    fn repairs_original_and_recovery_shards_in_place() {
        let shard_size = 64;
        let case = RepairCase {
            original_count: 4,
            recovery_count: 3,
            shard_size,
            original_shards: (0..4)
                .map(|shard_index| {
                    (0..shard_size)
                        .map(|byte_index| ((shard_index * 31 + byte_index) % 251) as u8)
                        .collect::<Vec<u8>>()
                })
                .collect(),
            missing_indices: vec![1, 6],
        };

        let repaired_slots = run_repair_case(&case);
        let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
        let reference = reference_slots(&case.original_shards, &recovery);

        for &index in &case.missing_indices {
            assert_eq!(repaired_slots[index], reference[index]);
        }
    }

    #[test]
    fn encodes_recovery_shards_in_place() {
        let original_count = 3;
        let recovery_count = 2;
        let shard_size = 48;
        let original_shards = (0..original_count)
            .map(|shard_index| {
                (0..shard_size)
                    .map(|byte_index| ((shard_index * 19 + byte_index * 7) % 251) as u8)
                    .collect::<Vec<u8>>()
            })
            .collect::<Vec<_>>();
        let expected_recovery = encode_reference_recovery(&original_shards, recovery_count);
        let mut arena = OwnedShardArena::new(original_count + recovery_count, shard_size);

        {
            let bytes = arena.bytes_mut((original_count + recovery_count) * shard_size);

            for (slot_index, shard) in original_shards.iter().enumerate() {
                let start = slot_index * shard_size;
                bytes[start..start + shard_size].copy_from_slice(shard);
            }
        }

        let encoded_count = leopard_encode(original_count, shard_size, arena.handle)
            .expect("encoding should succeed");

        assert_eq!(encoded_count, recovery_count);

        let bytes = arena.bytes_mut((original_count + recovery_count) * shard_size);
        for (recovery_index, shard) in expected_recovery.iter().enumerate() {
            let start = (original_count + recovery_index) * shard_size;
            assert_eq!(&bytes[start..start + shard_size], shard.as_slice());
        }
    }

    #[test]
    fn rejects_odd_shard_sizes() {
        let error = alloc_shard_arena(4, 63).expect_err("odd shard sizes should fail");
        assert_eq!(
            error,
            RepairError::InvalidShardSize {
                shard_size: 63
            }
        );
    }

    #[test]
    fn rejects_insufficient_available_shards() {
        let shard_size = 32;
        let arena = OwnedShardArena::new(5, shard_size);
        let missing = OwnedU32Buffer::new(&[0, 1, 2]);

        let error = leopard_repair(
            4,
            shard_size,
            unsafe { std::slice::from_raw_parts(missing.ptr, missing.len) },
            arena.handle,
        )
        .expect_err("repair should fail when too many shards are missing");

        assert_eq!(
            error,
            RepairError::NotEnoughAvailableShards {
                original_count: 4,
                available_count: 2,
            }
        );
    }

    #[test]
    fn rejects_stale_handles_when_pointer_addresses_are_reused() {
        let arena_meta = ShardArenaMeta {
            byte_len: 64,
            slot_count: 2,
            shard_size: 32,
        };
        let mut registry = AllocationRegistry::default();
        let first = registry.register(0x1000, Allocation::ShardArena(arena_meta));

        let removed = registry
            .remove(first, "shard arena")
            .expect("first handle should remove cleanly");
        assert_eq!(removed.ptr, 0x1000);

        let second = registry.register(0x1000, Allocation::ShardArena(arena_meta));
        assert_ne!(first.into_raw(), second.into_raw());

        let error = registry
            .lookup(first, "shard arena")
            .expect_err("stale handle should fail after slot reuse");
        assert_eq!(
            error,
            RepairError::UnknownHandle {
                target: "shard arena",
                handle: first.into_raw(),
            }
        );

        let entry = registry
            .lookup(second, "shard arena")
            .expect("fresh handle should resolve");
        assert_eq!(entry.ptr, 0x1000);
    }

    prop_compose! {
        fn encode_case_strategy()
            (
                original_count in 1_usize..10,
                recovery_count in 1_usize..10,
                shard_words in 1_usize..32,
            )
            (
                original_count in Just(original_count),
                recovery_count in Just(recovery_count),
                shard_size in Just(shard_words * 2),
                shard_bytes in prop::collection::vec(any::<u8>(), original_count * shard_words * 2),
            ) -> EncodeCase {
                let original_shards = shard_bytes
                    .chunks(shard_size)
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>();

                EncodeCase {
                    original_count,
                    recovery_count,
                    shard_size,
                    original_shards,
                }
            }
    }

    prop_compose! {
        fn repair_case_strategy()
            (
                original_count in 2_usize..10,
                recovery_count in 1_usize..10,
                shard_words in 1_usize..32,
            )
            (
                original_count in Just(original_count),
                recovery_count in Just(recovery_count),
                shard_size in Just(shard_words * 2),
                shard_bytes in prop::collection::vec(any::<u8>(), original_count * shard_words * 2),
                missing_indices in prop::sample::subsequence(
                    (0_usize..(original_count + recovery_count)).collect::<Vec<_>>(),
                    1..=recovery_count,
                ),
            ) -> RepairCase {
                let original_shards = shard_bytes
                    .chunks(shard_size)
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>();

                RepairCase {
                    original_count,
                    recovery_count,
                    shard_size,
                    original_shards,
                    missing_indices,
                }
            }
    }

    proptest! {
        #[test]
        fn pbt_encode_matches_reference_recovery(case in encode_case_strategy()) {
            let encoded_slots = run_encode_case(&case);
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);

            prop_assert_eq!(&encoded_slots, &reference);
        }

        #[test]
        fn pbt_repair_restores_the_entire_layout(case in repair_case_strategy()) {
            let repaired_slots = run_repair_case(&case);
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);

            prop_assert_eq!(&repaired_slots, &reference);
        }

        #[test]
        fn pbt_recovers_every_requested_slot(case in repair_case_strategy()) {
            let repaired_slots = run_repair_case(&case);
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);

            for index in case.missing_indices {
                prop_assert_eq!(&repaired_slots[index], &reference[index]);
            }
        }

        #[test]
        fn pbt_noop_repair_preserves_existing_layout(case in encode_case_strategy()) {
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);
            let mut arena = OwnedShardArena::new(case.slot_count(), case.shard_size);

            write_slots(&mut arena, case.shard_size, &reference);

            let repaired_count = leopard_repair(case.original_count, case.shard_size, &[], arena.handle)
                .expect("repair without missing indices should succeed");

            prop_assert_eq!(repaired_count, 0);
            prop_assert_eq!(read_slots(&mut arena, case.slot_count(), case.shard_size), reference);
        }

        #[test]
        fn pbt_rejects_duplicate_missing_indices(case in encode_case_strategy(), duplicate_seed in any::<u16>()) {
            let duplicate_index = usize::from(duplicate_seed) % case.slot_count();
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);
            let mut arena = OwnedShardArena::new(case.slot_count(), case.shard_size);
            let missing = OwnedU32Buffer::new(&[duplicate_index as u32, duplicate_index as u32]);

            write_slots(&mut arena, case.shard_size, &reference);

            let error = leopard_repair(
                case.original_count,
                case.shard_size,
                unsafe { std::slice::from_raw_parts(missing.ptr, missing.len) },
                arena.handle,
            )
            .expect_err("duplicate missing indices should fail");

            prop_assert_eq!(error, RepairError::DuplicateMissingIndex { index: duplicate_index });
        }

        #[test]
        fn pbt_rejects_out_of_range_missing_indices(case in encode_case_strategy()) {
            let recovery = encode_reference_recovery(&case.original_shards, case.recovery_count);
            let reference = reference_slots(&case.original_shards, &recovery);
            let mut arena = OwnedShardArena::new(case.slot_count(), case.shard_size);
            let missing = OwnedU32Buffer::new(&[case.slot_count() as u32]);

            write_slots(&mut arena, case.shard_size, &reference);

            let error = leopard_repair(
                case.original_count,
                case.shard_size,
                unsafe { std::slice::from_raw_parts(missing.ptr, missing.len) },
                arena.handle,
            )
            .expect_err("out-of-range missing indices should fail");

            prop_assert_eq!(
                error,
                RepairError::MissingIndexOutOfRange {
                    index: case.slot_count(),
                    slot_count: case.slot_count(),
                }
            );
        }
    }
}
