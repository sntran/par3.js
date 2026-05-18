//! WebAssembly-facing PAR repair and encoding primitives.
//!
//! The Rust core owns two low-level responsibilities:
//! 1. managing typed buffers inside linear memory so JavaScript can stream bytes in place,
//! 2. delegating encoding and repair work to `reed-solomon-simd` without copying more than necessary.
//!
//! The exported API deliberately works in terms of registered raw pointers rather than rich Rust
//! types because the Cloudflare Worker and the local CLI both need to fill shard arenas directly
//! from Web Streams or filesystem reads. Tests exercise the exact same pointer-based entrypoints so
//! that the native suite stays close to production behavior.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{BTreeSet, HashMap};
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
    NullPointer { target: &'static str },
    UnknownPointer { target: &'static str, ptr: usize },
    AllocationTypeMismatch { target: &'static str, ptr: usize },
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
            Self::NullPointer { target } => write!(f, "null pointer for {}", target),
            Self::UnknownPointer { target, ptr } => {
                write!(f, "unknown {} pointer: {}", target, ptr)
            }
            Self::AllocationTypeMismatch { target, ptr } => write!(
                f,
                "pointer {} is not registered as the requested {} allocation",
                ptr, target
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

type Registry = Mutex<HashMap<usize, Allocation>>;

fn allocation_registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_allocation(ptr: usize, allocation: Allocation) {
    allocation_registry()
        .lock()
        .expect("allocation registry poisoned")
        .insert(ptr, allocation);
}

fn remove_allocation(ptr: usize) -> Option<Allocation> {
    allocation_registry()
        .lock()
        .expect("allocation registry poisoned")
        .remove(&ptr)
}

fn alloc_shard_arena_inner(slot_count: usize, shard_size: usize) -> Result<*mut u8, RepairError> {
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

    register_allocation(
        ptr as usize,
        Allocation::ShardArena(ShardArenaMeta {
            byte_len,
            slot_count,
            shard_size,
        }),
    );

    std::mem::forget(arena);

    Ok(ptr)
}

fn free_shard_arena_inner(ptr: *mut u8) -> Result<(), RepairError> {
    if ptr.is_null() {
        return Err(RepairError::NullPointer {
            target: "shard arena",
        });
    }

    match remove_allocation(ptr as usize) {
        Some(Allocation::ShardArena(meta)) => {
            unsafe {
                drop(Vec::from_raw_parts(ptr, meta.byte_len, meta.byte_len));
            }
            Ok(())
        }
        Some(_) => Err(RepairError::AllocationTypeMismatch {
            target: "shard arena",
            ptr: ptr as usize,
        }),
        None => Err(RepairError::UnknownPointer {
            target: "shard arena",
            ptr: ptr as usize,
        }),
    }
}

fn alloc_u32_buffer_inner(len: usize) -> *mut u32 {
    if len == 0 {
        return std::ptr::null_mut();
    }

    let mut buffer = vec![0_u32; len];
    let ptr = buffer.as_mut_ptr();
    register_allocation(ptr as usize, Allocation::U32Buffer(U32BufferMeta { len }));
    std::mem::forget(buffer);
    ptr
}

fn free_u32_buffer_inner(ptr: *mut u32) -> Result<(), RepairError> {
    if ptr.is_null() {
        return Ok(());
    }

    match remove_allocation(ptr as usize) {
        Some(Allocation::U32Buffer(meta)) => {
            unsafe {
                drop(Vec::from_raw_parts(ptr, meta.len, meta.len));
            }
            Ok(())
        }
        Some(_) => Err(RepairError::AllocationTypeMismatch {
            target: "u32 buffer",
            ptr: ptr as usize,
        }),
        None => Err(RepairError::UnknownPointer {
            target: "u32 buffer",
            ptr: ptr as usize,
        }),
    }
}

fn with_registered_arena_mut<T>(
    ptr: *mut u8,
    f: impl FnOnce(ShardArenaMeta, &mut [u8]) -> Result<T, RepairError>,
) -> Result<T, RepairError> {
    if ptr.is_null() {
        return Err(RepairError::NullPointer {
            target: "shard arena",
        });
    }

    let meta = {
        let registry = allocation_registry();
        let guard = registry.lock().expect("allocation registry poisoned");
        match guard.get(&(ptr as usize)) {
            Some(Allocation::ShardArena(meta)) => *meta,
            Some(_) => {
                return Err(RepairError::AllocationTypeMismatch {
                    target: "shard arena",
                    ptr: ptr as usize,
                })
            }
            None => {
                return Err(RepairError::UnknownPointer {
                    target: "shard arena",
                    ptr: ptr as usize,
                })
            }
        }
    };

    let bytes = unsafe { std::slice::from_raw_parts_mut(ptr, meta.byte_len) };
    f(meta, bytes)
}

fn with_registered_u32_slice<T>(
    ptr: *const u32,
    len: usize,
    f: impl FnOnce(&[u32]) -> Result<T, RepairError>,
) -> Result<T, RepairError> {
    if len == 0 {
        return f(&[]);
    }

    if ptr.is_null() {
        return Err(RepairError::NullPointer {
            target: "u32 buffer",
        });
    }

    let meta = {
        let registry = allocation_registry();
        let guard = registry.lock().expect("allocation registry poisoned");
        match guard.get(&(ptr as usize)) {
            Some(Allocation::U32Buffer(meta)) => *meta,
            Some(_) => {
                return Err(RepairError::AllocationTypeMismatch {
                    target: "u32 buffer",
                    ptr: ptr as usize,
                })
            }
            None => {
                return Err(RepairError::UnknownPointer {
                    target: "u32 buffer",
                    ptr: ptr as usize,
                })
            }
        }
    };

    if len > meta.len {
        return Err(RepairError::Codec(format!(
            "missing index buffer length {} exceeds registered length {}",
            len, meta.len
        )));
    }

    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    f(slice)
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

pub fn alloc_shard_arena(slot_count: usize, shard_size: usize) -> Result<*mut u8, RepairError> {
    alloc_shard_arena_inner(slot_count, shard_size)
}

/// Release a shard arena previously created by [`alloc_shard_arena`].
pub fn free_shard_arena(ptr: *mut u8) -> Result<(), RepairError> {
    free_shard_arena_inner(ptr)
}

/// Allocate a `u32` buffer inside linear memory for missing-index lists.
///
/// A zero-length request returns a null pointer so callers can naturally model the "no missing
/// indices" case without paying for an unnecessary allocation.
pub fn alloc_u32_buffer(len: usize) -> *mut u32 {
    alloc_u32_buffer_inner(len)
}

/// Release a buffer previously created by [`alloc_u32_buffer`].
pub fn free_u32_buffer(ptr: *mut u32) -> Result<(), RepairError> {
    free_u32_buffer_inner(ptr)
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
    shard_ptr: *mut u8,
) -> Result<usize, RepairError> {
    with_registered_arena_mut(shard_ptr, |meta, bytes| {
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
    shard_ptr: *mut u8,
) -> Result<usize, RepairError> {
    with_registered_arena_mut(shard_ptr, |meta, bytes| {
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
pub fn alloc_shard_arena_js(slot_count: usize, shard_size: usize) -> Result<usize, JsValue> {
    alloc_shard_arena(slot_count, shard_size)
        .map(|ptr| ptr as usize)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = free_shard_arena)]
/// JavaScript/Wasm binding for [`free_shard_arena`].
pub fn free_shard_arena_js(ptr: usize) -> Result<(), JsValue> {
    free_shard_arena(ptr as *mut u8).map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = alloc_u32_buffer)]
/// JavaScript/Wasm binding for [`alloc_u32_buffer`].
pub fn alloc_u32_buffer_js(len: usize) -> usize {
    alloc_u32_buffer(len) as usize
}

#[wasm_bindgen(js_name = free_u32_buffer)]
/// JavaScript/Wasm binding for [`free_u32_buffer`].
pub fn free_u32_buffer_js(ptr: usize) -> Result<(), JsValue> {
    free_u32_buffer(ptr as *mut u32).map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = leopard_repair)]
/// JavaScript/Wasm binding for [`leopard_repair`].
pub fn leopard_repair_js(
    original_count: usize,
    shard_size: usize,
    missing_indices_ptr: usize,
    missing_indices_len: usize,
    shard_ptr: usize,
) -> Result<usize, JsValue> {
    with_registered_u32_slice(
        missing_indices_ptr as *const u32,
        missing_indices_len,
        |missing_indices| {
            leopard_repair(
                original_count,
                shard_size,
                missing_indices,
                shard_ptr as *mut u8,
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
    shard_ptr: usize,
) -> Result<usize, JsValue> {
    leopard_encode(original_count, shard_size, shard_ptr as *mut u8)
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
        ptr: *mut u8,
    }

    impl OwnedShardArena {
        fn new(slot_count: usize, shard_size: usize) -> Self {
            Self {
                ptr: alloc_shard_arena(slot_count, shard_size).expect("arena allocation failed"),
            }
        }

        fn bytes_mut(&mut self, byte_len: usize) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, byte_len) }
        }
    }

    impl Drop for OwnedShardArena {
        fn drop(&mut self) {
            if !self.ptr.is_null() {
                free_shard_arena(self.ptr).expect("arena free failed");
            }
        }
    }

    struct OwnedU32Buffer {
        ptr: *mut u32,
        len: usize,
    }

    impl OwnedU32Buffer {
        fn new(values: &[u32]) -> Self {
            let ptr = alloc_u32_buffer(values.len());
            if !ptr.is_null() {
                unsafe {
                    std::slice::from_raw_parts_mut(ptr, values.len()).copy_from_slice(values);
                }
            }
            Self {
                ptr,
                len: values.len(),
            }
        }
    }

    impl Drop for OwnedU32Buffer {
        fn drop(&mut self) {
            if self.len > 0 {
                free_u32_buffer(self.ptr).expect("u32 buffer free failed");
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
        let arena_ptr = arena.ptr;

        {
            let bytes = arena.bytes_mut(case.byte_len());

            for (slot_index, shard) in case.original_shards.iter().enumerate() {
                let start = slot_index * case.shard_size;
                bytes[start..start + case.shard_size].copy_from_slice(shard);
            }
        }

        let encoded_count =
            leopard_encode(case.original_count, case.shard_size, arena_ptr).expect("encoding should succeed");

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
        let arena_ptr = arena.ptr as usize;

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
            arena_ptr as *mut u8,
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
        let arena_ptr = arena.ptr;

        {
            let bytes = arena.bytes_mut((original_count + recovery_count) * shard_size);

            for (slot_index, shard) in original_shards.iter().enumerate() {
                let start = slot_index * shard_size;
                bytes[start..start + shard_size].copy_from_slice(shard);
            }
        }

        let encoded_count = leopard_encode(original_count, shard_size, arena_ptr)
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
            arena.ptr,
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

            let repaired_count = leopard_repair(case.original_count, case.shard_size, &[], arena.ptr)
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
                arena.ptr,
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
                arena.ptr,
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
