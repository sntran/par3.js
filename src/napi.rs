use napi::bindgen_prelude::{AsyncTask, Buffer, BufferSlice};
use napi::{Env, Error, Status, Task};
use napi_derive::napi;
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{DefaultRateDecoder, DefaultRateEncoder, RateDecoder, RateEncoder};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};

use super::{validate_chunk_range, validate_missing_indices, RepairError};

const CHUNK_ALIGNMENT_BYTES: usize = 64;
const ENCODE_TARGET_JOBS_PER_LOGICAL_CPU: usize = 2;
const REPAIR_TARGET_JOBS_PER_LOGICAL_CPU: usize = 3;
const MIN_ENCODE_PARALLEL_CHUNK_BYTES: usize = 16 * 1024;
const MIN_REPAIR_PARALLEL_CHUNK_BYTES: usize = 256 * 1024;
static NATIVE_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();

thread_local! {
    // Rate codecs own the DefaultEngine state for a Rayon worker. Reuse them across windows so
    // the SIMD loop does not allocate or rebuild tables while worker threads are fighting for
    // memory bandwidth.
    static ENCODE_THREAD_STATE: RefCell<Option<EncodeThreadState>> = RefCell::new(None);
    static REPAIR_THREAD_STATE: RefCell<Option<RepairThreadState>> = RefCell::new(None);
}

struct CodecLayout {
    recovery_count: usize,
    slot_count: usize,
}

struct RepairPlan {
    missing_count: usize,
    available_original_indices: Vec<usize>,
    available_recovery_indices: Vec<usize>,
    missing_original_indices: Vec<usize>,
    missing_recovery_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChunkRange {
    offset: usize,
    length: usize,
}

struct AlignedArena {
    ptr: NonNull<u8>,
    byte_length: usize,
    layout: Layout,
    released: AtomicBool,
}

pub struct EncodeTask {
    original_count: usize,
    shard_size: usize,
    shard_bytes: Buffer,
}

pub struct RepairTask {
    original_count: usize,
    shard_size: usize,
    missing_indices: Vec<u32>,
    shard_bytes: Buffer,
}

struct EncodeThreadState {
    encoder: DefaultRateEncoder<DefaultEngine>,
}

struct RepairThreadState {
    decoder: DefaultRateDecoder<DefaultEngine>,
    encoder: DefaultRateEncoder<DefaultEngine>,
}

fn to_napi_error(error: RepairError) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

impl AlignedArena {
    fn allocate(byte_length: usize) -> napi::Result<Arc<Self>> {
        if byte_length == 0 {
            return Err(Error::new(
                Status::InvalidArg,
                "aligned arena byte_length must be greater than 0".to_owned(),
            ));
        }

        // Allocate the arena in Rust, not V8. Rayon splits each shard on 64-byte boundaries; if
        // the backing store starts off-line, adjacent jobs can share a cache line at the split and
        // burn cycles invalidating each other's stores. `alloc_zeroed` also gives recovery slots a
        // deterministic blank state before JavaScript writes any bytes.
        let layout = Layout::from_size_align(byte_length, CHUNK_ALIGNMENT_BYTES).map_err(|_| {
            Error::new(
                Status::InvalidArg,
                format!("invalid aligned arena layout for {} bytes", byte_length),
            )
        })?;
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or_else(|| {
            Error::new(
                Status::GenericFailure,
                format!("failed to allocate {} aligned arena bytes", byte_length),
            )
        })?;

        Ok(Arc::new(Self {
            ptr,
            byte_length,
            layout,
            released: AtomicBool::new(false),
        }))
    }

    fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    fn release(&self, env: Option<Env>) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }

        #[cfg(not(test))]
        if let Some(env) = env {
            let _ = env.adjust_external_memory(-(self.byte_length as i64));
        }

        #[cfg(test)]
        let _ = env;
    }
}

#[cfg(not(test))]
fn finalize_aligned_arena(env: Env, arena: Arc<AlignedArena>) {
    // After BufferSlice accepts the external backing store, V8 owns the only successful physical
    // free path. JavaScript may drop references or call `free()` on the facade, but the native
    // allocation is released here so N-API cannot race a manual dealloc against a later finalizer.
    arena.release(Some(env));
}

#[cfg(not(test))]
#[napi(js_name = "allocate_aligned_arena")]
pub fn allocate_aligned_arena_js(env: Env, byte_length: u32) -> napi::Result<Buffer> {
    let arena = AlignedArena::allocate(byte_length as usize)?;

    let buffer = match unsafe {
        BufferSlice::from_external(
            &env,
            arena.ptr.as_ptr(),
            arena.byte_length,
            Arc::clone(&arena),
            finalize_aligned_arena,
        )
        .and_then(|buffer_slice| buffer_slice.into_buffer(&env))
    } {
        Ok(buffer) => buffer,
        Err(error) => {
            // The pointer was never exposed to V8, so this failure path can release synchronously.
            // Once `from_external` succeeds, deallocation moves to the finalizer above.
            arena.release(None);
            return Err(error);
        }
    };

    if !arena.is_released() {
        let _ = env.adjust_external_memory(arena.byte_length as i64);
    }

    Ok(buffer)
}

impl EncodeThreadState {
    fn new(
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<Self, RepairError> {
        Ok(Self {
            encoder: DefaultRateEncoder::new(
                original_count,
                recovery_count,
                shard_bytes,
                DefaultEngine::new(),
                None,
            )?,
        })
    }

    #[inline(always)]
    fn warm(
        &mut self,
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<(), RepairError> {
        self.encoder.reset(original_count, recovery_count, shard_bytes)?;
        Ok(())
    }

    #[inline(always)]
    fn encoder(
        &mut self,
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<&mut DefaultRateEncoder<DefaultEngine>, RepairError> {
        self.encoder.reset(original_count, recovery_count, shard_bytes)?;
        Ok(&mut self.encoder)
    }
}

impl RepairThreadState {
    fn new(
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<Self, RepairError> {
        Ok(Self {
            decoder: DefaultRateDecoder::new(
                original_count,
                recovery_count,
                shard_bytes,
                DefaultEngine::new(),
                None,
            )?,
            encoder: DefaultRateEncoder::new(
                original_count,
                recovery_count,
                shard_bytes,
                DefaultEngine::new(),
                None,
            )?,
        })
    }

    #[inline(always)]
    fn warm(
        &mut self,
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<(), RepairError> {
        self.decoder.reset(original_count, recovery_count, shard_bytes)?;
        self.encoder.reset(original_count, recovery_count, shard_bytes)?;
        Ok(())
    }

    #[inline(always)]
    fn codecs(
        &mut self,
        original_count: usize,
        recovery_count: usize,
        shard_bytes: usize,
    ) -> Result<
        (
            &mut DefaultRateDecoder<DefaultEngine>,
            &mut DefaultRateEncoder<DefaultEngine>,
        ),
        RepairError,
    > {
        self.decoder.reset(original_count, recovery_count, shard_bytes)?;
        self.encoder.reset(original_count, recovery_count, shard_bytes)?;
        Ok((&mut self.decoder, &mut self.encoder))
    }
}

fn init_encode_thread_state(original_count: usize, recovery_count: usize, shard_bytes: usize) {
    ENCODE_THREAD_STATE.with(|thread_state| {
        let mut thread_state = thread_state.borrow_mut();

        if let Some(state) = thread_state.as_mut() {
            state
                .warm(original_count, recovery_count, shard_bytes)
                .expect("validated encode thread state warmup");
        } else {
            *thread_state = Some(
                EncodeThreadState::new(original_count, recovery_count, shard_bytes)
                    .expect("validated encode thread state init"),
            );
        }
    });
}

fn with_encode_thread_state<R>(
    callback: impl FnOnce(&mut EncodeThreadState) -> Result<R, RepairError>,
) -> Result<R, RepairError> {
    ENCODE_THREAD_STATE.with(|thread_state| {
        let mut thread_state = thread_state.borrow_mut();
        callback(
            thread_state
                .as_mut()
                .expect("encode thread state initialized in try_for_each_init"),
        )
    })
}

fn init_repair_thread_state(original_count: usize, recovery_count: usize, shard_bytes: usize) {
    REPAIR_THREAD_STATE.with(|thread_state| {
        let mut thread_state = thread_state.borrow_mut();

        if let Some(state) = thread_state.as_mut() {
            state
                .warm(original_count, recovery_count, shard_bytes)
                .expect("validated repair thread state warmup");
        } else {
            *thread_state = Some(
                RepairThreadState::new(original_count, recovery_count, shard_bytes)
                    .expect("validated repair thread state init"),
            );
        }
    });
}

fn with_repair_thread_state<R>(
    callback: impl FnOnce(&mut RepairThreadState) -> Result<R, RepairError>,
) -> Result<R, RepairError> {
    REPAIR_THREAD_STATE.with(|thread_state| {
        let mut thread_state = thread_state.borrow_mut();
        callback(
            thread_state
                .as_mut()
                .expect("repair thread state initialized in try_for_each_init"),
        )
    })
}

fn validate_arena_buffer(
    original_count: usize,
    shard_size: usize,
    bytes: &[u8],
) -> Result<usize, RepairError> {
    if original_count == 0 {
        return Err(RepairError::ZeroOriginalCount);
    }

    if shard_size == 0 || shard_size % 2 != 0 {
        return Err(RepairError::InvalidShardSize { shard_size });
    }

    if bytes.len() % shard_size != 0 {
        return Err(RepairError::Codec(format!(
            "arena byte length {} is not divisible by shard_size {}",
            bytes.len(), shard_size
        )));
    }

    let slot_count = bytes.len() / shard_size;
    if slot_count < original_count {
        return Err(RepairError::InvalidSlotLayout {
            original_count,
            slot_count,
        });
    }

    Ok(slot_count)
}

fn validate_encode_layout(
    original_count: usize,
    shard_size: usize,
    bytes: &[u8],
) -> Result<CodecLayout, RepairError> {
    let slot_count = validate_arena_buffer(original_count, shard_size, bytes)?;
    let recovery_count = slot_count - original_count;

    if !ReedSolomonEncoder::supports(original_count, recovery_count) {
        return Err(RepairError::UnsupportedShardCount {
            original_count,
            recovery_count,
        });
    }

    Ok(CodecLayout {
        recovery_count,
        slot_count,
    })
}

fn validate_repair_layout(
    original_count: usize,
    shard_size: usize,
    bytes: &[u8],
) -> Result<CodecLayout, RepairError> {
    let slot_count = validate_arena_buffer(original_count, shard_size, bytes)?;
    let recovery_count = slot_count - original_count;

    if !ReedSolomonEncoder::supports(original_count, recovery_count)
        || !ReedSolomonDecoder::supports(original_count, recovery_count)
    {
        return Err(RepairError::UnsupportedShardCount {
            original_count,
            recovery_count,
        });
    }

    Ok(CodecLayout {
        recovery_count,
        slot_count,
    })
}

fn build_repair_plan(
    original_count: usize,
    slot_count: usize,
    missing_indices: &[u32],
) -> Result<RepairPlan, RepairError> {
    let missing_set = validate_missing_indices(missing_indices, slot_count)?;
    let missing_count = missing_set.len();

    if missing_count == 0 {
        return Ok(RepairPlan {
            missing_count,
            available_original_indices: (0..original_count).collect(),
            available_recovery_indices: (0..slot_count - original_count).collect(),
            missing_original_indices: Vec::new(),
            missing_recovery_indices: Vec::new(),
        });
    }

    let available_count = slot_count - missing_count;
    if available_count < original_count {
        return Err(RepairError::NotEnoughAvailableShards {
            original_count,
            available_count,
        });
    }

    let mut missing_flags = vec![false; slot_count].into_boxed_slice();
    let mut available_original_indices = Vec::with_capacity(original_count.saturating_sub(missing_count));
    let mut available_recovery_indices = Vec::with_capacity(available_count.saturating_sub(original_count));
    let mut missing_original_indices = Vec::new();
    let mut missing_recovery_indices = Vec::new();

    for index in missing_set {
        missing_flags[index] = true;
        if index < original_count {
            missing_original_indices.push(index);
        } else {
            missing_recovery_indices.push(index - original_count);
        }
    }

    for index in 0..original_count {
        if !missing_flags[index] {
            available_original_indices.push(index);
        }
    }

    for recovery_index in 0..slot_count - original_count {
        let slot_index = original_count + recovery_index;
        if !missing_flags[slot_index] {
            available_recovery_indices.push(recovery_index);
        }
    }

    Ok(RepairPlan {
        missing_count,
        available_original_indices,
        available_recovery_indices,
        missing_original_indices,
        missing_recovery_indices,
    })
}

#[inline(always)]
fn dynamic_chunk_bytes_for_cpus_with_constraints(
    shard_size: usize,
    logical_cpu_count: usize,
    target_jobs_per_logical_cpu: usize,
    min_parallel_chunk_bytes: usize,
) -> usize {
    if shard_size <= CHUNK_ALIGNMENT_BYTES {
        return shard_size;
    }

    let target_jobs = logical_cpu_count.max(1) * target_jobs_per_logical_cpu.max(1);
    let target_chunk_bytes = shard_size
        .div_ceil(target_jobs)
        .max(shard_size.min(min_parallel_chunk_bytes));
    let aligned_chunk_bytes = target_chunk_bytes - (target_chunk_bytes % CHUNK_ALIGNMENT_BYTES);

    aligned_chunk_bytes.max(CHUNK_ALIGNMENT_BYTES).min(shard_size)
}

#[inline(always)]
fn dynamic_chunk_bytes_for_cpus(shard_size: usize, logical_cpu_count: usize) -> usize {
    dynamic_chunk_bytes_for_cpus_with_constraints(
        shard_size,
        logical_cpu_count,
        ENCODE_TARGET_JOBS_PER_LOGICAL_CPU,
        MIN_ENCODE_PARALLEL_CHUNK_BYTES,
    )
}

#[inline(always)]
fn dynamic_repair_chunk_bytes_for_cpus(shard_size: usize, logical_cpu_count: usize) -> usize {
    dynamic_chunk_bytes_for_cpus_with_constraints(
        shard_size,
        logical_cpu_count,
        REPAIR_TARGET_JOBS_PER_LOGICAL_CPU,
        MIN_REPAIR_PARALLEL_CHUNK_BYTES,
    )
}

#[inline(always)]
fn dynamic_encode_chunk_bytes(shard_size: usize) -> usize {
    dynamic_chunk_bytes_for_cpus(shard_size, num_cpus::get().max(1))
}

#[inline(always)]
fn dynamic_repair_chunk_bytes(shard_size: usize) -> usize {
    positive_env_usize("PAR3_NATIVE_REPAIR_CHUNK_BYTES")
        .map(|chunk_bytes| chunk_bytes.min(shard_size))
        .unwrap_or_else(|| dynamic_repair_chunk_bytes_for_cpus(shard_size, native_thread_count()))
}

fn positive_env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn native_thread_count() -> usize {
    positive_env_usize("PAR3_NATIVE_THREADS")
    .unwrap_or_else(|| num_cpus::get_physical().max(1))
}

fn native_thread_pool() -> &'static ThreadPool {
    NATIVE_THREAD_POOL.get_or_init(|| {
        ThreadPoolBuilder::new()
            .num_threads(native_thread_count())
            .build()
            .expect("native thread pool initialization")
    })
}

fn build_chunk_ranges(shard_size: usize, chunk_bytes: usize) -> Result<Vec<ChunkRange>, RepairError> {
    if chunk_bytes == 0 || (chunk_bytes % CHUNK_ALIGNMENT_BYTES != 0 && chunk_bytes != shard_size) {
        return Err(RepairError::InvalidChunkRange {
            chunk_offset: 0,
            chunk_length: chunk_bytes,
            shard_size,
        });
    }

    let mut chunk_ranges = Vec::new();
    let mut chunk_offset = 0;
    while chunk_offset < shard_size {
        let remaining_bytes = shard_size - chunk_offset;
        let chunk_length = remaining_bytes.min(chunk_bytes);
        validate_chunk_range(shard_size, chunk_offset, chunk_length)?;
        chunk_ranges.push(ChunkRange {
            offset: chunk_offset,
            length: chunk_length,
        });
        chunk_offset += chunk_length;
    }

    Ok(chunk_ranges)
}

#[inline(always)]
fn collect_chunk_windows<'arena>(
    slot_count: usize,
    shard_size: usize,
    chunk_ranges: &[ChunkRange],
    bytes: &'arena mut [u8],
) -> Vec<Vec<&'arena mut [u8]>> {
    let mut chunk_windows = (0..chunk_ranges.len())
        .map(|_| Vec::with_capacity(slot_count))
        .collect::<Vec<Vec<&'arena mut [u8]>>>();

    for shard_bytes in bytes.chunks_exact_mut(shard_size).take(slot_count) {
        let mut remaining = shard_bytes;
        let mut chunk_offset = 0;

        for (chunk_index, chunk_range) in chunk_ranges.iter().enumerate() {
            debug_assert_eq!(chunk_offset, chunk_range.offset);
            let (window, next) = remaining.split_at_mut(chunk_range.length);
            chunk_windows[chunk_index].push(window);
            remaining = next;
            chunk_offset += chunk_range.length;
        }
    }

    chunk_windows
}

#[inline(always)]
fn encode_shard_windows_in_place(
    encoder: &mut DefaultRateEncoder<DefaultEngine>,
    original_count: usize,
    recovery_count: usize,
    shard_windows: &mut [&mut [u8]],
) -> Result<(), RepairError> {
    for slot_index in 0..original_count {
        encoder.add_original_shard(&shard_windows[slot_index][..])?;
    }

    let result = encoder.encode()?;
    for recovery_index in 0..recovery_count {
        let target = &mut shard_windows[original_count + recovery_index];
        target.copy_from_slice(
            result
                .recovery(recovery_index)
                .ok_or(RepairError::MissingRecoveryShard {
                    index: recovery_index,
                })?,
        );
    }

    Ok(())
}

#[inline(always)]
fn repair_shard_windows_in_place(
    decoder: &mut DefaultRateDecoder<DefaultEngine>,
    encoder: &mut DefaultRateEncoder<DefaultEngine>,
    original_count: usize,
    repair_plan: &RepairPlan,
    shard_windows: &mut [&mut [u8]],
) -> Result<(), RepairError> {
    if !repair_plan.missing_original_indices.is_empty() {
        for &slot_index in &repair_plan.available_original_indices {
            decoder.add_original_shard(slot_index, &shard_windows[slot_index][..])?;
        }

        for &recovery_index in &repair_plan.available_recovery_indices {
            let slot_index = original_count + recovery_index;
            decoder.add_recovery_shard(recovery_index, &shard_windows[slot_index][..])?;
        }

        let result = decoder.decode()?;
        for &index in &repair_plan.missing_original_indices {
            let target = &mut shard_windows[index];
            target.copy_from_slice(
                result
                    .restored_original(index)
                    .ok_or(RepairError::MissingOriginalShard { index })?,
            );
        }
    }

    if !repair_plan.missing_recovery_indices.is_empty() {
        for slot_index in 0..original_count {
            encoder.add_original_shard(&shard_windows[slot_index][..])?;
        }

        let result = encoder.encode()?;
        for &recovery_index in &repair_plan.missing_recovery_indices {
            let target = &mut shard_windows[original_count + recovery_index];
            target.copy_from_slice(
                result
                    .recovery(recovery_index)
                    .ok_or(RepairError::MissingRecoveryShard {
                        index: recovery_index,
                    })?,
            );
        }
    }

    Ok(())
}

fn parallel_encode_in_buffer_with_chunk_bytes(
    original_count: usize,
    shard_size: usize,
    bytes: &mut [u8],
    chunk_bytes: usize,
) -> Result<usize, RepairError> {
    let layout = validate_encode_layout(original_count, shard_size, bytes)?;
    let chunk_ranges = build_chunk_ranges(shard_size, chunk_bytes)?;
    let max_chunk_bytes = chunk_ranges.first().map_or(shard_size, |chunk_range| chunk_range.length);
    let mut chunk_windows = collect_chunk_windows(layout.slot_count, shard_size, &chunk_ranges, bytes);

    // `try_for_each_init` touches each Rayon worker before it processes chunk windows. The
    // thread-local cache builds `DefaultEngine::new()` on first use for that worker and only resets
    // dimensions afterward. Keep the closure below allocation-free: all Vec construction is done
    // before this point, and the SIMD section should only read shard slices and write recovery
    // bytes.
    chunk_windows.par_iter_mut().try_for_each_init(
        || init_encode_thread_state(original_count, layout.recovery_count, max_chunk_bytes),
        |_, window| {
            let chunk_length = window.first().map_or(0, |shard_window| shard_window.len());
            with_encode_thread_state(|thread_state| {
                let encoder = thread_state.encoder(original_count, layout.recovery_count, chunk_length)?;
                encode_shard_windows_in_place(
                    encoder,
                    original_count,
                    layout.recovery_count,
                    window.as_mut_slice(),
                )
            })
        },
    )?;

    Ok(layout.recovery_count)
}

fn parallel_repair_in_buffer_with_chunk_bytes(
    original_count: usize,
    shard_size: usize,
    missing_indices: &[u32],
    bytes: &mut [u8],
    chunk_bytes: usize,
) -> Result<usize, RepairError> {
    let layout = validate_repair_layout(original_count, shard_size, bytes)?;
    let repair_plan = build_repair_plan(original_count, layout.slot_count, missing_indices)?;
    if repair_plan.missing_count == 0 {
        return Ok(0);
    }

    let chunk_ranges = build_chunk_ranges(shard_size, chunk_bytes)?;
    let max_chunk_bytes = chunk_ranges.first().map_or(shard_size, |chunk_range| chunk_range.length);
    let mut chunk_windows = collect_chunk_windows(layout.slot_count, shard_size, &chunk_ranges, bytes);

    // Repair has the same rule as encode: initialize per-worker codec state outside the window
    // body, then keep the closure free of heap work while the SIMD engine is saturating the memory
    // bus.
    chunk_windows.par_iter_mut().try_for_each_init(
        || init_repair_thread_state(original_count, layout.recovery_count, max_chunk_bytes),
        |_, window| {
            let chunk_length = window.first().map_or(0, |shard_window| shard_window.len());
            with_repair_thread_state(|thread_state| {
                let (decoder, encoder) = thread_state.codecs(
                    original_count,
                    layout.recovery_count,
                    chunk_length,
                )?;
                repair_shard_windows_in_place(
                    decoder,
                    encoder,
                    original_count,
                    &repair_plan,
                    window.as_mut_slice(),
                )
            })
        },
    )?;

    Ok(repair_plan.missing_count)
}

fn parallel_encode_in_buffer(
    original_count: usize,
    shard_size: usize,
    bytes: &mut [u8],
) -> Result<usize, RepairError> {
    native_thread_pool().install(|| {
        parallel_encode_in_buffer_with_chunk_bytes(
            original_count,
            shard_size,
            bytes,
            dynamic_encode_chunk_bytes(shard_size),
        )
    })
}

fn parallel_repair_in_buffer(
    original_count: usize,
    shard_size: usize,
    missing_indices: &[u32],
    bytes: &mut [u8],
) -> Result<usize, RepairError> {
    native_thread_pool().install(|| {
        parallel_repair_in_buffer_with_chunk_bytes(
            original_count,
            shard_size,
            missing_indices,
            bytes,
            dynamic_repair_chunk_bytes(shard_size),
        )
    })
}

#[napi]
impl Task for EncodeTask {
    type Output = u32;
    type JsValue = u32;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        parallel_encode_in_buffer(self.original_count, self.shard_size, self.shard_bytes.as_mut())
            .map(|encoded| encoded as u32)
            .map_err(to_napi_error)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi]
impl Task for RepairTask {
    type Output = u32;
    type JsValue = u32;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        parallel_repair_in_buffer(
            self.original_count,
            self.shard_size,
            &self.missing_indices,
            self.shard_bytes.as_mut(),
        )
        .map(|repaired| repaired as u32)
        .map_err(to_napi_error)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi(js_name = "leopard_encode")]
pub fn leopard_encode_js(
    original_count: u32,
    shard_size: u32,
    shard_bytes: Buffer,
) -> AsyncTask<EncodeTask> {
    AsyncTask::new(EncodeTask {
        original_count: original_count as usize,
        shard_size: shard_size as usize,
        shard_bytes,
    })
}

#[napi(js_name = "leopard_repair")]
pub fn leopard_repair_js(
    original_count: u32,
    shard_size: u32,
    missing_indices: Vec<u32>,
    shard_bytes: Buffer,
) -> AsyncTask<RepairTask> {
    AsyncTask::new(RepairTask {
        original_count: original_count as usize,
        shard_size: shard_size as usize,
        missing_indices,
        shard_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AlignedArena,
        build_chunk_ranges,
        dynamic_chunk_bytes_for_cpus,
        dynamic_repair_chunk_bytes_for_cpus,
        parallel_encode_in_buffer_with_chunk_bytes,
        parallel_repair_in_buffer_with_chunk_bytes,
        ChunkRange,
        MIN_ENCODE_PARALLEL_CHUNK_BYTES,
        MIN_REPAIR_PARALLEL_CHUNK_BYTES,
    };

    fn build_original_shards(original_count: usize, shard_size: usize) -> Vec<Vec<u8>> {
        (0..original_count)
            .map(|shard_index| {
                let mut shard = vec![0; shard_size];
                for offset in 0..shard_size {
                    shard[offset] = ((shard_index * 37 + offset * 17) % 251) as u8;
                }
                shard
            })
            .collect()
    }

    fn write_originals(bytes: &mut [u8], shard_size: usize, original_shards: &[Vec<u8>]) {
        for (slot_index, shard) in original_shards.iter().enumerate() {
            let start = slot_index * shard_size;
            bytes[start..start + shard_size].copy_from_slice(shard);
        }
    }

    #[test]
    fn dynamic_chunk_bytes_stays_aligned_and_respects_minimum_work_quantum() {
        let shard_size = 4 * 1024 * 1024;
        let chunk_bytes = dynamic_chunk_bytes_for_cpus(shard_size, 8);

        assert_eq!(chunk_bytes % 64, 0);
        assert!(chunk_bytes >= MIN_ENCODE_PARALLEL_CHUNK_BYTES);
        assert!(shard_size.div_ceil(chunk_bytes) <= 16);
    }

    #[test]
    fn repair_chunk_bytes_use_coarser_parallel_granularity() {
        let shard_size = 4 * 1024 * 1024;
        let encode_chunk_bytes = dynamic_chunk_bytes_for_cpus(shard_size, 8);
        let repair_chunk_bytes = dynamic_repair_chunk_bytes_for_cpus(shard_size, 8);

        assert_eq!(repair_chunk_bytes % 64, 0);
        assert!(repair_chunk_bytes >= MIN_REPAIR_PARALLEL_CHUNK_BYTES);
        assert!(repair_chunk_bytes >= encode_chunk_bytes);
    }

    #[test]
    fn aligned_arena_allocation_is_64_byte_aligned() {
        let arena = AlignedArena::allocate(4096).unwrap();

        assert_eq!(arena.ptr.as_ptr() as usize % 64, 0);
        arena.release(None);
    }

    #[test]
    fn build_chunk_ranges_preserve_cache_line_boundaries() {
        let chunk_ranges = build_chunk_ranges(130, 64).unwrap();

        assert_eq!(
            chunk_ranges,
            vec![
                ChunkRange {
                    offset: 0,
                    length: 64,
                },
                ChunkRange {
                    offset: 64,
                    length: 64,
                },
                ChunkRange {
                    offset: 128,
                    length: 2,
                },
            ]
        );
    }

    #[test]
    fn parallel_encode_matches_existing_chunked_codec() {
        let original_count = 3;
        let recovery_count = 2;
        let shard_size = 130;
        let slot_count = original_count + recovery_count;
        let chunk_bytes = 64;
        let original_shards = build_original_shards(original_count, shard_size);
        let mut expected = vec![0; slot_count * shard_size];
        let mut actual = vec![0; slot_count * shard_size];

        write_originals(&mut expected, shard_size, &original_shards);
        write_originals(&mut actual, shard_size, &original_shards);

        for (chunk_offset, chunk_length) in [(0, 64), (64, 64), (128, 2)] {
            super::super::encode_in_place(
                original_count,
                shard_size,
                slot_count,
                &mut expected,
                chunk_offset,
                chunk_length,
            )
            .unwrap();
        }

        parallel_encode_in_buffer_with_chunk_bytes(original_count, shard_size, &mut actual, chunk_bytes)
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_repair_matches_existing_chunked_codec() {
        let original_count = 3;
        let recovery_count = 2;
        let shard_size = 130;
        let slot_count = original_count + recovery_count;
        let chunk_bytes = 64;
        let missing_indices = vec![1_u32, 4_u32];
        let original_shards = build_original_shards(original_count, shard_size);
        let mut encoded = vec![0; slot_count * shard_size];

        write_originals(&mut encoded, shard_size, &original_shards);
        parallel_encode_in_buffer_with_chunk_bytes(original_count, shard_size, &mut encoded, chunk_bytes)
            .unwrap();

        let mut expected = encoded.clone();
        let mut actual = encoded;

        for &missing_index in &missing_indices {
            let start = missing_index as usize * shard_size;
            expected[start..start + shard_size].fill(0);
            actual[start..start + shard_size].fill(0);
        }

        for (chunk_offset, chunk_length) in [(0, 64), (64, 64), (128, 2)] {
            super::super::repair_in_place(
                original_count,
                shard_size,
                &missing_indices,
                slot_count,
                &mut expected,
                chunk_offset,
                chunk_length,
            )
            .unwrap();
        }

        parallel_repair_in_buffer_with_chunk_bytes(
            original_count,
            shard_size,
            &missing_indices,
            &mut actual,
            chunk_bytes,
        )
        .unwrap();

        assert_eq!(actual, expected);
    }
}
