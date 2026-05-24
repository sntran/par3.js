# par3

`par3` is an isomorphic PAR3-style erasure-coding library and streaming repair
service. It exposes one JavaScript API while selecting a different codec backend
for edge runtimes and local Node.js.

- Cloudflare Workers (`workerd`), browsers, and the default export load
  `lib/mod.js`, backed by the repository-local `wasm32-simd128` WebAssembly
  build.
- Node.js loads `lib/mod.node.js`, backed by the Rust N-API addon in
  `src/napi.rs`.
- Both profiles share `lib/par3-core.js` for layout validation, search-param
  parsing, slot accounting, and size caps.

The Worker path is built for bounded CPU slices and linear streams. The Node
path is built for physical cores, 64-byte cache lines, and the native SIMD engine
selected by `reed-solomon-simd`.

## Overview & Isomorphic Design

`package.json` uses conditional exports:

```json
{
  "exports": {
    ".": {
      "workerd": "./lib/mod.js",
      "browser": "./lib/mod.js",
      "node": "./lib/mod.node.js",
      "default": "./lib/mod.js"
    }
  }
}
```

That split keeps the public `Par3` class stable while changing the memory owner:

- Edge runtimes use WebAssembly linear memory from `pkg/par3_bg.wasm`. The wasm
  package auto-initializes on import; there is no default `init()` export.
- Node.js uses an external Buffer whose backing store is allocated by Rust and
  released by the V8 finalizer.
- The CLI in `bin/main.js`, the Worker in `_worker.js`, and the examples all use
  the same `Par3` surface.

```text
                  ┌──────────────────────────────┐
                  │   Isomorphic Consumer API    │
                  └──────────────┬───────────────┘
                                 │
                   Conditional Package Exports
                                 │
                ┌────────────────┴────────────────┐
                ▼                                 ▼
      [ workerd / Browser ]                  [ Node.js ]
     Single-Threaded WASM               Multi-Threaded N-API
  ┌─────────────────────────┐        ┌─────────────────────────┐
  │  - JS chunk loop        │        │  - Rust aligned arena   │
  │  - Wasm heap isolation  │        │  - Revocable JS Proxy   │
  │  - CPU-humane yielding  │        │  - Rayon worker pools   │
  │  - SIMD128 vectors      │        │  - Zero-alloc closures  │
  └─────────────────────────┘        └─────────────────────────┘
```

High-value source files:

- `src/lib.rs`: wasm pointer registry, shard arenas, and codec entrypoints.
- `src/napi.rs`: N-API arena allocation, Rayon scheduling, and native tasks.
- `lib/par3-core.js`: shared validation and layout rules.
- `lib/mod.js`: WebAssembly profile.
- `lib/mod.node.js`: Node native profile.
- `lib/multipart.js`: RFC 7578 multipart parser and encoder.
- `_worker.js`: streamed HTTP encode and repair service.
- `bin/main.js`: local CLI and executable entrypoint.

## The Edge Profile (WASM)

The Edge profile runs in a single JavaScript isolate. It stores shard bytes in
wasm linear memory and hands JavaScript typed-array views into that memory for
streamed writes and reads. The Worker never needs Node native state and stays
inside Cloudflare's `nodejs_compat` layer.

`BasePar3.CODEC_CHUNK_BYTES` is 1 MiB. `lib/mod.js` runs encode and repair one
chunk at a time, then awaits `setTimeout(0)` before the next chunk. Keep that
yield. It gives V8 a scheduling point between codec slices, lets stream
microtasks run, and prevents long repair jobs from starving the Worker event
loop.

The chunk alignment rule is part of the codec contract:

- chunks start on 64-byte boundaries,
- mid-shard chunks have 64-byte lengths,
- only the true shard tail may end off-boundary,
- `assertChunkAlignment()` rejects a JavaScript profile whose chunk size breaks
  that rule.

Build the wasm profile with:

```bash
npm run build:wasm
```

That runs `scripts/build-wasm.sh`, installs the `wasm32-unknown-unknown` target
if needed, and emits `pkg/` with `simd128` enabled.

## The Metal Profile (N-API)

The Metal profile is selected only by the `node` export condition. It loads
`lib/par3.node`, runs N-API tasks off the main thread, and uses Rayon over the
Rust `reed-solomon-simd` default engine. On hosts where that crate selects
AVX-512, AVX2, or another native SIMD backend, Node gets that hardware path
without changing the JavaScript API.

The native arena is Rust-owned:

- `src/napi.rs` allocates the backing store with `std::alloc::alloc_zeroed`.
- the layout is 64-byte aligned so Rayon jobs do not fight over cache-line
  boundaries,
- the hot encode and repair closures reuse thread-local rate codecs,
- the Rayon pool defaults to physical-core count, with `PAR3_NATIVE_THREADS` as
  an override,
- `PAR3_NATIVE_REPAIR_CHUNK_BYTES` can raise or lower repair work granularity
  for local profiling.

The intended hot path is allocation-free after the chunk windows and repair plan
are built. Thread-local codec state is warmed before each worker processes its
window; the inner loop should only reset codec state, add shard slices, execute
SIMD work, and copy restored bytes into the arena.

Build the native profile with:

```bash
npm run build:native
```

The generated `lib/par3.node` file is a local build product. Do not commit it.

## Profile Benchmark

Run both profiles with:

```bash
npm run benchmark
```

Current local run on May 24, 2026:

- payload: 250 MiB,
- layout: 100 original shards, 20 recovery shards,
- `shard_size`: 2621440 bytes,
- arena: 300 MiB,
- wasm build: `wasm32-simd128`,
- native build: N-API release addon with `target-cpu=native`.

| Implementation | Encode ms | Encode MiB/s | Repair ms | Repair MiB/s | Encode vs WASM | Repair vs WASM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| WASM | 640.73 | 390.18 | 3144.50 | 79.50 | 1.00x | 1.00x |
| Native | 279.91 | 893.14 | 1332.61 | 187.60 | 2.29x | 2.36x |

Treat these as host-local numbers. The relative gap depends on CPU SIMD width,
core count, memory bandwidth, and runtime limits. The benchmark is still useful:
it exercises the same public API on both profiles and verifies repaired shard
bytes before reporting timings.

## Streaming Protocol

The Worker exposes one resource path and lets HTTP verbs select the operation:

- `POST /`: ingest original shards and stream generated recovery shards.
- `PATCH /`: ingest present shards and stream requested repairs.

This keeps the API REST-idiomatic without adding encode and repair URL paths.

Required query parameters:

- `n` or `original_count`: number of original shards.
- `s` or `shard_size`: shard size in bytes. It must be non-zero and even.
- `r` or `recovery_count`: recovery shard count.
- `total_shard_count`: accepted instead of `recovery_count` when the caller
  already knows the full slot count.

Repair-only query parameters:

- repeated `i` or `missing_indices`: shard indexes that the client wants back.

The Worker rejects streaming requests unless `recovery_count` or
`total_shard_count` locks the slot layout up front. It no longer grows layouts
after ingress starts.

The request body must be binary `multipart/form-data`:

- optional `digests`: one contiguous SHA-256 digest blob, 32 bytes per slot,
  with all-zero digests disabling verification for a slot,
- `shard_<slotIndex>`: raw shard bytes.

When `digests` is present, it must arrive before shard parts. Each shard is
verified before it is committed to the repair set.

The response is also `multipart/form-data`. The Worker returns headers
immediately, then a background pump reads ingress, runs the codec, and writes
parts as they are ready. If ingress or repair fails after headers have been
sent, the response body aborts. There is no late JSON error body on a live
stream.

Backpressure is a correctness rule here. Keep the path linear:

- do not call `arrayBuffer()` on the full request or response body,
- do not use `.tee()` in the hot path,
- process each multipart part as a stream,
- write each output part through the response writer before advancing.

That shape lets downstream TCP pressure slow the response writer, then the
codec, then multipart ingress, then the upstream client. Buffering the full body
moves shard storage into the V8 heap and can OOM the isolate under large repair
sets.

Example encode request:

```bash
curl -X POST 'https://<your-worker>/?n=4&r=2&s=65536' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_1=@./shards/shard_1.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream'
```

Example repair request:

```bash
curl -X PATCH 'https://<your-worker>/?n=4&r=2&s=65536&i=1' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream' \
  -F 'shard_4=@./shards/shard_4.bin;type=application/octet-stream'
```

Repair returns only the requested missing indexes. The codec still marks every
unreceived slot as missing before repair so reconstruction stays correct when a
client omits recovery shards it does not need returned.

## Security & Memory Safety

The wasm profile uses generation-tagged allocation handles in `src/lib.rs`.
Stale handles stop resolving when a registry slot is reused, even if the raw
pointer value repeats later.

The Node profile has a stricter V8 boundary:

- `#arena` is private to the facade,
- issued shard views are Proxies,
- `.buffer`, `.parent`, `.byteOffset`, and `.offset` are trapped so callers
  cannot unwrap the external backing store and retain a mutable alias,
- `#runWhileLocked()` invalidates every issued view before a native background
  task starts,
- `free()` invalidates issued views and drops the only JavaScript reference to
  the external Buffer.

After Rust hands an arena to V8, the successful allocation path has one physical
free owner: the V8 finalizer callback registered with N-API. JavaScript can make
the arena unreachable, but it does not call `dealloc` directly. That ownership
line blocks double-free paths and stops stale JavaScript aliases from mutating
memory while Rayon owns the buffer.

## Local CLI

The executable entrypoint is `bin/main.js`. From a checkout:

```bash
node ./bin/main.js -h
node ./bin/main.js create ...
node ./bin/main.js repair ...
```

Porcelain mode packs input files into PAR2-style manifest and recovery packets:

```bash
node ./bin/main.js create \
  -r 2 \
  -s 65536 \
  --set-name archive \
  --output-dir ./parity \
  ./archive.bin ./photo.jpg
```

```bash
node ./bin/main.js repair \
  ./parity/archive.par2 \
  --input-dir ./downloads \
  --output-dir ./repaired
```

Raw-shard mode remains available for slot-level workflows:

```bash
node ./bin/main.js create \
  -n 4 \
  -r 2 \
  -s 65536 \
  --shard 0=./shards/shard_0.bin \
  --shard 1=./shards/shard_1.bin \
  --shard 2=./shards/shard_2.bin \
  --shard 3=./shards/shard_3.bin \
  --output-dir ./created
```

```bash
node ./bin/main.js repair \
  -n 4 \
  -r 2 \
  -s 65536 \
  --shard 0=./shards/shard_0.bin \
  --shard 2=./shards/shard_2.bin \
  --shard 3=./shards/shard_3.bin \
  --shard 4=./shards/shard_4.bin \
  --output-dir ./repaired \
  --missing-index 1
```

## Interoperability With PAR2

PAR2 compatibility is a project goal, not an afterthought. The porcelain CLI can
read and write a strict PAR2-style packet subset today. That layer carries main
packets, file description packets, checksums, and recovery slice packets using
the PAR2 packet dialect in `lib/envelope.js`.

The current compatibility boundary is explicit:

- packet framing, packet checksums, file metadata, and recovery-block storage use
  the PAR2-style envelope path,
- the shard codec is still this repository's native erasure-coding engine,
- raw-shard mode remains available when a workflow needs direct slot control,
- mixed PAR2/PAR3 packet families are rejected instead of silently merged.

Do not document this as full legacy PAR2 matrix compatibility until the codec
itself is proven against independent PAR2 implementations. The current envelope
layer is meant to make the packet surface compatible first while keeping the
codec path testable inside this repository.

For slot-oriented interoperability work, split the original file into fixed-size
raw blocks that match `shard_size`, then copy each block into the
`shard_<slot>.bin` naming scheme expected by the raw CLI and Worker paths:

```bash
split -b 65536 --numeric-suffixes=0 --suffix-length=3 ./archive.bin ./raw/shard_
tail_shard=$(printf '%s\n' ./raw/shard_* | sort | tail -n 1)
truncate -s 65536 "$tail_shard"
cat ./raw/shard_000 > ./shards/shard_0.bin
cat ./raw/shard_001 > ./shards/shard_1.bin
cat ./raw/shard_002 > ./shards/shard_2.bin
cat ./raw/shard_003 > ./shards/shard_3.bin
```

The `truncate` step is required when the original file length is not an exact
multiple of `shard_size`; every shard handed to `par3` must have the declared
byte length.

For the parity side, extract raw recovery payloads from `.par2` volumes with a
packet-aware tool. Once those payloads are unwrapped, copy each one into the
matching recovery slot file that the raw-shard workflow should see:

```bash
cat ./unwrapped/recovery_000.bin > ./shards/shard_4.bin
cat ./unwrapped/recovery_001.bin > ./shards/shard_5.bin
```

After both sides are in raw shard form, `par3 create` and `par3 repair` operate
on them directly.

## Example Worker

`examples/fetch_encode_repair_gzip.js` imports `_worker.js` as an internal
service and runs a linear pipeline:

1. fetch a remote asset once,
2. split it into fixed-size original shards,
3. stream originals into `POST /`,
4. omit the last one or two originals from `PATCH /`,
5. stream surviving originals plus generated recovery shards into `PATCH /`,
6. write the repaired tail directly into `CompressionStream("gzip")`.

The example needs an upstream `Content-Length` header so it can choose the shard
layout before streaming starts.

Run it locally:

```bash
npm run build:wasm
npx wrangler dev ./examples/fetch_encode_repair_gzip.js --local
```

The response includes `x-demo-dropped-shards` with the original shard indexes
that were intentionally omitted from the repair request.

## Build And Test

Install dependencies:

```bash
npm install
```

Run the default validation:

```bash
npm test
```

That runs:

- `cargo test --lib`,
- the Node `node:test` suite after rebuilding wasm.

Run the enforced JavaScript coverage gate:

```bash
npm run test:coverage
```

It enforces 100% line, branch, and function coverage for:

- `_worker.js`,
- `bin/main.js`,
- `lib/envelope.js`,
- `lib/mod.js`,
- `lib/multipart.js`.

Run the native addon coverage gate:

```bash
npm run test:native
```

That rebuilds `lib/par3.node` and enforces 100% line, branch, and function
coverage for `lib/mod.node.js`.

For Rust-only edits, start with:

```bash
cargo test --lib
```

For Rust or wasm-facing JavaScript edits, rebuild wasm with:

```bash
npm run build:wasm
```

Generated artifacts are ignored by Git:

- `pkg/`,
- `target/`,
- `lib/*.node`,
- `.coverage*`,
- `coverage_output.txt`.

## Project Layout

- `src/lib.rs`: wasm codec core, pointer registry, and handle validation.
- `src/napi.rs`: native arena allocation and Rayon codec tasks.
- `lib/par3-core.js`: shared layout validation and resource caps.
- `lib/mod.js`: wasm-backed `Par3` implementation.
- `lib/mod.node.js`: native-backed `Par3` implementation.
- `lib/envelope.js`: PAR2/PAR3 packet parser and packer for CLI porcelain.
- `lib/multipart.js`: multipart decoder and encoder utilities.
- `_worker.js`: streamed Worker API for `POST /` and `PATCH /`.
- `examples/fetch_encode_repair_gzip.js`: fetch, encode, repair, gzip demo.
- `bin/main.js`: CLI implementation and executable.
- `scripts/build-wasm.sh`: wasm build helper.
- `scripts/test-coverage.mjs`: JS coverage gate.
- `scripts/test-native-coverage.mjs`: native facade coverage gate.

## Contributing

See `CONTRIBUTING.md` for workflow details and `AGENTS.md` for repository-local
rules.
