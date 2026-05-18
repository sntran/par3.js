# par3

`par3` is a streaming PAR3-style erasure coding service and local repair tool. It combines a Rust
`reed-solomon-simd` core, a Cloudflare Worker that processes multipart uploads over Web Streams,
and a Node.js CLI for repairing shard sets from disk.

The repository targets Node.js 26.1+ for local development while keeping Worker-facing code inside
Cloudflare's `nodejs_compat` compatibility layer.

## Overview

The repository has five main layers:

1. `src/lib.rs`
   Rust manages shard arenas in wasm linear memory and performs encode/repair operations.
2. `lib/multipart.js`
  Generic RFC 7578 multipart decoder/encoder utilities used by the Worker request path and the
  protocol-focused test suite.
3. `lib/mod.js`
  The shared `Par3` class owns shard-layout validation, arena growth, in-place repair, and shard
  extraction for both runtime entrypoints.
4. `_worker.js`
  The Worker reads layout metadata from URL search params, returns a streaming response
  immediately, ingests binary shard parts directly into wasm memory in the background, and aborts
  the response stream if a late ingress or repair error occurs under Cloudflare's `nodejs_compat`
  layer.
5. `bin/main.js`
  The local CLI exposes `create` and `repair` subcommands that read shard files from disk and
  write recovery or repaired output shards into an output directory.

## Features

- Streaming multipart request handling in the Worker
- Generic RFC 7578 multipart parsing and encoding in `lib/multipart.js`
- In-place encode and repair over wasm linear memory
- Shared `Par3` runtime used by both the Worker and the CLI
- Built-in `create` and `repair` subcommands with `par3cmdline`-style `-n`, `-r`, and `-s` flags
- Deterministic property-style tests in Rust and Node.js
- Built-in Node.js CLI with no third-party runtime dependencies for argument parsing
- Enforced 100% line, branch, and function coverage for maintained JS entrypoints

## Prerequisites

- Node.js 26.1+
- Rust stable
- `wasm-pack`

## Install

```bash
npm install
```

## Build

```bash
npm run build
```

This runs `scripts/build-wasm.sh`, which ensures the `wasm32-unknown-unknown` target is present and
builds the package into `pkg/` with `simd128` enabled.

The `-C target-feature=+simd128` flag is currently future-proofing rather than an active fast path
for this dependency stack. `reed-solomon-simd` 3.1.0 documents optimized engines for SSSE3, AVX2,
and NEON, but no WebAssembly SIMD backend, so the wasm build currently falls back to the scalar
engine. The crate's public `rate::Engine` extension point is a plausible hook for a future
`wasm32-simd128` backend, but this repository does not yet ship that custom engine.

## Test

```bash
npm test
```

The default test command runs:

- `cargo test --lib`
- Node's built-in `node:test` suite for Worker and CLI behavior

The Node-side test harness also uses newer built-in runtime features, including disposable
temporary directories and `node:test` method mocks.

JS coverage for maintained source files is available with:

```bash
npm run test:coverage
```

This command uses Node's built-in test coverage output plus a repository-local coverage gate to
enforce 100% line, branch, and function coverage for `_worker.js`, `bin/main.js`, `lib/mod.js`,
and `lib/multipart.js`.

## Worker request contract

The Worker expects layout metadata on the request URL and a strictly binary `multipart/form-data`
body.

Query parameters:

- `n` or `original_count`: number of original data shards
- `r` or `recovery_count`: number of recovery shards
- `s` or `shard_size`: shard size in bytes, must be even
- repeated `i` or `missing_indices`: shard indexes the caller wants returned
- `total_shard_count`: optional explicit slot count when `recovery_count` is not supplied

The Worker body may contain only:

- `digests`: optional raw SHA-256 digest blob containing one 32-byte digest per slot
- `shard_<slotIndex>`: raw shard bytes

When `digests` is present, it must appear before shard parts so each shard can be verified before
it is committed to the repair set. Each slot consumes 32 bytes in the digest blob; an all-zero
digest disables verification for that slot.

The Worker requires a locked slot count for streaming safety. Include either `r`/`recovery_count`
or `total_shard_count`; requests that rely on slot-count inference are rejected.

Resource caps enforced before any wasm allocation:

- `total_shard_count <= 32768`
- `shard_size <= 10485760` (10 MiB)
- `total_shard_count * shard_size <= 134217728` (128 MiB)

Example `curl` request:

```bash
curl -X POST 'https://<your-worker>/repair?n=4&r=2&s=65536&i=1' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream' \
  -F 'shard_4=@./shards/shard_4.bin;type=application/octet-stream'
```

Important runtime rule:

- The Worker only returns shard indexes listed in `missing_indices`.
- The codec still needs every unreceived slot marked as missing before repair.
- If `digests` are provided, every received shard is verified before it is committed to the repair
  set.
- The body must remain binary-only: only `digests` and `shard_<slotIndex>` parts are accepted.
- The Worker returns the multipart response headers immediately; if ingress or repair fails after
  that point, the response body aborts and the client sees a stream-level failure rather than a
  late JSON error payload.

Worker compatibility is declared explicitly in `wrangler.toml` via
`compatibility_flags = ["nodejs_compat"]`.

That distinction is what keeps reconstruction correct when clients intentionally omit extra recovery
shards they do not need returned.

## Local CLI

The CLI uses built-in Node.js modules for argument parsing and filesystem access.

When the package is installed, the executable name is `par3`. From a checkout, `node
./bin/main.js` runs the same interface.

The command naming intentionally matches `par3cmdline` where the workflows overlap:

- `par3 -h`
- `par3 -V`
- `par3 c(reate) ...`
- `par3 r(epair) ...`

This project does not implement the full `.par3` volume-file workflow from `par3cmdline`; its CLI
repairs raw shard layouts directly.

### Usage

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

### Full repair example

Assume slot `1` and recovery slot `5` are missing from a six-slot layout (`0..5`):

```bash
node ./bin/main.js repair \
  -n 4 \
  -r 2 \
  -s 65536 \
  --shard 0=./fixtures/shard_0.bin \
  --shard 2=./fixtures/shard_2.bin \
  --shard 3=./fixtures/shard_3.bin \
  --shard 4=./fixtures/shard_4.bin \
  --output-dir ./repaired
```

The CLI will:

1. infer that slots `1` and `5` are missing,
2. repair both slots so the codec sees the complete missing set,
3. write `repaired/shard_1.bin` and `repaired/shard_5.bin`.

If you only want one repaired shard written back out, add repeated `--missing-index` flags.

## Interoperability with PAR2

`par3` consumes raw shard bytes. It does not parse `.par2` packet envelopes, so PAR2 recovery
volumes must be unwrapped into raw shard payloads before you hand them to this repository.

For the data side, split the original file into fixed-size raw blocks that match `shard_size`, then
copy each block into the `shard_<slot>.bin` naming scheme expected by the CLI and Worker:

```bash
split -b 65536 --numeric-suffixes=0 --suffix-length=3 ./archive.bin ./raw/shard_
tail_shard=$(printf '%s\n' ./raw/shard_* | sort | tail -n 1)
truncate -s 65536 "$tail_shard"
cat ./raw/shard_000 > ./shards/shard_0.bin
cat ./raw/shard_001 > ./shards/shard_1.bin
cat ./raw/shard_002 > ./shards/shard_2.bin
cat ./raw/shard_003 > ./shards/shard_3.bin
```

That `truncate` step is required whenever the original file length is not already an exact multiple
of `shard_size`; it pads the final tail shard so every shard handed to `par3` is exactly 65536
bytes long.

For the parity side, first extract the raw recovery payloads from the `.par2` volumes with a
packet-aware PAR2 tool. Once those payloads are unwrapped, copy each one into the matching recovery
slot file that `par3` should see:

```bash
cat ./unwrapped/recovery_000.bin > ./shards/shard_4.bin
cat ./unwrapped/recovery_001.bin > ./shards/shard_5.bin
```

After both sides are in raw shard form, `par3 create` and `par3 repair` work on them directly.

## Project layout

- `src/lib.rs`: Rust codec core and wasm bindings
- `lib/multipart.js`: generic RFC 7578 multipart protocol helpers
- `lib/mod.js`: shared `Par3` shard repair/runtime abstraction
- `_worker.js`: Cloudflare Worker immediate-return repair orchestration and stream lifetime management
- `bin/main.js`: local CLI implementation and executable entrypoint
- `_worker_test.js`: Worker property-style and protocol tests
- `bin/main_test.js`: CLI behavior tests
- `lib/mod_test.js`: direct `Par3` class tests
- `scripts/build-wasm.sh`: wasm build helper
- `scripts/test-coverage.mjs`: built-in coverage gate for maintained JS files

## Contributing

See `CONTRIBUTING.md` for workflow details and `AGENTS.md` for repository-specific agent notes.
