# par3

`par3` is a streaming PAR3-style erasure coding service and local repair tool. It combines a Rust
`reed-solomon-simd` core, a Cloudflare Worker that processes multipart uploads over Web Streams,
and a Node.js CLI for repairing shard sets from disk.

The repository targets Node.js 26.1+ for local development while keeping Worker-facing code inside
Cloudflare's `nodejs_compat` compatibility layer.

## Overview

The repository has four main layers:

1. `src/lib.rs`
   Rust manages shard arenas in wasm linear memory and performs encode/repair operations.
2. `lib/mod.js`
   The shared `Par3` class owns shard-layout validation, arena growth, in-place repair, and shard
   extraction for both runtime entrypoints.
3. `_worker.js`
  The Worker parses multipart requests as a stream, copies shard bytes into wasm memory, and
  streams only the requested repaired shards back to the caller under Cloudflare's
  `nodejs_compat` layer.
4. `bin/main.js`
  The local CLI reads shard files from disk, repairs every absent slot, and writes repaired output
  shards into an output directory.

## Features

- Streaming multipart request handling in the Worker
- In-place encode and repair over wasm linear memory
- Shared `Par3` runtime used by both the Worker and the CLI
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
enforce 100% line, branch, and function coverage for `_worker.js`, `bin/main.js`, and `lib/mod.js`.

## Worker request contract

The Worker expects a `multipart/form-data` body where the first part is named `metadata` and shard
parts are named `shard_<slotIndex>`.

Metadata fields:

- `original_count`: number of original data shards
- `recovery_count`: number of recovery shards, or omit this and provide `total_shard_count`
- `shard_size`: shard size in bytes, must be even
- `missing_indices`: shard indexes the caller wants returned

Example metadata payload:

```json
{
  "original_count": 4,
  "recovery_count": 2,
  "shard_size": 65536,
  "missing_indices": [1]
}
```

Example `curl` request:

```bash
curl -X POST https://<your-worker>/repair \
  -F 'metadata={"original_count":4,"recovery_count":2,"shard_size":65536,"missing_indices":[1]};type=application/json' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream' \
  -F 'shard_4=@./shards/shard_4.bin;type=application/octet-stream'
```

Important runtime rule:

- The Worker only returns shard indexes listed in `missing_indices`.
- The codec still needs every unreceived slot marked as missing before repair.

Worker compatibility is declared explicitly in `wrangler.toml` via
`compatibility_flags = ["nodejs_compat"]`.

Successful repair responses also include `x-par3-repaired-count` with the number of returned shard
parts.

That distinction is what keeps reconstruction correct when clients intentionally omit extra recovery
shards they do not need returned.

## Local CLI

The CLI uses built-in Node.js modules for argument parsing and filesystem access.

When the package is installed, the executable name is `par3`. From a checkout, `node
./bin/main.js` runs the same interface.

The command naming intentionally matches `par3cmdline` where the workflows overlap:

- `par3 -h`
- `par3 -V`
- `par3 r(epair) ...`

This project does not implement the full `.par3` volume-file workflow from `par3cmdline`; its CLI
repairs raw shard layouts directly.

### Usage

```bash
node ./bin/main.js repair \
  --original-count 4 \
  --recovery-count 2 \
  --shard-size 65536 \
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
  --original-count 4 \
  --recovery-count 2 \
  --shard-size 65536 \
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

## Project layout

- `src/lib.rs`: Rust codec core and wasm bindings
- `lib/mod.js`: shared `Par3` shard repair/runtime abstraction
- `_worker.js`: Cloudflare Worker streaming repair pipeline
- `bin/main.js`: local CLI implementation and executable entrypoint
- `_worker_test.js`: Worker property-style and protocol tests
- `bin/main_test.js`: CLI behavior tests
- `lib/mod_test.js`: direct `Par3` class tests
- `scripts/build-wasm.sh`: wasm build helper
- `scripts/test-coverage.mjs`: built-in coverage gate for maintained JS files

## Contributing

See `CONTRIBUTING.md` for workflow details and `AGENTS.md` for repository-specific agent notes.
