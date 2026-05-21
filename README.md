# par3

`par3` is a streaming PAR3-style erasure coding service and local repair tool. It combines a Rust
`reed-solomon-simd` core, a Cloudflare Worker that processes streamed multipart encode and repair
requests over Web Streams, and a Node.js CLI that can operate on either raw shard sets or
packetized PAR2-style envelopes.

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
  4. `lib/envelope.js`
    Node-side PAR2/PAR3 packet framing used by the CLI porcelain layer. It keeps file metadata,
    packet checksums, and recovery-block parsing out of the shared shard runtime.
  5. `_worker.js`
    The Worker reads layout metadata from URL search params, routes `POST /` for encode and
    `PATCH /` for repair through the same multipart ingress/egress pipeline, returns a streaming
    response immediately,
    and aborts the response stream if a late ingress or codec error occurs under Cloudflare's
    `nodejs_compat` layer.
  6. `bin/main.js`
    The local CLI exposes `create` and `repair` subcommands in two layers: a porcelain mode for
    packetized file sets and a raw-shard mode for direct slot-level workflows.

## Features

- Streaming multipart request handling in the Worker
- Verb-routed Worker encode and repair on `/`
- Generic RFC 7578 multipart parsing and encoding in `lib/multipart.js`
- Strict PAR2/PAR3 packet framing in `lib/envelope.js`
- In-place encode and repair over wasm linear memory
- Shared `Par3` runtime used by both the Worker and the CLI
- Built-in porcelain `create`/`repair` flows plus raw shard subcommands with `par3cmdline`-style
  `-n`, `-r`, and `-s` flags
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

## Performance

The wasm build now ships a repository-local `wasm32-simd128` engine that plugs into the
`reed-solomon-simd` rate API instead of falling back to the scalar path.

The shared JavaScript runtime also processes repair and recovery work asynchronously in aligned
chunks, yielding back to the event loop between chunks with `setTimeout(0)`. That keeps the Worker
response path streaming under heavy repair load instead of monopolizing the main thread until a
full-shard encode or repair completes.

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
enforce 100% line, branch, and function coverage for `_worker.js`, `bin/main.js`,
`lib/envelope.js`, `lib/mod.js`, and `lib/multipart.js`.

## Worker request contract

The Worker exposes the same binary multipart contract on the root path with method-based routing:

- `POST /`: ingest original shards and stream generated recovery shards back.
- `PATCH /`: ingest present shards and stream repaired requested outputs back.

Both methods expect layout metadata on the request URL and a strictly binary `multipart/form-data`
body.

Query parameters:

- `n` or `original_count`: number of original data shards
- `r` or `recovery_count`: number of recovery shards
- `s` or `shard_size`: shard size in bytes, must be even
- repeated `i` or `missing_indices`: shard indexes the caller wants returned from `PATCH /`
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

Example repair request:

```bash
curl -X PATCH 'https://<your-worker>/?n=4&r=2&s=65536&i=1' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream' \
  -F 'shard_4=@./shards/shard_4.bin;type=application/octet-stream'
```

Example encode request:

```bash
curl -X POST 'https://<your-worker>/?n=4&r=2&s=65536' \
  -F 'shard_0=@./shards/shard_0.bin;type=application/octet-stream' \
  -F 'shard_1=@./shards/shard_1.bin;type=application/octet-stream' \
  -F 'shard_2=@./shards/shard_2.bin;type=application/octet-stream' \
  -F 'shard_3=@./shards/shard_3.bin;type=application/octet-stream'
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

The CLI now has two surfaces:

- Porcelain mode: `create` packs input files into a PAR2-style manifest plus recovery volumes,
  and `repair` reads those envelopes back to restore missing or corrupt files.
- Raw-shard mode: the original `-n` / `-r` / `-s` interface still reads and writes slot files
  directly.

The packet layer is intentionally lean. It uses standard PAR2 packet framing and checksums, but it
still wraps the repository's native shard codec rather than a full legacy PAR2 matrix
implementation.

### Usage

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

### Raw shard mode

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

`par3` can now read and write a strict PAR2-style packet subset in porcelain mode. That packet
layer carries the manifest, file descriptions, and recovery blocks directly, while the raw-shard
mode remains available when you want slot-level control.

Because the repository still uses its native shard codec, the envelope layer is primarily intended
for round trips through this codebase rather than claiming bit-for-bit parity compatibility with
every legacy PAR2 implementation.

If you need the previous slot-oriented workflow, split the original file into fixed-size raw blocks
that match `shard_size`, then copy each block into the `shard_<slot>.bin` naming scheme expected by
the raw CLI and Worker paths:

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
packet-aware tool. Once those payloads are unwrapped, copy each one into the matching recovery
slot file that the raw-shard workflow should see:

```bash
cat ./unwrapped/recovery_000.bin > ./shards/shard_4.bin
cat ./unwrapped/recovery_001.bin > ./shards/shard_5.bin
```

After both sides are in raw shard form, `par3 create` and `par3 repair` work on them directly.

## Example worker

`examples/fetch_encode_repair_gzip.js` is a self-contained edge example that imports `_worker.js`
as an internal microservice. It demonstrates one fully streaming chain:

- fetch a remote asset once
- split it into fixed-size original shards
- stream every original shard into `POST /` to generate recovery shards
- intentionally omit the last one or two original shards from `PATCH /`
- stream the surviving originals plus generated recovery shards into `PATCH /`
- write the rebuilt tail directly into `CompressionStream("gzip")`

Because the example chooses its shard layout up front, the upstream response must include a
`Content-Length` header.

To run the example locally, build the wasm package first and then point Wrangler at the example
module instead of the default `_worker.js` entrypoint from `wrangler.toml`:

```bash
npm run build:wasm
npx wrangler dev ./examples/fetch_encode_repair_gzip.js --local
```

Open Wrangler's local URL, submit a remote asset URL, and the example will stream back a gzip file
whose tail was reconstructed through the repair path. The response also includes
`x-demo-dropped-shards` so you can see which original shard indexes were intentionally omitted.

## Project layout

- `src/lib.rs`: Rust codec core and wasm bindings
- `lib/envelope.js`: strict PAR2/PAR3 packet parser/packer for the CLI porcelain layer
- `lib/multipart.js`: generic RFC 7578 multipart protocol helpers
- `lib/mod.js`: shared `Par3` shard repair/runtime abstraction
- `_worker.js`: Cloudflare Worker immediate-return POST/PATCH orchestration and stream lifetime management
- `examples/fetch_encode_repair_gzip.js`: fetch-once encode/repair/gzip example built on `_worker.js`
- `bin/main.js`: local CLI implementation and executable entrypoint
- `_worker_test.js`: Worker property-style and protocol tests
- `bin/main_test.js`: CLI behavior tests
- `lib/envelope_test.js`: packet envelope tests
- `lib/mod_test.js`: direct `Par3` class tests
- `scripts/build-wasm.sh`: wasm build helper
- `scripts/test-coverage.mjs`: built-in coverage gate for maintained JS files

## Contributing

See `CONTRIBUTING.md` for workflow details and `AGENTS.md` for repository-specific agent notes.
