# Agent Notes

This repository is intentionally set up for small, validated changes.

## First checks

- Run `npm test` before and after behavior changes.
- Rebuild wasm with `npm run build` when you touch Rust or wasm-facing JS.
- Use `npm run test:coverage` for enforced JS coverage on `_worker.js`, `bin/main.js`, and `lib/mod.js`.

## Repository-specific rules

- JS tests use Node's built-in `node:test` runner. Do not add Vitest back.
- Local development targets Node.js 26.x; prefer modern built-in Node features that do not leak
  beyond what the Worker `nodejs_compat` layer supports.
- The generated wasm package auto-initializes on import. There is no default `init()` export.
- Read linear memory from `pkg/par3_bg.wasm`, not from `pkg/par3.js`.
- Worker repairs must mark every unreceived slot as missing, even when only a subset of repaired
  shards are returned to the client.
- Worker requests must declare `recovery_count` or `total_shard_count`; the Worker no longer grows
  layouts on the fly.
- Keep the Worker request path streaming. Do not rewrite it to buffer the full multipart body.
- Shared shard repair state lives in `lib/mod.js` via the `Par3` class. Prefer changing that
  shared layer before duplicating behavior in `_worker.js` or `bin/main.js`.
- The CLI implementation and executable entrypoint both live in `bin/main.js`.
- `reed-solomon-simd` 3.1.0 has optimized SSSE3/AVX2/Neon engines but no wasm SIMD backend; the
  wasm build currently runs the scalar engine even though `scripts/build-wasm.sh` keeps
  `-C target-feature=+simd128` enabled for future compatibility.

## Preferred validation order

1. `cargo test --lib` for Rust-only changes
2. `npm test` for end-to-end repo validation
3. `npm run test:coverage` when you extend JS tests or coverage targets

## High-value files

- `src/lib.rs`: pointer registry, shard arena management, codec entrypoints
- `lib/mod.js`: shared `Par3` layout validation, arena lifecycle, and repair logic
- `_worker.js`: request parsing, threshold logic, multipart response generation
- `bin/main.js`: local CLI implementation over the shared `Par3` runtime
- `_worker_test.js`: Worker behavior and property-style tests
- `bin/main_test.js`: CLI behavior and validation coverage
- `lib/mod_test.js`: shared-library coverage for `Par3`
