# Agent Notes

This repository is intentionally set up for small, validated changes.

## First checks

- Run `npm test` before and after behavior changes.
- Rebuild wasm with `npm run build` when you touch Rust or wasm-facing JS.
- Use `npm run test:coverage` for enforced JS coverage on `_worker.js`, `bin/main.js`, `lib/mod.js`, and `lib/multipart.js`.

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
- The wasm build now includes a repository-local `wasm32-simd128` engine layered through
  `reed-solomon-simd`'s rate API; preserve the existing chunk alignment rules when changing the
  shared JS runtime or Rust chunked bindings.

## Preferred validation order

1. `cargo test --lib` for Rust-only changes
2. `npm test` for end-to-end repo validation
3. `npm run test:coverage` when you extend JS tests or touch `_worker.js`, `bin/main.js`, `lib/mod.js`, or `lib/multipart.js`

## High-value files

- `src/lib.rs`: pointer registry, shard arena management, codec entrypoints
- `lib/multipart.js`: generic RFC 7578 multipart decoder/encoder utilities shared by the Worker tests and request path
- `lib/mod.js`: shared `Par3` layout validation, arena lifecycle, and repair logic
- `_worker.js`: immediate-return Worker orchestration, multipart ingestion, threshold logic, and response stream lifetime management
- `bin/main.js`: local CLI implementation over the shared `Par3` runtime
- `_worker_test.js`: Worker behavior and property-style tests
- `bin/main_test.js`: CLI behavior and validation coverage
- `lib/mod_test.js`: shared-library coverage for `Par3`
