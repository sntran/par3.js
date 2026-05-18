# Contributing

## Prerequisites

- Node.js 26.1+
- Rust stable
- `wasm-pack`

## Setup

```bash
npm install
```

## Standard workflow

1. Write or update tests first.
2. Make the smallest change that satisfies the failing test.
3. Run the narrowest relevant validation command.
4. Finish with `npm test`.

## Commands

```bash
npm run build
npm test
npm run test:coverage
cargo test --lib
```

The installed CLI entrypoint is `par3`, and the repository-local executable shim is
`node ./bin/main.js`.

## Testing expectations

- Rust math changes should include unit tests plus `proptest` coverage where practical.
- Worker behavior changes should be covered in `_worker_test.js` using Node's built-in
  test runner.
- CLI changes should be covered in `bin/main_test.js` and should prefer built-in Node facilities
  already available in the 26.x line when they simplify cleanup or mocking.
- Shared `Par3` runtime changes should be covered directly in `lib/mod_test.js`.
- `npm run test:coverage` must keep `_worker.js`, `bin/main.js`, `lib/mod.js`, and
  `lib/multipart.js` at 100% line, branch, and function coverage.

## Design constraints

- Keep the Worker streaming. Avoid buffering entire multipart bodies.
- Keep Worker-facing code compatible with Cloudflare's `nodejs_compat` layer.
- Keep shared shard-layout and arena behavior in `lib/mod.js` instead of duplicating it between
  the Worker and `bin/main.js`.
- Preserve the distinction between:
  - every slot the codec must treat as missing, and
  - the subset of repaired shards the caller wants returned or written.
- Keep wasm memory access explicit and local so detached/stale buffer bugs stay visible in tests.

## Documentation expectations

- Update `README.md` when the public CLI, Worker contract, or build/test workflow changes.
- Add doc comments to exported Rust and JS modules when behavior becomes less obvious.

## Before sending a change

- Ensure `npm test` passes.
- Ensure new source behavior has matching tests.
- Keep generated `pkg/` artifacts consistent with the current Rust code if you changed wasm-facing
  exports.
