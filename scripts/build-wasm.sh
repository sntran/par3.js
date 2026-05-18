#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack is required to build the WebAssembly package" >&2
  echo "Install it with: cargo install wasm-pack" >&2
  exit 1
fi

cd "$ROOT_DIR"

rustup target add wasm32-unknown-unknown >/dev/null
RUSTFLAGS="-C target-feature=+simd128" wasm-pack build \
  --target bundler \
  --out-dir pkg \
  --release \
  --no-typescript