# daft-dashboard

## Requirements

- Node.js ≥22
- npm ≥11.10.0 — required for the `min-release-age` supply-chain setting in `frontend/.npmrc` to take effect. Older npm versions silently ignore it, leaving local `npm install <pkg>` unprotected. Upgrade with `npm install -g npm@latest`.

## Build

The frontend (Next.js, under `frontend/`) is statically embedded into the Rust binary at compile time via `include_dir!` (see `src/assets.rs`). There is no separate dev server — **any frontend change requires `make build`** (or `make build-release`) from the repo root to take effect.

## Architecture

- **Backend**: Axum server (`src/`) exposes SSE streams and REST endpoints under `/client/`.
- **Frontend**: Next.js app (`frontend/`) compiled to static assets, embedded in the binary.
- **State**: `DashboardState` (in `src/state.rs`) holds query info in a `DashMap` (in memory, currently no persistence).
