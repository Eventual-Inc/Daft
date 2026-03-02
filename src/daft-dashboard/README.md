# daft-dashboard

## Build

The frontend (Next.js, under `frontend/`) is statically embedded into the Rust binary at compile time via `include_dir!` (see `src/assets.rs`). There is no separate dev server — **any frontend change requires `make build`** (or `make build-release`) from the repo root to take effect.

## Architecture

- **Backend**: Axum server (`src/`) exposes SSE streams and REST endpoints under `/client/`.
- **Frontend**: Next.js app (`frontend/`) compiled to static assets, embedded in the binary.
- **State**: `DashboardState` (in `src/state.rs`) holds query info in a `DashMap`. Query results (`RecordBatch`) are stored in `QueryState::Finished` but excluded from serialization (`#[serde(skip_serializing)]`) — served separately via `/client/query/{id}/results`.
