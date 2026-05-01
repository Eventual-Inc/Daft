# daft-dashboard agent notes

See [README.md](./README.md) for general build requirements (Node.js, npm) and
architecture. This file documents pitfalls specific to agents running in
sandboxed or offline build environments.

## Offline / sandboxed builds

The dashboard frontend imports `Geist_Mono` from `next/font/google` (in
`frontend/src/lib/font.ts`), which causes Next.js to fetch the font from
`fonts.googleapis.com` during `npm run build`. Sandboxed containers without
egress to that host fail this fetch and abort the Next.js build, which in
turn fails `make build` for the whole Daft project.

**If you are running in a sandboxed container (e.g. Claude Code on the web),
set `DAFT_DASHBOARD_OFFLINE_FONT=1` for every build:**

```sh
export DAFT_DASHBOARD_OFFLINE_FONT=1
make build
```

This makes `next.config.ts` swap `@/lib/font` for `@/lib/font.offline` (a
system-mono stub) via Turbopack `resolveAlias`. `next build` then runs
end-to-end, so TS/React code still gets typechecked — only the served font
is degraded to the system monospace fallback. Online builds without the env
var are unchanged and serve real Geist Mono.

### If even npm can't reach the registry

Set `DAFT_DASHBOARD_SKIP_BUILD=1` to skip the frontend build entirely. Daft
itself builds fine without the bundled dashboard assets; only the in-process
dashboard UI is unavailable.
