# daft-dashboard agent notes

See [README.md](./README.md) for general build requirements (Node.js, npm) and
architecture. This file documents pitfalls specific to agents running in
sandboxed or offline build environments.

## Offline / sandboxed builds

The dashboard frontend imports `Geist_Mono` from `next/font/google` (in
`frontend/src/lib/font.ts`), which causes Next.js to fetch the font from
`fonts.googleapis.com` during `npm run build`. Sandboxed containers without
egress to that host fail this fetch and abort the Next.js build.

`build.rs` handles this automatically: if the first `next build` fails, it
retries once with `DAFT_DASHBOARD_OFFLINE_FONT=1`, which makes
`next.config.ts` swap `./lib/font` for `./lib/font.offline` (a system-mono
stub) via `NormalModuleReplacementPlugin`. The retry runs `next build`
end-to-end, so the TS/React code still gets typechecked — only the served
font is degraded to the system monospace fallback.

Net effect: `make build` succeeds in offline containers and still typechecks
the dashboard frontend. The dashboard UI, when served, will render in the
system mono font instead of Geist Mono.

### Forcing the offline-font path explicitly

```sh
DAFT_DASHBOARD_OFFLINE_FONT=1 make build
```

Useful if you want to skip the doomed first attempt in a known-offline
environment.

### Skipping the dashboard build entirely

If even `npm ci` can't reach the npm registry, the retry won't help.
Set `DAFT_DASHBOARD_SKIP_BUILD=1` to skip the frontend build entirely; Daft
itself builds fine without the bundled dashboard assets, only the in-process
dashboard UI is unavailable.

```sh
DAFT_DASHBOARD_SKIP_BUILD=1 make build
```

Release builds (`build-release`, `build-whl`) still hard-fail on a failed
frontend build by design — the offline-font retry applies to them too, but
both attempts must succeed.
