# daft-dashboard agent notes

## Offline / sandboxed builds

The dashboard frontend (`frontend/`) is a Next.js app. `frontend/src/lib/utils.ts`
imports `Geist_Mono` from `next/font/google`, which causes Next.js to fetch the
font from `fonts.googleapis.com` during `npm run build`. In an offline or
network-restricted container that fetch fails and the Next.js build aborts.

`build.rs` invokes `npm run build` as part of `cargo build` for this crate, so a
failed font fetch will cause `make build` for the whole Daft project to fail
with a "Frontend asset build failed" error.

### Recommended workaround in sandboxed agents

Set `DAFT_DASHBOARD_SKIP_BUILD=1` in the build environment. This makes
`build.rs` skip the frontend build entirely. Daft itself builds fine without
the dashboard assets; only the in-process dashboard UI is unavailable, which
agents and CI typically do not need.

```sh
export DAFT_DASHBOARD_SKIP_BUILD=1
make build
```

For Claude Code on the web, add this to the session start hook or container
setup so it is set for every `make build` invocation.

### Behavior without the env var

`build.rs` still degrades gracefully in debug builds: if `npm ci` or
`npm run build` fails, it emits a `cargo:warning` and returns successfully
rather than panicking. So `make build` should now succeed in offline
containers even without `DAFT_DASHBOARD_SKIP_BUILD`, but you will lose the
bundled dashboard assets and see a warning. Release builds (`build-release`,
`build-whl`) still hard-fail on a failed frontend build by design.

### Fixing properly (online dev machines)

If you are an agent with network access and want to actually produce the
dashboard assets:

```sh
cd src/daft-dashboard/frontend
npm ci
npm run build
```

After the first successful build, Next.js caches the Google font under
`frontend/.next/cache/`, so subsequent builds work even offline.

### Permanent fix (not yet applied)

The cleanest long-term fix is to replace `next/font/google` in
`frontend/src/lib/utils.ts` with `next/font/local` and commit the Geist Mono
`.woff2` files into the repo. That removes the network dependency entirely.
This is intentionally not done here to keep the change minimal, but it is the
right move if offline builds become a recurring pain point.
