// Swapped in for ./font.ts via Turbopack resolveAlias in next.config.ts when
// DAFT_DASHBOARD_OFFLINE_FONT=1. Set this env var in sandboxed containers
// where fonts.googleapis.com is unreachable so `next build` (and therefore
// TypeScript/React typechecking) can still complete offline.
export const main = {
  className: "font-mono",
};
