// Swapped in for ./font.ts via webpack NormalModuleReplacementPlugin in
// next.config.ts when DAFT_DASHBOARD_OFFLINE_FONT=1. Used by build.rs as a
// fallback when fonts.googleapis.com is unreachable so `next build` (and
// therefore TypeScript/React typechecking) can still complete offline.
export const main = {
  className: "font-mono",
};
