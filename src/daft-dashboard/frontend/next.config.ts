import type { NextConfig } from "next";

// When DAFT_DASHBOARD_OFFLINE_FONT=1, swap @/lib/font (which calls
// next/font/google and fetches Geist Mono at build time) for
// @/lib/font.offline (a system-mono stub). build.rs sets this env var
// automatically as a retry when the first `next build` fails — the typical
// cause is a sandboxed container that can't reach fonts.googleapis.com. This
// lets `next build` finish so TS/React code still gets typechecked even
// though the served UI loses Geist Mono.
const offlineFont = process.env.DAFT_DASHBOARD_OFFLINE_FONT === "1";

const nextConfig: NextConfig = {
  output: "export",
  reactCompiler: true,
  turbopack: offlineFont
    ? {
        resolveAlias: {
          "@/lib/font": "./src/lib/font.offline.ts",
        },
      }
    : undefined,
};

export default nextConfig;
