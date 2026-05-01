import type { NextConfig } from "next";

// When DAFT_DASHBOARD_OFFLINE_FONT=1, swap @/lib/font (which calls
// next/font/google and fetches Geist Mono at build time) for
// @/lib/font.offline (a system-mono stub). Set this in sandboxed containers
// that can't reach fonts.googleapis.com so `next build` finishes and
// TS/React code still gets typechecked. See src/daft-dashboard/AGENTS.md.
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
