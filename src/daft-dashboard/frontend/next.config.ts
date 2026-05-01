import type { NextConfig } from "next";

// DAFT_DASHBOARD_OFFLINE_FONT=1 swaps @/lib/font for @/lib/font.offline
// (a system-mono stub) so `next build` works without fetching from
// fonts.googleapis.com.
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
