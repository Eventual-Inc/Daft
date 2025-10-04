import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  experimental: {
    reactCompiler: true,
  },
};

export default nextConfig;
