"use client";

import React from "react";
import { Wifi, WifiOff } from "lucide-react";
import useSWR from "swr";
import { genApiUrl } from "../server-provider";

// Fetcher function for SWR
const connectionFetcher = async (url: string): Promise<boolean> => {
  try {
    const response = await fetch(url, {
      method: "HEAD",
      signal: AbortSignal.timeout(5000), // 5 second timeout
    });
    return response.ok;
  } catch {
    return false;
  }
};

export function ConnectionStatus() {
  const { data: isConnected, isLoading } = useSWR<boolean>(
    genApiUrl("/api/ping"),
    connectionFetcher,
    {
      refreshInterval: 3000, // Refresh every 3 seconds
      revalidateOnFocus: true,
      revalidateOnReconnect: true,
      dedupingInterval: 5000, // Dedupe requests within 5 seconds
      errorRetryCount: 3,
      errorRetryInterval: 5000,
    }
  );

  // Don't show anything while loading initially
  if (isLoading && isConnected === undefined) {
    return null;
  }

  // Determine connection status: true if connected, false if error or disconnected
  const connected = isConnected === true;

  return (
    <div
      className={`h-full flex items-center justify-center space-x-4 px-6 ${
        connected ? "bg-green-600" : "bg-red-500"
      }`}
    >
      {connected ? (
        <>
          <Wifi className="h-5 w-5 text-white" strokeWidth={3} />
          <span className="text-white font-mono font-bold">Connected</span>
        </>
      ) : (
        <>
          <WifiOff className="h-5 w-5 text-white" strokeWidth={3} />
          <span className="text-white font-mono font-bold">Disconnected</span>
        </>
      )}
    </div>
  );
}
