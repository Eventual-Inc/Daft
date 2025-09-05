"use client";

import React from "react";
import { Wifi, WifiOff } from "lucide-react";
import useSWR from "swr";

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
    "http://localhost:3238/api/ping",
    connectionFetcher,
    {
      refreshInterval: 10000, // Refresh every 10 seconds
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
    <div className="flex items-center space-x-3">
      {connected ? (
        <>
          <Wifi className="h-5 w-5 text-green-500" />
          <span className="text-base text-green-500 font-medium">Connected</span>
        </>
      ) : (
        <>
          <WifiOff className="h-5 w-5 text-red-500" />
          <span className="text-base text-red-500 font-medium">Disconnected</span>
        </>
      )}
    </div>
  );
}
