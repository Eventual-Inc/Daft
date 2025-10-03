"use client";

import { SWRConfig } from "swr";
import React, { useEffect, useState } from "react";
import { QuerySummaryMap, QueriesContext } from "@/hooks/use-queries";

// ---------------------- Utils ---------------------- //

/**
 * Get the API base URL from environment variables or use default
 */
export function genUrl(path: string): string {
  // console.log("API_URL:", process.env.API_URL, window, window.location.origin);
  // // Check for API_URL (for server-side usage)
  // if (typeof window !== 'undefined' && process.env.API_URL) {
  //   return process.env.API_URL;
  // }

  // // For same-port deployment (Axum serving both frontend and API)
  // if (typeof window !== 'undefined') {
  //   return window.location.origin; // Uses current host and port
  // }

  // Default fallback for development
  const base = "http://localhost:3238";

  return new URL(path, base).toString();
}

/**
 * Build a complete API URL by appending a path to the base URL
 */
export function fetcher(
  path: string,
  options?: RequestInit
): Promise<Response> {
  const url = genUrl(path);
  console.log("fetching", url);
  return fetch(url, options).then(res => res.json());
}

// ---------------------- Server Provider ---------------------- //

export function ServerProvider({ children }: { children: React.ReactNode }) {
  const [queries, setQueries] = useState<QuerySummaryMap | null>(null);

  // TODO: Play around with useSWRSubscription again
  useEffect(() => {
    const es = new EventSource(genUrl("api/queries/subscribe"));
    es.onopen = () => {
      console.warn("Connected to queries SSE endpoint");
    };
    es.onerror = event => {
      console.error("Error subscribing to queries:", event);
    };

    es.addEventListener("initial_state", event => {
      setQueries(JSON.parse(event.data));
    });
    es.addEventListener("status_update", event => {
      const newQueries = JSON.parse(event.data);
      setQueries(prev => {
        return { ...prev, newQueries };
      });
    });

    return () => {
      console.log("Closing queries SSE endpoint");
      es.close();
    };
  }, [setQueries]);

  return (
    <SWRConfig
      value={{
        fetcher,
        refreshInterval: 5000,
        revalidateOnFocus: false,
        revalidateOnReconnect: false,
        errorRetryCount: 3,
        errorRetryInterval: 1000,
      }}
    >
      <QueriesContext value={queries}>{children}</QueriesContext>
    </SWRConfig>
  );
}
