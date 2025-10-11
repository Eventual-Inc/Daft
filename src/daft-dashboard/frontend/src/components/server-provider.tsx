"use client";

import { SWRConfig } from "swr";
import React, { useEffect, useState } from "react";
import {
  QuerySummaryMap,
  QueriesContext,
  QuerySummary,
} from "@/hooks/use-queries";

// ---------------------- Utils ---------------------- //

/**
 * Get the API base URL from environment variables or use default
 */
export function genApiUrl(path: string): string {
  let base;

  // For same-port deployment (Axum serving both frontend and API)
  if (process.env.NODE_ENV !== "development" && typeof window !== "undefined") {
    base = window.location.origin; // Uses current host and port
  } else {
    // Default fallback for development
    base = "http://localhost:3238";
  }

  return new URL(path, base).toString();
}

/**
 * Build a complete API URL by appending a path to the base URL
 */
export function fetcher(
  path: string,
  options?: RequestInit
): Promise<Response> {
  const url = genApiUrl(path);
  return fetch(url, options).then(res => res.json());
}

// ---------------------- Server Provider ---------------------- //

export function ServerProvider({ children }: { children: React.ReactNode }) {
  const [queries, setQueries] = useState<QuerySummaryMap | null>(null);

  // TODO: Play around with useSWRSubscription again
  useEffect(() => {
    const es = new EventSource(genApiUrl("/client/queries/subscribe"));
    es.onopen = () => {
      console.warn("Connected to queries SSE endpoint");
    };
    es.onerror = event => {
      console.error("Error subscribing to queries:", event);
    };

    es.addEventListener("initial_state", event => {
      const allQueries: QuerySummaryMap = JSON.parse(event.data);
      setQueries(allQueries);
    });
    es.addEventListener("status_update", event => {
      const queryUpdate: QuerySummary = JSON.parse(event.data);
      setQueries(prev => {
        return { ...prev, [queryUpdate.id]: queryUpdate };
      });
    });

    return () => {
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
