"use client";

import { useSearchParams, useRouter } from "next/navigation";
import React from "react";
import useSWR from "swr";
import { QueryMetadata } from "@/types/queryInfo";
import mermaid from "mermaid";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { toHumanReadableDate, calculateDurationFromStartTime } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { ArrowLeft } from "lucide-react";
import QueryStatusBadge from "@/components/query-status-badge";

mermaid.initialize({
  startOnLoad: true
});

// Fetcher function for SWR
const fetcher = async (url: string): Promise<QueryMetadata> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Failed to fetch queries");
  }
  const json: QueryMetadata = await response.json();
  return json;
};

export default function QueryPage() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const queryName: string = searchParams.get("name") || "";

  const { data: queryInfo, error, isLoading } = useSWR<QueryMetadata>(
    queryName ? `http://localhost:3238/api/queries/${queryName}` : null,
    fetcher,
    {
      refreshInterval: 5000, // Refresh every 5 seconds
      revalidateOnFocus: true,
    }
  );

  if (error) {
    return (
      <div className="gap-4 space-y-4">
        <div className="text-center text-red-500">
          Failed to load query data. Please try again.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="gap-4 space-y-4">
        <div className="text-center">
          Loading query data...
        </div>
      </div>
    );
  }

  if (!queryInfo || !queryName) {
    return (
      <div className="gap-4 space-y-4">
        <div className="text-center">
          Query not found.
        </div>
      </div>
    );
  }

  return (
    <div className="gap-6 space-y-6">
      <div className="flex items-center gap-4">
        <Button 
          variant="outline" 
          size="sm" 
          onClick={() => router.push("/queries")}
          className="flex items-center gap-2"
        >
          <ArrowLeft size={16} />
          Back to Queries
        </Button>
        <h1 className="text-2xl font-bold">Query Details</h1>
      </div>
      
      <div className="lg border border-daft-border-primary">
        <Card className="border-0 shadow-none">
          <CardHeader className="pb-4">
            <CardTitle className="text-lg">Query Statistics</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-6">
              <div className="space-y-2">
                <CardDescription className="text-daft-text-muted">Query Name</CardDescription>
                <p className="text-daft-text-primary font-medium">{queryInfo.info.name}</p>
              </div>
              <div className="space-y-2">
                <CardDescription className="text-daft-text-muted">Status</CardDescription>
                <QueryStatusBadge status={queryInfo.status as "Running" | "Completed" | "Failed"} />
              </div>
              <div className="space-y-2">
                <CardDescription className="text-daft-text-muted">Start Time</CardDescription>
                <p className="text-daft-text-primary font-medium">{toHumanReadableDate(Number(queryInfo.start_time))}</p>
              </div>
              <div className="space-y-2">
                <CardDescription className="text-daft-text-muted">Duration</CardDescription>
                <p className="text-daft-text-primary font-medium">
                  {queryInfo.duration || (queryInfo.status === "Running" ? calculateDurationFromStartTime(Number(queryInfo.start_time)) : "n/a")}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
      
      <div className="lg border border-daft-border-primary">
        <Card className="border-0 shadow-none">
          <CardHeader className="pb-4">
            <CardTitle className="text-lg">Query Plan</CardTitle>
          </CardHeader>
          <CardContent className="justify-center flex py-8">
            <div className="text-daft-text-muted">Coming Soon</div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};
