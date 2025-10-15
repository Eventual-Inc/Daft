"use client";

import { genApiUrl } from "@/components/server-provider";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useEffect, useState } from "react";
import { toHumanReadableDate, main } from "@/lib/utils";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import LoadingPage from "@/components/loading";
import { OperatorInfo, QueryInfo } from "../query/types";
import Logo from "@/components/navbar/logo";

/**
 * Query detail page component
 * Displays details for a specific query by ID using query parameters
 */
function NotebookPageInner() {
  const searchParams = useSearchParams();
  const queryId = searchParams.get("id");
  const [query, setQuery] = useState<QueryInfo | null>(null);

  useEffect(() => {
    const es = new EventSource(genApiUrl(`/client/query/${queryId}/subscribe`));
    es.onopen = () => {
      console.info("Connected to query SSE endpoint");
    };
    es.onerror = event => {
      console.error("Error subscribing to query:", event);
    };
    // These overwrite
    es.addEventListener("initial_state", event => {
      const data: QueryInfo = JSON.parse(event.data);
      console.log("initial_state", data);
      setQuery(data);
    });
    // TODO: Consistent ordering of statistics
    es.addEventListener("query_info", event => {
      const data: QueryInfo = JSON.parse(event.data);
      console.log("query_info", data);
      setQuery(data);
    });
    // Merges with existing info
    es.addEventListener("operator_info", event => {
      setQuery(prev => {
        if (!prev) return prev;

        const plan_info = "plan_info" in prev.state ? prev.state.plan_info : undefined;
        const old_exec_info = "exec_info" in prev.state ? prev.state.exec_info : undefined;
        const data: Record<number, OperatorInfo> = JSON.parse(event.data).update;

        console.log("operator_info", data)
        if (plan_info && old_exec_info && old_exec_info.exec_start_sec !== undefined) {
          const new_exec_info = { ...old_exec_info, operators: data };
          const new_state = {
            status: "Executing" as const,
            plan_info: plan_info,
            exec_info: new_exec_info,
          };
          return { ...prev, state: new_state };
        }

        return prev;
      });
    });
    return () => {
      console.info("Closing query SSE endpoint");
      es.close();
    };
  }, [queryId, setQuery]);

  if (!query) {
    return <LoadingPage />;
  }

  const end_sec = query.state.status === "Finished" ? query.state.end_sec : null;

  return (
    <div className="w-full h-full flex flex-col">
      <div className="w-full h-[150px] flex">
        <Logo />
        <div className="flex-1">TEST</div>
        
      </div>
    </div>
  );
}

export default function NotebookPage() {
  return (
    <Suspense fallback={<LoadingPage />}>
      <NotebookPageInner />
    </Suspense>
  );
}
