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
import { Status } from "./status";
import { ExecutingState, OperatorInfo, QueryInfo } from "./types";
import ProgressTable from "./progress-table";


function formatPlanJSON(plan: string) {
  const parsedPlan = JSON.parse(plan);
  return JSON.stringify(parsedPlan, null, 2);
}

/**
 * Query detail page component
 * Displays details for a specific query by ID using query parameters
 */
function QueryPageInner() {
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
      setQuery(data);
    });
    // TODO: Consistent ordering of statistics
    es.addEventListener("query_info", event => {
      const data: QueryInfo = JSON.parse(event.data);
      setQuery(data);
    });
    // Merges with existing info
    es.addEventListener("operator_info", event => {
      setQuery(prev => {
        if (!prev) return prev;

        const plan_info =
          "plan_info" in prev.state ? prev.state.plan_info : undefined;
        const old_exec_info =
          "exec_info" in prev.state ? prev.state.exec_info : undefined;
        const data: Record<number, OperatorInfo> = JSON.parse(event.data);

        if (
          plan_info &&
          old_exec_info &&
          old_exec_info.exec_start_sec !== undefined
        ) {
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

  const end_sec =
    query.state.status === "Finished" || query.state.status === "Canceled" || query.state.status === "Failed"
      ? query.state.end_sec
      : null;

  return (
    <div className="h-full flex flex-col">
      {/* Fixed Header Section */}
      <div className="flex-shrink-0 space-y-4">
        <Breadcrumb>
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink
                asChild
                className="text-lg font-mono text-zinc-400 hover:text-white"
              >
                <Link href="/queries">All Queries</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink asChild className="text-lg font-mono font-bold">
                <p>Query {queryId}</p>
              </BreadcrumbLink>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>

        <div className="w-full h-[150px] flex">
          <div className="w-1/4 h-full border-r px-6 py-4">
            <Status
              status={query.state.status}
              start_sec={query.start_sec}
              end_sec={end_sec}
            />
          </div>
          <div className="flex-1 h-full px-6 py-4">
            <div className="grid grid-cols-2 gap-6 h-full">
              <div className="space-y-3">
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Query ID
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    {query.id}
                  </p>
                </div>
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Start Time
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    {toHumanReadableDate(query.start_sec)}
                  </p>
                </div>
              </div>
              <div className="space-y-3">
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Runner
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    Native (Swordfish)
                  </p>
                </div>
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    End Time
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    {end_sec ? toHumanReadableDate(end_sec) : "..."}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className="w-full h-[20px] flex"></div>
      </div>

      {/* Scrollable Content Section */}
      <div className="flex-1">
        <Tabs
          defaultValue="progress-table"
          className="w-full h-full flex flex-col"
        >
          <TabsList className="grid w-full grid-cols-4 flex-shrink-0">
            <TabsTrigger
              value="progress-table"
              disabled={
                query.state.status === "Pending" ||
                query.state.status === "Optimizing" ||
                query.state.status === "Setup"
              }
            >
              Progress Table
            </TabsTrigger>
            <TabsTrigger
              value="optimized-plan"
              disabled={!("plan_info" in query.state)}
            >
              Optimized Plan
            </TabsTrigger>
            <TabsTrigger value="unoptimized-plan">Unoptimized Plan</TabsTrigger>
          </TabsList>

          <TabsContent
            value="progress-table"
            className="mt-4 flex-1 overflow-auto"
          >
            <div className="bg-zinc-900 h-full">
              {query.state.status === "Pending" ||
                query.state.status === "Optimizing" ? (
                <div className="p-8 text-center">
                  <p className={`${main.className} text-zinc-400`}>
                    Execution not yet started
                  </p>
                </div>
              ) : (
                <ProgressTable exec_state={query.state as ExecutingState} />
              )}
            </div>
          </TabsContent>

          <TabsContent
            value="optimized-plan"
            className="mt-4 flex-1 overflow-auto"
          >
            <div className="bg-zinc-900 p-4">
              {query.state.status === "Pending" ? (
                <p className={`${main.className} text-zinc-400`}>
                  Plan not yet optimized
                </p>
              ) : (
                <pre
                  className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap`}
                >
                  {"plan_info" in query.state
                    ? formatPlanJSON(query.state.plan_info.optimized_plan)
                    : "No optimized plan available"}
                </pre>
              )}
            </div>
          </TabsContent>

          <TabsContent
            value="unoptimized-plan"
            className="mt-4 flex-1 overflow-auto"
          >
            <div className="bg-zinc-900 p-4">
              <pre
                className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap`}
              >
                {formatPlanJSON(query.unoptimized_plan)}
              </pre>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}

export default function QueryPage() {
  return (
    <Suspense fallback={<LoadingPage />}>
      <QueryPageInner />
    </Suspense>
  );
}
