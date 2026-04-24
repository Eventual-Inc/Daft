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
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { toHumanReadableDate, main, getEngineName } from "@/lib/utils";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import LoadingPage from "@/components/loading";
import { Status } from "./status";
import { ExecutingState, OperatorInfo, QueryInfo } from "./types";
import PhysicalPlanTree from "./physical-plan-tree";
import PlanVisualizer from "./plan-visualizer";
import ResultPreview from "./result-preview";
import TasksSidebar from "./tasks-sidebar";

/**
 * Query detail page component
 * Displays details for a specific query by ID using query parameters
 */
function QueryPageInner() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();
  const queryId = searchParams.get("id");
  const debug = useMemo(() => searchParams.has("debug"), [searchParams]);
  const [query, setQuery] = useState<QueryInfo | null>(null);

  const engine = query ? getEngineName(query.runner) : null;
  const isFlotilla = engine === "Flotilla";

  // Tab + tasks-sidebar state is URL-driven so deep links survive reload.
  // - `tab`: active tab id
  // - `tasks=open`: tasks sidebar visible (Flotilla + Execution tab only)
  // - `origin`: node id used as sidebar filter and/or plan highlight
  const urlTab = searchParams.get("tab");
  const urlOrigin = searchParams.get("origin");
  const originParam = urlOrigin != null ? Number(urlOrigin) : null;
  const activeTab = urlTab ?? "progress-table";
  const tasksOpen = searchParams.get("tasks") === "open";

  const updateParams = useCallback(
    (updates: Record<string, string | null>) => {
      const params = new URLSearchParams(searchParams.toString());
      for (const [k, v] of Object.entries(updates)) {
        if (v == null) params.delete(k);
        else params.set(k, v);
      }
      router.replace(`${pathname}?${params.toString()}`, { scroll: false });
    },
    [router, pathname, searchParams],
  );

  const handleTabChange = useCallback(
    (next: string) => {
      // Leaving the Execution tab clears the origin hint so a fresh return
      // doesn't carry a stale filter/highlight. Sidebar open-state persists.
      updateParams({ tab: next, origin: null });
    },
    [updateParams],
  );

  // Plan node -> open tasks sidebar, filtered to that origin.
  const handleViewTasksForNode = useCallback(
    (nodeId: number) => {
      updateParams({ tasks: "open", origin: String(nodeId) });
    },
    [updateParams],
  );

  // Toolbar button -> open tasks sidebar, no filter.
  const handleOpenTasks = useCallback(() => {
    updateParams({ tasks: "open" });
  }, [updateParams]);

  const handleCloseTasks = useCallback(() => {
    // Closing the sidebar also clears the filter so reopening starts fresh.
    updateParams({ tasks: null, origin: null });
  }, [updateParams]);

  // Task row origin link -> highlight the plan node (sidebar stays open).
  const handleSelectOrigin = useCallback(
    (nodeId: number) => {
      updateParams({ origin: String(nodeId) });
    },
    [updateParams],
  );

  const handleClearOrigin = useCallback(() => {
    updateParams({ origin: null });
  }, [updateParams]);

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
      if (debug) console.log("[debug] initial_state (query)", data);
      setQuery(data);
    });
    // TODO: Consistent ordering of statistics
    es.addEventListener("query_info", event => {
      const data: QueryInfo = JSON.parse(event.data);
      if (debug) console.log("[debug] query_info", data);
      setQuery(data);
    });
    // Merges with existing info, preserving the current status
    es.addEventListener("operator_info", event => {
      const data: Record<number, OperatorInfo> = JSON.parse(event.data);
      if (debug) console.log("[debug] operator_info", data);
      setQuery(prev => {
        if (!prev) return prev;
        if (!("exec_info" in prev.state)) return prev;

        const new_exec_info = { ...prev.state.exec_info, operators: data };
        return {
          ...prev,
          state: { ...prev.state, exec_info: new_exec_info } as typeof prev.state,
        };
      });
    });
    return () => {
      console.info("Closing query SSE endpoint");
      es.close();
    };
  }, [queryId, debug, setQuery]);

  if (!query) {
    return <LoadingPage />;
  }

  const end_sec =
    query.state.status === "Finished" || query.state.status === "Canceled" || query.state.status === "Failed"
      ? query.state.end_sec
      : query.state.status === "Dead"
        ? query.state.marked_dead_sec
        : null;

  const isActive =
    query.state.status === "Pending" ||
    query.state.status === "Optimizing" ||
    query.state.status === "Setup" ||
    query.state.status === "Executing";
  const last_heartbeat_sec = isActive || query.state.status === "Dead"
    ? query.last_heartbeat_sec
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

        <div className="w-full flex border-b border-zinc-800">
          <div className="w-1/4 border-r border-zinc-800 px-6 py-4">
            <Status
              status={query.state.status}
              start_sec={query.start_sec}
              end_sec={end_sec}
              last_heartbeat_sec={last_heartbeat_sec}
            />
          </div>
          <div className="flex-1 px-6 py-4">
            <div className="grid grid-cols-2 gap-6">
              <div className="space-y-3">
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Query ID
                  </h3>
                  <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                    {query.id}
                  </p>
                </div>
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Start Time
                  </h3>
                  <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                    {toHumanReadableDate(query.start_sec)}
                  </p>
                </div>
                <div>
                  <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                    Entrypoint
                  </h3>
                  <p className={`${main.className} text-sm font-mono break-all text-zinc-100`} title={query.entrypoint || ""}>
                    {query.entrypoint || "-"}
                  </p>
                </div>
              </div>
              <div className="space-y-3">
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    Engine
                  </h3>
                  <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                    {getEngineName(query.runner)}
                  </p>
                </div>
                <div>
                  <h3
                    className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}
                  >
                    End Time
                  </h3>
                  <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                    {end_sec ? toHumanReadableDate(end_sec) : "..."}
                  </p>
                </div>
                {query.ray_dashboard_url && (
                  <div>
                    <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                      Ray Dashboard
                    </h3>
                    <a
                      href={query.ray_dashboard_url.startsWith("http") ? query.ray_dashboard_url : `http://${query.ray_dashboard_url}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className={`${main.className} text-sm font-mono text-blue-500 hover:underline`}
                    >
                      Open in Ray
                    </a>
                  </div>
                )}
              </div>
              {(query.daft_version || query.python_version || query.ray_version) && (
              <div className="space-y-3">
                {query.daft_version && (
                  <div>
                    <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                      Daft Version
                    </h3>
                    <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                      {query.daft_version}
                    </p>
                  </div>
                )}
                {query.python_version && (
                  <div>
                    <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                      Python Version
                    </h3>
                    <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                      {query.python_version}
                    </p>
                  </div>
                )}
                {query.ray_version && (
                  <div>
                    <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                      Ray Version
                    </h3>
                    <p className={`${main.className} text-lg font-mono text-zinc-100`}>
                      {query.ray_version}
                    </p>
                  </div>
                )}
              </div>
              )}
            </div>
          </div>
        </div>
        <div className="w-full h-[20px] flex"></div>
      </div>

      {/* Scrollable Content Section */}
      <div className="flex-1">
        <Tabs
          value={activeTab}
          onValueChange={handleTabChange}
          className="w-full h-full flex flex-col"
        >
          <TabsList className={`grid w-full flex-shrink-0 ${engine === "Swordfish" ? "grid-cols-4" : "grid-cols-3"}`}>
            <TabsTrigger
              value="progress-table"
              disabled={
                query.state.status === "Pending" ||
                query.state.status === "Optimizing" ||
                query.state.status === "Setup"
              }
            >
              Execution
            </TabsTrigger>
            <TabsTrigger
              value="optimized-plan"
              disabled={!("plan_info" in query.state && query.state.plan_info)}
            >
              Optimized Plan
            </TabsTrigger>
            <TabsTrigger value="unoptimized-plan">Unoptimized Plan</TabsTrigger>
            {/* Results preview only supported for Swordfish for now (#6559) */}
            {engine === "Swordfish" && (
              <TabsTrigger
                value="results"
                disabled={query.state.status !== "Finished"}
              >
                Results
              </TabsTrigger>
            )}
          </TabsList>

          <TabsContent
            value="progress-table"
            className="mt-4 flex-1 overflow-hidden"
          >
            <div className="bg-zinc-900 h-full flex">
              {"exec_info" in query.state && query.state.exec_info !== null ? (
                <>
                  <div className="flex-1 min-w-0 h-full">
                    <PhysicalPlanTree
                      exec_state={query.state as ExecutingState}
                      highlightedNodeId={originParam}
                      onViewTasks={isFlotilla ? handleViewTasksForNode : undefined}
                      tasksOpen={isFlotilla && tasksOpen}
                      onOpenTasks={isFlotilla ? handleOpenTasks : undefined}
                    />
                  </div>
                  {isFlotilla && tasksOpen && queryId && (
                    <div className="w-1/2 min-w-[480px] max-w-[900px] flex-shrink-0 border-l border-zinc-800 h-full">
                      <TasksSidebar
                        exec_state={query.state as ExecutingState}
                        originFilter={originParam}
                        onClearFilter={handleClearOrigin}
                        onSelectOrigin={handleSelectOrigin}
                        onClose={handleCloseTasks}
                      />
                    </div>
                  )}
                </>
              ) : (
                <div className="p-8 text-center w-full">
                  <p className={`${main.className} text-zinc-400`}>
                    No execution data available
                  </p>
                </div>
              )}
            </div>
          </TabsContent>

          <TabsContent
            value="optimized-plan"
            className="mt-4 flex-1 overflow-auto"
          >
            {query.state.status === "Pending" ? (
              <div className="bg-zinc-900 p-4">
                <p className={`${main.className} text-zinc-400`}>
                  Plan not yet optimized
                </p>
              </div>
            ) : (
              <PlanVisualizer
                planJson={
                  "plan_info" in query.state && query.state.plan_info
                    ? query.state.plan_info.optimized_plan
                    : ""
                }
              />
            )}
          </TabsContent>

          <TabsContent
            value="unoptimized-plan"
            className="mt-4 flex-1 overflow-auto"
          >
            <PlanVisualizer planJson={query.unoptimized_plan} />
          </TabsContent>

          <TabsContent
            value="results"
            className="mt-4 flex-1 overflow-auto"
          >
            <div className="bg-zinc-900 h-full">
              {queryId && <ResultPreview queryId={queryId} />}
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
