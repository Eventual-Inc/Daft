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
import TasksSidebar from "./tasks-sidebar";

const TAB_TRIGGER_CLS = "rounded-none bg-transparent px-4 py-2.5 text-sm font-medium text-zinc-400 shadow-none transition-colors hover:text-zinc-100 data-[state=active]:bg-transparent data-[state=active]:text-white data-[state=active]:shadow-none data-[state=active]:border-b-2 data-[state=active]:border-white data-[state=active]:-mb-px disabled:opacity-30";

const MetaField = ({
  label,
  value,
  href,
  mono,
  title,
}: {
  label: string;
  value: string;
  href?: string;
  mono?: boolean;
  title?: string;
}) => (
  <div className="min-w-0">
    <p className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500 leading-none mb-1`}>
      {label}
    </p>
    {href ? (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className={`${main.className} text-base ${mono ? "font-mono" : ""} text-blue-400 hover:underline`}
      >
        {value}
      </a>
    ) : (
      <p
        className={`${main.className} text-base ${mono ? "font-mono" : ""} text-zinc-100`}
        title={title}
      >
        {value}
      </p>
    )}
  </div>
);

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
  // - `node`: node id used as sidebar filter (containment in node_ids) and
  //   sticky plan highlight.
  const urlTab = searchParams.get("tab");
  const urlNode = searchParams.get("node");
  const nodeFilter = urlNode != null ? Number(urlNode) : null;
  const activeTab = urlTab ?? "progress-table";
  const tasksOpen = searchParams.get("tasks") === "open";

  // Transient hover preview from the sidebar — not URL-backed (hover would
  // thrash the URL). Drives the amber preview ring on the plan tree.
  // Hovering a task highlights *all* nodes in its node_ids chain, so this
  // genuinely needs to be a set; the click filter is a single id (nodeFilter).
  const [hoveredNodeIds, setHoveredNodeIds] = useState<ReadonlySet<number> | null>(null);

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
      // Leaving the Execution tab clears the node hint so a fresh return
      // doesn't carry a stale filter/highlight. Sidebar open-state persists.
      updateParams({ tab: next, node: null });
    },
    [updateParams],
  );

  // Plan node -> open tasks sidebar, filtered to that node.
  const handleViewTasksForNode = useCallback(
    (nodeId: number) => {
      updateParams({ tasks: "open", node: String(nodeId) });
    },
    [updateParams],
  );

  // Toolbar button -> open tasks sidebar, no filter.
  const handleOpenTasks = useCallback(() => {
    updateParams({ tasks: "open" });
  }, [updateParams]);

  const handleCloseTasks = useCallback(() => {
    // Closing the sidebar also clears the filter so reopening starts fresh.
    updateParams({ tasks: null, node: null });
  }, [updateParams]);

  // Task row chip click -> filter sidebar to tasks containing this node id.
  const handleSelectNode = useCallback(
    (nodeId: number) => {
      updateParams({ node: String(nodeId) });
    },
    [updateParams],
  );

  const handleClearNodeFilter = useCallback(() => {
    updateParams({ node: null });
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
      {/* Compact Header */}
      <div className="flex-shrink-0">
        <div className="px-6 pt-0 pb-1">
          <Breadcrumb>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink
                  asChild
                  className="text-xs font-mono text-zinc-500 hover:text-white"
                >
                  <Link href="/queries">All Queries</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbLink asChild className="text-xs font-mono text-zinc-300">
                  <p>Query {queryId}</p>
                </BreadcrumbLink>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>
        </div>

        <div className="w-full flex items-stretch">
          {/* Status — fixed width so the divider never shifts during execution */}
          <div className="w-52 flex-shrink-0 px-6 py-5 flex items-center justify-start">
            <Status
              status={query.state.status}
              start_sec={query.start_sec}
              end_sec={end_sec}
              last_heartbeat_sec={last_heartbeat_sec}
              errorMessage={query.state.status === "Failed" ? query.state.message : undefined}
            />
          </div>

          <div className="w-px bg-zinc-800 my-4 flex-shrink-0" />

          {/* Metadata — 4 columns, 2 rows */}
          <div className="flex-1 px-6 py-5 grid grid-cols-4 gap-x-8 gap-y-3 content-center overflow-hidden">
            {/* Row 1 */}
            <MetaField label="Query ID" value={query.id} mono />
            <MetaField label="Engine" value={getEngineName(query.runner)} mono />
            <MetaField label="Start Time" value={toHumanReadableDate(query.start_sec)} mono />
            <MetaField label="End Time" value={end_sec ? toHumanReadableDate(end_sec) : "—"} mono />

            {/* Row 2 */}
            <MetaField label="Entrypoint" value={query.entrypoint || "—"} mono title={query.entrypoint} />
            <MetaField label="Daft Version" value={query.daft_version || "—"} mono />
            {query.ray_dashboard_url ? (
              <MetaField
                label="Ray Dashboard"
                href={query.ray_dashboard_url.startsWith("http") ? query.ray_dashboard_url : `http://${query.ray_dashboard_url}`}
                value="Open in Ray"
              />
            ) : (
              <div />
            )}
            {(query.python_version || query.ray_version) ? (
              <MetaField
                label="Versions"
                value={[query.python_version && `Python ${query.python_version}`, query.ray_version && `Ray ${query.ray_version}`].filter(Boolean).join(" | ")}
                mono
              />
            ) : (
              <div />
            )}
          </div>
        </div>
      </div>

      {/* Scrollable Content Section */}
      <div className="flex-1">
        <Tabs
          value={activeTab}
          onValueChange={handleTabChange}
          className="w-full h-full flex flex-col"
        >
          <TabsList className="flex w-full flex-shrink-0 justify-start gap-x-0 rounded-none bg-transparent p-0 border-b border-zinc-800">
            <TabsTrigger
              value="progress-table"
              disabled={
                query.state.status === "Pending" ||
                query.state.status === "Optimizing" ||
                query.state.status === "Setup"
              }
              className={TAB_TRIGGER_CLS}
            >
              Execution
            </TabsTrigger>
            <TabsTrigger
              value="optimized-plan"
              disabled={!("plan_info" in query.state && query.state.plan_info)}
              className={TAB_TRIGGER_CLS}
            >
              Optimized Plan
            </TabsTrigger>
            <TabsTrigger
              value="unoptimized-plan"
              className={TAB_TRIGGER_CLS}
            >
              Unoptimized Plan
            </TabsTrigger>
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
                      highlightedNodeId={nodeFilter}
                      hoveredNodeIds={hoveredNodeIds}
                      onViewTasks={isFlotilla ? handleViewTasksForNode : undefined}
                      tasksOpen={isFlotilla && tasksOpen}
                      onOpenTasks={isFlotilla ? handleOpenTasks : undefined}
                    />
                  </div>
                  {isFlotilla && tasksOpen && queryId && (
                    <div className="w-1/2 min-w-[480px] max-w-[900px] flex-shrink-0 border-l border-zinc-800 h-full">
                      <TasksSidebar
                        exec_state={query.state as ExecutingState}
                        nodeFilter={nodeFilter}
                        onClearFilter={handleClearNodeFilter}
                        onSelectNode={handleSelectNode}
                        onHoverNodes={setHoveredNodeIds}
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
