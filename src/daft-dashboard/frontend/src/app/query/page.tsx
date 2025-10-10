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
import { toHumanReadableDate, main, toHumanReadableDuration } from "@/lib/utils";
import { LoaderCircle } from "lucide-react";
import Image from "next/image";
import FishCake from "@/public/fish-cake-filled.svg";
import AnimatedFish from "@/components/animated-fish";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import LoadingPage from "@/components/loading";

type PlanInfo = {
  plan_start_sec: number;
  plan_end_sec: number;
  optimized_plan: string;
};

type OperatorStatus = "Pending" | "Executing" | "Finished";

type NodeInfo = {
  name: string;
  id: number;
  node_category: "Intermediate" | "Source" | "StreamingSink" | "BlockingSink";
  context: Record<string, string>;
};

type Stat =
  | {
      type: "Count";
      value: number;
    }
  | {
      type: "Bytes";
      value: number;
    }
  | {
      type: "Percent";
      value: number;
    }
  | {
      type: "Float";
      value: number;
    }
  | {
      type: "Duration";
      value: number;
    };

type OperatorInfo = {
  status: OperatorStatus;
  node_info: NodeInfo;
  stats: Record<string, Stat>;
};

type ExecInfo = {
  exec_start_sec: number;
  operators: Record<number, OperatorInfo>;
  // TODO: Logs
};

type QueryState =
  | {
      status: "Pending";
    }
  | {
      status: "Optimizing";
      plan_start_sec: number;
    }
  | {
      status: "Setup";
      plan_info: PlanInfo;
    }
  | {
      status: "Executing";
      plan_info: PlanInfo;
      exec_info: ExecInfo;
    }
  | {
      status: "Finalizing";
      plan_info: PlanInfo;
      exec_info: ExecInfo;
      exec_end_sec: number;
    }
  | {
      status: "Finished";
      plan_info: PlanInfo;
      exec_info: ExecInfo;
      exec_end_sec: number;
      end_sec: number;
    };

type QueryInfo = {
  id: string;
  start_sec: number;
  unoptimized_plan: string;
  state: QueryState;
};

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
      console.warn("Connected to query SSE endpoint");
    };
    es.onerror = event => {
      console.error("Error subscribing to query:", event);
    };
    // These overwrite
    es.addEventListener("initial_state", event => {
      setQuery(JSON.parse(event.data));
    });
    // TODO: Consistent ordering of statistics
    es.addEventListener("query_info", event => {
      setQuery(JSON.parse(event.data));
    });
    // Merges with existing info
    es.addEventListener("operator_info", event => {
      setQuery(prev => {
        if (!prev) return prev;

        const plan_info = "plan_info" in prev.state ? prev.state.plan_info : undefined;
        const old_exec_info = "exec_info" in prev.state ? prev.state.exec_info : undefined;
        const data = JSON.parse(event.data);

        console.log(data)
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
      console.log("Closing query SSE endpoint");
      es.close();
    };
  }, [queryId, setQuery]);

  if (!query) {
    return <LoadingPage />;
  }


  // Custom status component that shows only status without duration text
  const CustomStatus = ({ state }: { state: QueryState }) => {
    const getStatusDisplay = () => {
      switch (state.status) {
        case "Pending":
          return {
            icon: <LoaderCircle size={16} strokeWidth={3} className="text-chart-1" />,
            label: "Waiting to Start",
            textColor: "text-chart-1"
          };
        case "Optimizing":
          return {
            icon: <LoaderCircle size={16} strokeWidth={3} className="text-orange-500" />,
            label: "Optimizing",
            textColor: "text-orange-500"
          };
        case "Setup":
          return {
            icon: <LoaderCircle size={16} strokeWidth={3} className="text-magenta-500" />,
            label: "Setting Up Runner",
            textColor: "text-magenta-500"
          };
        case "Executing":
          return {
            icon: <AnimatedFish />,
            label: "Running",
            textColor: "text-(--daft-accent)"
          };
        case "Finalizing":
          return {
            icon: <LoaderCircle size={16} strokeWidth={3} className="text-blue-500" />,
            label: "Finalizing Query",
            textColor: "text-blue-500"
          };
        case "Finished":
          return {
            icon: <Image src={FishCake} alt="Fish Cake" className="animate-[spin_5s_linear_infinite]" height={20} width={20} />,
            label: "Finished",
            textColor: "text-green-500"
          };
        default:
          return {
            icon: <LoaderCircle size={16} strokeWidth={3} className="text-chart-2" />,
            label: "Unknown",
            textColor: "text-chart-2"
          };
      }
    };

    const statusDisplay = getStatusDisplay();

    return (
      <div className="flex items-center justify-center gap-x-4 w-full">
        <div className="scale-150">
          {statusDisplay.icon}
        </div>
        <span className={`${main.className} ${statusDisplay.textColor} font-bold text-4xl`}>
          {statusDisplay.label}
        </span>
      </div>
    );
  };

  // Bigger status component for the query page
  const BigStatus = ({ state }: { state: QueryState }) => {
    const [currentTime, setCurrentTime] = useState(() => Date.now());

    useEffect(() => {
      const interval = setInterval(() => {
        setCurrentTime(Date.now());
      }, 1000);
      return () => clearInterval(interval);
    }, []);

    const duration = Math.round(currentTime / 1000 - query.start_sec);

    return (
      <div className="flex flex-col items-center justify-center h-full space-y-4">
        <CustomStatus state={state} />
        <div className={`${main.className} text-sm font-mono text-zinc-400`}>
          {toHumanReadableDuration(duration)}
        </div>
      </div>
    );
  };

  return (
    <div className="h-screen flex flex-col">
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
            <BigStatus state={query.state} />
          </div>
          <div className="flex-1 h-full px-6 py-4">
            <div className="grid grid-cols-2 gap-6 h-full">
              <div className="space-y-3">
                <div>
                  <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                    Query ID
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    {query.id}
                  </p>
                </div>
                <div>
                  <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                    Start Time
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    {toHumanReadableDate(query.start_sec)}
                  </p>
                </div>
              </div>
              <div className="space-y-3">
                <div>
                  <h3 className={`${main.className} text-sm font-semibold text-zinc-400 mb-1`}>
                    Runner
                  </h3>
                  <p className={`${main.className} text-lg font-mono`}>
                    Native (Swordfish)
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className="w-full h-[20px] border-b flex"></div>
      </div>

      {/* Scrollable Content Section */}
      <div className="flex-1 overflow-hidden">
        <Tabs defaultValue="unoptimized-plan" className="w-full h-full flex flex-col">
          <TabsList className="grid w-full grid-cols-4 flex-shrink-0">
            <TabsTrigger value="unoptimized-plan">Unoptimized Plan</TabsTrigger>
            <TabsTrigger
              value="optimized-plan"
              disabled={!("plan_info" in query.state)}
            >
              Optimized Plan
            </TabsTrigger>
            <TabsTrigger
              value="progress-table"
              disabled={query.state.status === "Pending" || query.state.status === "Optimizing" || query.state.status === "Setup"}
            >
              Progress Table
            </TabsTrigger>
            <TabsTrigger
              value="results"
              disabled={query.state.status !== "Finished"}
            >
              Results
            </TabsTrigger>
          </TabsList>

          <TabsContent value="unoptimized-plan" className="mt-4 flex-1 overflow-auto">
            <div className="bg-zinc-900 rounded-lg p-4 h-full">
              <h3 className={`${main.className} text-lg font-semibold mb-3`}>Unoptimized Plan</h3>
              <pre className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap`}>
                {query.unoptimized_plan}
              </pre>
            </div>
          </TabsContent>

          <TabsContent value="optimized-plan" className="mt-4 flex-1 overflow-auto">
            <div className="bg-zinc-900 rounded-lg p-4 h-full">
              <h3 className={`${main.className} text-lg font-semibold mb-3`}>Optimized Plan</h3>
              {query.state.status === "Pending" ? (
                <p className={`${main.className} text-zinc-400`}>Plan not yet optimized</p>
              ) : (
                <pre className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap`}>
                  {"plan_info" in query.state ? query.state.plan_info.optimized_plan : "No optimized plan available"}
                </pre>
              )}
            </div>
          </TabsContent>

          <TabsContent value="progress-table" className="mt-4 flex-1 overflow-auto">
            <div className="bg-zinc-900 rounded-lg h-full">
              <div className="p-4 border-b border-zinc-700">
                <h3 className={`${main.className} text-lg font-semibold`}>Progress Table</h3>
              </div>

              {query.state.status === "Pending" || query.state.status === "Optimizing" ? (
                <div className="p-8 text-center">
                  <p className={`${main.className} text-zinc-400`}>Execution not yet started</p>
                </div>
              ) : (
                <div className="overflow-auto h-full">
                  <div className="min-w-[710px]">
                    {/* Table Headers */}
                    <div className="bg-zinc-800 grid grid-cols-[40px_100px_200px_120px_120px_1fr] gap-0 items-center min-h-[55px] border-b border-zinc-600">
                      <div className="px-3 py-4"></div>
                      <div className="px-3 py-4 text-right text-sm font-medium text-zinc-300">Status</div>
                      <div className="px-3 py-4 text-sm font-medium text-zinc-300">Name</div>
                      <div className="px-3 py-4 text-right text-sm font-medium text-zinc-300">Rows In</div>
                      <div className="px-3 py-4 text-right text-sm font-medium text-zinc-300">Rows Out</div>
                      <div className="px-3 py-4 text-sm font-medium text-zinc-300">Extra Stats</div>
                    </div>

                    {/* Operator Rows */}
                    <div className="divide-y divide-zinc-700">
                      {Object.entries("exec_info" in query.state ? query.state.exec_info.operators : {}).map(([operatorId, operator]) => {
                        const getStatusIcon = () => {
                          switch (operator.status) {
                            case "Finished":
                              return (
                                <Image
                                  src={FishCake}
                                  alt="Fish Cake"
                                  className="animate-spin"
                                  height={20}
                                  width={20}
                                  style={{ animationDuration: '3s' }}
                                />
                              );
                            case "Executing":
                              return <AnimatedFish />;
                            case "Pending":
                            default:
                              return (
                                <div className="w-5 h-5 border-2 border-zinc-400 border-t-transparent rounded-full animate-spin"></div>
                              );
                          }
                        };

                        const getStatusText = () => {
                          if (operator.status === "Finished") {
                            return "Finished";
                          } else if (operator.status === "Executing") {
                            return "Running";
                          } else {
                            return "Pending";
                          }
                        };

                        const getStatusColor = () => {
                          switch (operator.status) {
                            case "Finished":
                              return "text-green-500";
                            case "Executing":
                              return "text-purple-500";
                            case "Pending":
                            default:
                              return "text-zinc-400";
                          }
                        };

                        // Extract stats from operator.stats
                        console.log("Operator stats:", operator.stats);
                        const rowsIn = operator.stats?.["rows in"]?.value || 0;
                        const rowsOut = operator.stats?.["rows out"]?.value || 0;

                        // Get extra stats (all stats except the ones we're already displaying)
                        const formatStatValue = (stat: Stat) => {
                          switch (stat.type) {
                            case "Count":
                              return stat.value.toLocaleString();
                            case "Bytes":
                              const bytes = stat.value;
                              if (bytes >= 1024 * 1024 * 1024) {
                                return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
                              } else if (bytes >= 1024 * 1024) {
                                return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
                              } else if (bytes >= 1024) {
                                return `${(bytes / 1024).toFixed(1)} KiB`;
                              } else {
                                return `${bytes} B`;
                              }
                            case "Percent":
                              return `${stat.value.toFixed(1)}%`;
                            case "Duration":
                              return `${stat.value.toFixed(1)}s`;
                            case "Float":
                              return stat.value.toFixed(2);
                            default:
                              return String((stat as any).value);
                          }
                        };

                        const extraStats = Object.entries(operator.stats)
                          .filter(([key]) => !["rows in", "rows out", "cpu us"].includes(key))
                          .map(([key, stat]) => `${key.charAt(0).toUpperCase() + key.slice(1)}: ${formatStatValue(stat)}`)
                          .join(", ");

                        return (
                          <div
                            key={operatorId}
                            className="grid grid-cols-[40px_100px_200px_120px_120px_1fr] gap-0 items-center min-h-[55px] hover:bg-zinc-800/50 transition-colors"
                          >
                            <div className="px-3 py-4 flex items-center justify-center">
                              {getStatusIcon()}
                            </div>
                            <div className="px-3 py-4 text-right text-sm">
                              <span className={getStatusColor()}>{getStatusText()}</span>
                            </div>
                            <div className="px-3 py-4 text-sm text-zinc-300 truncate">
                              {operator.node_info.name}
                            </div>
                            <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono">
                              {rowsIn.toLocaleString()}
                            </div>
                            <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono">
                              {rowsOut.toLocaleString()}
                            </div>
                            <div className="px-3 py-4 text-sm text-zinc-400 font-mono">
                              {extraStats || "-"}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </TabsContent>

          <TabsContent value="results" className="mt-4 flex-1 overflow-auto">
            <div className="bg-zinc-900 rounded-lg p-4 h-full">
              <h3 className={`${main.className} text-lg font-semibold mb-3`}>Results</h3>
              {query.state.status !== "Finished" ? (
                <p className={`${main.className} text-zinc-400`}>Query not yet finished</p>
              ) : (
                <div className="space-y-2">
                  <p className={`${main.className} text-sm text-zinc-400`}>
                    Query completed at: {toHumanReadableDate(query.state.end_sec)}
                  </p>
                  <p className={`${main.className} text-sm text-zinc-400`}>
                    Total duration: {toHumanReadableDuration(query.state.end_sec - query.start_sec)}
                  </p>
                  <div className="text-sm text-zinc-300">
                    <p>Results will be displayed here when available</p>
                  </div>
                </div>
              )}
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
