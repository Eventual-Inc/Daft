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
import { Suspense, useEffect, useMemo, useState } from "react";
import { toHumanReadableDate, main, getEngineName } from "@/lib/utils";
import LoadingPage from "@/components/loading";
import { Status } from "../status";
import { OperatorInfo, QueryInfo } from "../types";
import SparklinesView from "./sparklines-view";

function getExecInfo(query: QueryInfo) {
  const state = query.state;
  if (
    state.status === "Executing" ||
    state.status === "Finalizing" ||
    state.status === "Finished"
  ) {
    return state.exec_info;
  }
  return null;
}

function getEndSec(query: QueryInfo): number | null {
  const state = query.state;
  if (
    state.status === "Finished" ||
    state.status === "Canceled" ||
    state.status === "Failed"
  ) {
    return state.end_sec;
  }
  return null;
}

function SparklinesPageInner() {
  const searchParams = useSearchParams();
  const queryId = searchParams.get("id");
  const debug = useMemo(() => searchParams.has("debug"), [searchParams]);
  const [query, setQuery] = useState<QueryInfo | null>(null);

  useEffect(() => {
    if (!queryId) return;
    const es = new EventSource(genApiUrl(`/client/query/${queryId}/subscribe`));
    es.onopen = () => {
      console.info("Connected to query SSE endpoint (sparklines)");
    };
    es.onerror = event => {
      console.error("Error subscribing to query:", event);
    };
    es.addEventListener("initial_state", event => {
      const data: QueryInfo = JSON.parse(event.data);
      if (debug) console.log("[debug] initial_state (sparklines)", data);
      setQuery(data);
    });
    es.addEventListener("query_info", event => {
      const data: QueryInfo = JSON.parse(event.data);
      if (debug) console.log("[debug] query_info (sparklines)", data);
      setQuery(data);
    });
    es.addEventListener("operator_info", event => {
      const data: Record<number, OperatorInfo> = JSON.parse(event.data);
      if (debug) console.log("[debug] operator_info (sparklines)", data);
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
      console.info("Closing query SSE endpoint (sparklines)");
      es.close();
    };
  }, [queryId, debug]);

  if (!query) {
    return <LoadingPage />;
  }

  const endSec = getEndSec(query);
  const execInfo = getExecInfo(query);
  const notStarted =
    query.state.status === "Pending" ||
    query.state.status === "Optimizing" ||
    query.state.status === "Setup";

  return (
    <div className="h-full flex flex-col">
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
              <BreadcrumbLink
                asChild
                className="text-lg font-mono text-zinc-400 hover:text-white"
              >
                <Link href={`/query?id=${queryId}`}>Query {queryId}</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink asChild className="text-lg font-mono font-bold">
                <p>Sparklines</p>
              </BreadcrumbLink>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>

        <div className="flex gap-2">
          <Link
            href={`/query/timeline?id=${queryId}`}
            className={`${main.className} text-xs px-3 py-1 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-200 transition-colors`}
          >
            ← Timeline (Gantt)
          </Link>
        </div>

        <div className="w-full flex border-b border-zinc-800">
          <div className="w-1/4 border-r border-zinc-800 px-6 py-4">
            <Status
              status={query.state.status}
              start_sec={query.start_sec}
              end_sec={endSec}
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
                    {endSec ? toHumanReadableDate(endSec) : "..."}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className="w-full h-[20px] flex"></div>
      </div>

      <div className="flex-1 min-h-0">
        {notStarted ? (
          <div className="p-8 text-center">
            <p className={`${main.className} text-zinc-400`}>
              Execution not yet started
            </p>
          </div>
        ) : execInfo ? (
          <SparklinesView
            operators={execInfo.operators}
            query_start_sec={execInfo.exec_start_sec}
            query_end_sec={endSec}
            process_stats={query.process_stats}
          />
        ) : (
          <div className="p-8 text-center">
            <p className={`${main.className} text-zinc-400`}>
              No execution data available
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default function SparklinesPage() {
  return (
    <Suspense fallback={<LoadingPage />}>
      <SparklinesPageInner />
    </Suspense>
  );
}
