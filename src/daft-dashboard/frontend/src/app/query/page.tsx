"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";
import useSWR from "swr";

import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
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

  const { data, isLoading, error } = useSWR<QueryInfo>(
    queryId ? `/client/query/${queryId}` : null
  );

  if (isLoading || !data) {
    return <LoadingPage />;
  }

  if (error) {
    return <div>Error: {error.message}</div>;
  }

  return (
    <div className="space-y-4">
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

      <div className="space-y-4">
        <div className="text-center py-8 text-muted-foreground">
          Query details will be displayed here
        </div>
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
