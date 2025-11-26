import { createContext, useContext } from "react";

export type PendingStatus = {
  status: "Pending";
  start_sec: number;
};

export type PlanningStatus = {
  status: "Optimizing";
  plan_start_sec: number;
};

export type SetupStatus = {
  status: "Setup";
};

export type ExecutingStatus = {
  status: "Executing";
  exec_start_sec: number;
};

export type FinalizingStatus = {
  status: "Finalizing";
};

export type FinishedStatus = {
  status: "Finished";
  duration_sec: number;
};

export type FailedStatus = {
  status: "Failed";
  message: string;
};

export type CanceledStatus = {
  status: "Canceled";
  message: string;
};

export type DeadStatus = {
  status: "Dead";
};

export type QueryStatus =
  | PendingStatus
  | PlanningStatus
  | SetupStatus
  | ExecutingStatus
  | FinalizingStatus
  | FinishedStatus
  | FailedStatus
  | CanceledStatus
  | DeadStatus;

export type QueryStatusName =
  | "Pending"
  | "Optimizing"
  | "Setup"
  | "Executing"
  | "Finalizing"
  | "Finished"
  | "Failed"
  | "Canceled"
  | "Dead";

export type QuerySummary = {
  id: string;
  start_sec: number;
  status: QueryStatus;
};

export type QuerySummaryMap = { [_: string]: QuerySummary };
export const QueriesContext = createContext<QuerySummaryMap | null>(null);

// ---------------------- Hooks ---------------------- //

// Hook to fetch all queries
export function useQueries() {
  const queries = useContext(QueriesContext);

  return {
    queries: Object.values(queries || {}),
    isLoading: queries === null,
  };
}

// Hook to get actively running queries (not finished or finalizing)
export function useActiveQueries() {
  const { queries, isLoading } = useQueries();
  const activeQueries = queries
    .filter(
      query =>
        query.status.status !== "Finished" &&
        query.status.status !== "Finalizing" &&
        query.status.status !== "Dead" &&
        query.status.status !== "Canceled" &&
        query.status.status !== "Failed"
    )
    .sort((a, b) => {
      return a.start_sec - b.start_sec;
    });

  return {
    activeQueries,
    isLoading,
  };
}
