"use client";

import * as React from "react";
import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  createColumnHelper,
  useReactTable,
} from "@tanstack/react-table";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import useSWR, { mutate } from "swr";
import { QueryMetadata, QueryMetadataMap } from "@/types/queryInfo";
import { toHumanReadableDate, calculateDurationFromStartTime } from "@/lib/utils";
import { useRouter } from "next/navigation";
import QueryStatusBadge from "@/components/query-status-badge";
import { Button } from "@/components/ui/button";
import { RefreshCw } from "lucide-react";

const NAME: string = "name";
const START_TIME: string = "start_time.secs_since_epoch";
const TIME_DELTA: string = "time_delta";

const columnHelper = createColumnHelper<QueryMetadata>();
const columns = [
  columnHelper.accessor("info.name", {
    header: "Name",
    cell: (info) => info.getValue(),
  }),
  columnHelper.accessor("status", {
    header: "Status",
    cell: (info) => <QueryStatusBadge status={info.getValue()} />,
  }),
  columnHelper.accessor("start_time.secs_since_epoch", {
    header: "Start Time",
    cell: (info) => toHumanReadableDate(info.getValue()),
  }),
  columnHelper.accessor("duration", {
    header: "Duration",
    cell: (info) => {
      const duration = info.getValue();
      const status = info.row.original.status;
      const startTime = Number(info.row.original.start_time.secs_since_epoch);
      
      // If duration exists (completed/failed queries), use it
      if (duration) {
        return duration;
      }
      
      // If no duration but query is running, calculate real-time duration
      const currentTime = Date.now();
      if (status === "Running" && startTime) {
        return calculateDurationFromStartTime(startTime, currentTime);
      }
      
      return "n/a";
    },
  }),
];

// Fetcher function for SWR
const fetcher = async (url: string): Promise<QueryMetadataMap> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Failed to fetch queries");
  }
  const json: QueryMetadata[] = await response.json();
  const queryInfoMap: QueryMetadataMap = {};
  for (const queryMetadata of json) {
    queryInfoMap[queryMetadata.info.name] = queryMetadata;
  }
  return queryInfoMap;
};

export default function QueryList() {  
  const { data: queries = {}, error, isLoading } = useSWR<QueryMetadataMap>(
    "http://localhost:3238/api/queries",
    fetcher,
    {
      refreshInterval: 5000, // Refresh every 5 seconds
      revalidateOnFocus: true,
    }
  );

  const handleRefresh = () => {
    mutate("http://localhost:3238/api/queries");
  };

  const table = useReactTable({
    data: Object.values(queries),
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    enableSorting: true,
  });
  const router = useRouter();

  const spacing = (obj: { column: { columnDef: { accessorKey: string } } }) => {
    const accessorKey = obj.column.columnDef.accessorKey;
    if (accessorKey === NAME) return "px-[24px] w-[35%]";
    if (accessorKey === START_TIME) return "px-[24px] w-[20%]";
    if (accessorKey === TIME_DELTA) return "px-[24px] w-[15%]";
    return "px-[24px] w-[15%]";
  };

  if (error) {
    return (
      <div className="gap-4 space-y-4">
        <div className="text-center text-red-500">
          Failed to load queries. Please try again.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="gap-4 space-y-4">
        <div className="text-center">
          Loading queries...
        </div>
      </div>
    );
  }

  return (
    <div className="gap-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Queries</h1>
        <Button 
          variant="outline" 
          size="sm" 
          onClick={handleRefresh}
          disabled={isLoading}
          className="flex items-center gap-2"
        >
          <RefreshCw size={16} className={isLoading ? "animate-spin" : ""} />
          Refresh
        </Button>
      </div>
      <div className="lg border border-daft-border-primary">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="">
                {headerGroup.headers.map((header) =>
                  <TableHead key={header.id} className={`text-sm font-semibold ${spacing(header as any)} py-4`}>
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
                  </TableHead>
                )}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id} className="hover:bg-daft-bg-accent/50 transition-colors">
                  {row.getAllCells().map(cell => (
                    <TableCell
                      key={cell.id}
                      className={`py-5 text-base ${spacing(cell as any)} cursor-pointer`}
                      onClick={() => router.push(`/query?name=${encodeURIComponent(cell.row.original.info.name)}`)}
                    >
                      <div className="truncate">
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext(),
                        )}
                      </div>
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="h-32 text-center text-lg"
                >
                  💤 Start running some queries!
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
