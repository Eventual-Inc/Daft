"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  createColumnHelper,
} from "@tanstack/react-table";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Breadcrumb,
  BreadcrumbList,
  BreadcrumbItem,
  BreadcrumbLink,
} from "@/components/ui/breadcrumb";
import LoadingPage from "@/components/loading";

import { QuerySummary, useQueries } from "@/hooks/use-queries";
import { toHumanReadableDate, getEngineName } from "@/lib/utils";
import Status, { Duration } from "./status";
import Link from "next/link";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { ClipboardIcon, Database, ExternalLinkIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from "@/components/ui/tooltip";
import { dashboardUrl } from "@/components/server-provider";
import { TooltipArrow } from "@radix-ui/react-tooltip";

const STATUS: string = "state";

// Define status priority for sorting (lower number = higher priority)
const statusPriority: Record<string, number> = {
  Pending: 1,
  Optimizing: 2,
  Setup: 3,
  Executing: 4,
  Finalizing: 5,
  Finished: 6,
};

// Custom sorting function for status
const statusSortingFn = (rowA: any, rowB: any, columnId: string) => {
  const statusA = rowA.getValue(columnId)?.status || "";
  const statusB = rowB.getValue(columnId)?.status || "";
  const priorityA = statusPriority[statusA] || 999;
  const priorityB = statusPriority[statusB] || 999;
  return priorityA - priorityB;
};

// Handling of query data to column parsing
const columnHelper = createColumnHelper<QuerySummary>();
const columns = [
  columnHelper.accessor("id", {
    header: "Name",
    cell: info => {
      const id = info.getValue();
      return (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="max-w-[150px] truncate">{id}</div>
            </TooltipTrigger>
            <TooltipContent>
              <p>{id}</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      );
    },
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("status", {
    header: "Status",
    cell: info => <Status state={info.getValue()} showDuration={false} />,
    sortingFn: statusSortingFn,
  }),
  columnHelper.display({
    id: "duration",
    header: "Duration",
    cell: info => <Duration state={info.row.original.status} />,
  }),
  columnHelper.accessor("entrypoint", {
    header: "Entrypoint",
    cell: info => {
      const entry = info.getValue();
      if (!entry) return "-";
      return (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="max-w-[200px] truncate">{entry}</div>
                </TooltipTrigger>
                <TooltipContent className="max-w-none text-sm">
                  <p className="break-all font-mono">{entry}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          );
    },
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("start_sec", {
    header: "Start Time",
    cell: info => toHumanReadableDate(info.getValue()),
    sortingFn: "basic",
  }),
  columnHelper.accessor("runner", {
    header: "Engine",
    cell: info => getEngineName(info.getValue()),
    sortingFn: "alphanumeric",
  }),
  // @ts-ignore
  columnHelper.accessor("ray_dashboard_url", {
    header: "Ray UI",
    cell: info => {
      let url = info.getValue();
      if (url) {
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
          url = "http://" + url;
        }
        return (
          <a
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 text-blue-500 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            Open Ray UI <ExternalLinkIcon className="h-4 w-4" />
          </a>
        );
      }
      return null;
    },
    enableSorting: false,
  })
];

const Header = () => (
  <Breadcrumb>
    <BreadcrumbList>
      <BreadcrumbItem>
        <BreadcrumbLink
          asChild
          href="/queries"
          className="text-lg font-mono font-bold"
        >
          <Link href="/queries">All Queries</Link>
        </BreadcrumbLink>
      </BreadcrumbItem>
    </BreadcrumbList>
  </Breadcrumb>
);


const EmptyState = () => {
  const copyText = `DAFT_DASHBOARD_URL="${dashboardUrl()}" python`;
  const [copied, setCopied] = React.useState(false);
  const [tooltipOpen, setTooltipOpen] = React.useState(false);
  const timeoutRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  // Cleanup timeout on unmount
  React.useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(copyText);
      setCopied(true);
      setTooltipOpen(true);

      // Clear any existing timeout before setting a new one
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }

      // Reset after 2 seconds
      timeoutRef.current = setTimeout(() => {
        setCopied(false);
        setTooltipOpen(false);
        timeoutRef.current = null;
      }, 2000);
    } catch {
      // no-op: clipboard may be unavailable
    }
  };

  return (
    <Empty>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Database />
        </EmptyMedia>
        <EmptyTitle>No Queries Run Yet!</EmptyTitle>
        <EmptyDescription>
          Please connect your Daft script to the Dashboard to get started.
        </EmptyDescription>
        <EmptyContent>
          <div className="h-[5px]" />
          <div className="border px-4 py-2 font-mono text-sm bg-zinc-800 w-[550px] flex justify-between items-center">
            <span>
              {copyText} ...
            </span>
            <Tooltip open={tooltipOpen} onOpenChange={setTooltipOpen}>
              <TooltipTrigger asChild className="hover:cursor-pointer">
                <Button
                  size="icon"
                  className="relative z-10 h-6 w-6 text-zinc-50 hover:bg-zinc-700 hover:text-zinc-50 [&_svg]:h-3 [&_svg]:w-3 border border-zinc-500"
                  onClick={handleCopy}
                >
                  <ClipboardIcon />
                </Button>
              </TooltipTrigger>
              <TooltipContent className="bg-(--daft-accent) text-white font-bold">
                {copied ? "Copied!" : "Copy to clipboard"}
                <TooltipArrow className="fill-(--daft-accent)" />
              </TooltipContent>
            </Tooltip>
          </div>
        </EmptyContent>
      </EmptyHeader>
    </Empty>
  );
};

/**
 *  Main Component to display the queries in a table
 */
export default function QueryList() {
  "use no memo";

  const { queries, isLoading } = useQueries();
  const router = useRouter();

  const table = useReactTable({
    data: queries,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: "onChange",
    initialState: {
      sorting: [
        { id: "status", desc: false },
        { id: "start_sec", desc: true },
      ],
    },
  });

  const spacing = (obj: any) =>
    `px-[20px]`;

  const handleRowClick = (queryId: string) => {
    router.push(`/query?id=${queryId}`);
  };

  if (isLoading) {
    return <LoadingPage />;
  }

  if (queries.length === 0) {
    return <EmptyState />;
  }

  return (
    <div className="space-y-4 max-w-full px-6 mx-auto">
      <Header />

      <div className="border rounded-md overflow-hidden">
        <Table>
          <TableHeader className="bg-white">
            {table.getHeaderGroups().map(headerGroup => (
              <TableRow key={headerGroup.id} className="hover:bg-transparent border-b border-zinc-200">
                {headerGroup.headers.map((header, index) => (
                  <TableHead
                    key={header.id}
                    className={`relative text-xs font-mono font-bold text-black ${spacing(header)} ${index !== headerGroup.headers.length - 1 ? "border-r border-zinc-200" : ""}`}
                    style={{ width: header.getSize() }}
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
                    <div
                      onMouseDown={header.getResizeHandler()}
                      onTouchStart={header.getResizeHandler()}
                      className={`absolute right-0 top-0 h-full w-1 cursor-col-resize touch-none select-none bg-border/50 hover:bg-primary/50 ${
                        header.column.getIsResizing() ? "bg-primary" : ""
                      }`}
                    />
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map(row => (
                <TableRow
                  key={row.id}
                  className="hover:bg-zinc-800 transition-colors duration-50 cursor-pointer"
                  onClick={() => handleRowClick(row.original.id)}
                >
                  {row.getAllCells().map((cell, index) => (
                    <TableCell
                      key={cell.id}
                      className={`py-[15px] ${spacing(cell)} ${index !== row.getAllCells().length - 1 ? "border-r border-zinc-800" : ""}`}
                      style={{ width: cell.column.getSize() }}
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext()
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="h-24 text-center"
                >
                  No results
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
