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
import { toHumanReadableDate } from "@/lib/utils";
import Status from "./status";
import Link from "next/link";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { ClipboardIcon, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { dashboardUrl } from "@/components/server-provider";
import { TooltipArrow } from "@radix-ui/react-tooltip";

const STATUS: string = "state";

// Define status priority for sorting (lower number = higher priority)
const statusPriority: Record<string, number> = {
  Pending: 1,
  Planning: 2,
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
    cell: info => info.getValue(),
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("status", {
    header: "Status",
    cell: info => <Status state={info.getValue()} />,
    sortingFn: statusSortingFn,
  }),
  columnHelper.accessor("start_sec", {
    header: "Start Time",
    cell: info => toHumanReadableDate(info.getValue()),
    sortingFn: "basic",
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
    initialState: {
      sorting: [
        { id: "status", desc: false },
        { id: "start_sec", desc: true },
      ],
    },
  });

  const spacing = (obj: any) =>
    `px-[20px] ${obj.column.columnDef.accessorKey === STATUS ? "w-[60%]" : undefined}`;

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
    <div className="space-y-4 max-w-6xl mx-auto">
      <Header />

      <div className="border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map(headerGroup => (
              <TableRow key={headerGroup.id} className="">
                {headerGroup.headers.map(header => (
                  <TableHead
                    key={header.id}
                    className={`text-xs font-mono ${spacing(header)}`}
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
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
                  {row.getAllCells().map(cell => (
                    <TableCell
                      key={cell.id}
                      className={`py-[15px] ${spacing(cell)}`}
                    >
                      <div className="truncate">
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext()
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
