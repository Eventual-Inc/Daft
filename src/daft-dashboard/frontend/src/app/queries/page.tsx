"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  createColumnHelper,
  VisibilityState,
  getFilteredRowModel,
  ColumnFiltersState
} from "@tanstack/react-table";
import { ChevronDown, Columns3 } from "lucide-react";

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
import { Button } from "@/components/ui/button";
import LoadingPage from "@/components/loading";

import { QuerySummary, useQueries } from "@/hooks/use-queries";
import { toHumanReadableDate } from "@/lib/utils";
import Status from "./status";
import Link from "next/link";
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger, DropdownMenuCheckboxItem } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";

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

// Column ID to Name
const columnIdToName = {
  id: "ID",
  name: "Name",
  status: "Status",
  start_sec: "Start Time",
};

// Handling of query data to column parsing
const columnHelper = createColumnHelper<QuerySummary>();
const columns = [
  columnHelper.accessor("id", {
    header: columnIdToName.id,
    cell: info => info.getValue(),
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("name", {
    header: columnIdToName.name,
    cell: info => info.getValue(),
    sortingFn: "alphanumeric",
  }),
  columnHelper.accessor("status", {
    header: columnIdToName.status,
    cell: info => <Status state={info.getValue()} />,
    sortingFn: statusSortingFn,
  }),
  columnHelper.accessor("start_sec", {
    header: columnIdToName.start_sec,
    cell: info => toHumanReadableDate(info.getValue()),
    sortingFn: "basic",
  }),
];

const initialColumnVisibility: VisibilityState = {
  id: false,
  name: true,
  status: true,
  start_sec: true,
};

/**
 *  Main Component to display the queries in a table
 */
export default function QueryList() {
  "use no memo";

  const { queries, isLoading } = useQueries();
  const router = useRouter();
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>(initialColumnVisibility);
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])

  const table = useReactTable({
    data: queries,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onColumnVisibilityChange: setColumnVisibility,
    onColumnFiltersChange: setColumnFilters,
    state: {
      columnVisibility,
      columnFilters,
    },
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

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
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

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" className="ml-auto bg-zinc-800 hover:bg-zinc-700">
              <Columns3 className="h-4 w-4" />
              Columns
              <ChevronDown className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="bg-zinc-800">
            {table.getAllColumns().map((column) => (
              <DropdownMenuCheckboxItem key={column.id} className="capitalize hover:bg-zinc-700" checked={column.getIsVisible()} onCheckedChange={(value) => column.toggleVisibility(!!value)}>
                {columnIdToName[column.id as keyof typeof columnIdToName]}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="flex items-center justify-between on:bg-zinc-900">
        <Input
            type="text"
            autoComplete="off"
            data-1p-ignore data-lpignore="true" data-protonpass-ignore="true"
            placeholder="Filter by Name..."
            value={(table.getColumn("name")?.getFilterValue() as string) ?? ""}
            onChange={(event) =>
              table.getColumn("name")?.setFilterValue(event.target.value)
            }
            className="max-w-sm"
          />
      </div>

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
                  className="hover:bg-zinc-800 transition-colors duration-50 cursor-pointer font-mono"
                  onClick={() => handleRowClick(row.original.id)}
                >
                  {row.getVisibleCells().map(cell => (
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
                  colSpan={table.getVisibleFlatColumns().length}
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
