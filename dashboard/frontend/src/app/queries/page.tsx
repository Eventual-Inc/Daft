"use client";

import * as React from "react";
import {
    ColumnDef,
    flexRender,
    getCoreRowModel,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";

import StatusBadge from "@/components/status-badge";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { useAtom } from 'jotai';
import { queryInfoAtom, QueryInfo, ID, STATUS, MERMAID_PLAN } from '@/atoms/query_info';

// const NAME_KEY = "name";
// const STATUS_KEY = "status";
// type Query = {
//     [NAME_KEY]: string
//     [STATUS_KEY]: BadgeStatus,
// }
// const data: Query[] = [
//     {
//         [NAME_KEY]: "serene-darwin",
//         [STATUS_KEY]: "success",
//     },
//     {
//         [NAME_KEY]: "curious-newton",
//         [STATUS_KEY]: "pending",
//     },
//     {
//         [NAME_KEY]: "vibrant-edison",
//         [STATUS_KEY]: "failed",
//     },
//     {
//         [NAME_KEY]: "ecstatic-gauss",
//         [STATUS_KEY]: "cancelled",
//     },
// ];

const columns: ColumnDef<QueryInfo>[] = [
    {
        accessorKey: ID,
        header: "Name",
        cell: ({ row }) => row.getValue(ID),
    },
    {
        accessorKey: STATUS,
        header: "Status",
        cell: ({ row }) => <StatusBadge status={row.getValue(STATUS)} />,
    },
];

export default function DataTableDemo() {
    const [queryInfo, setQueryInfo] = useAtom(queryInfoAtom);
    React.useEffect(() => {
        (async () => {
            try {
                const response = await fetch('http://localhost:3239');
                const json = await response.json();
                console.log(json);
                setQueryInfo(json);
            } catch (error) { }
        })();
    });
    const table = useReactTable({
        data: queryInfo!,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <div className="mx-[20px]">
            <div className="flex items-center py-4">
                <Input
                    placeholder="Filter queries..."
                    value={(table.getColumn(ID)?.getFilterValue() as string) ?? ""}
                    onChange={(event) =>
                        table.getColumn(ID)?.setFilterValue(event.target.value)
                    }
                    className="max-w-sm"
                />
            </div>
            <div className="rounded-md border">
                <Table>
                    <TableHeader>
                        {table.getHeaderGroups().map((headerGroup) => (
                            <TableRow key={headerGroup.id}>
                                {headerGroup.headers.map((header) =>
                                    <TableHead key={header.id} className="px-[20px] text-xs">
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
                                <TableRow key={row.id}>
                                    {row.getAllCells().map(cell => (
                                        <TableCell key={cell.id} className={`px-[20px] py-[15px] ${(cell.column.columnDef as { accessorKey: string }).accessorKey === "name" ? "w-[75%]" : undefined}`}>
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
                                    className="h-24 text-center"
                                >
                                    No results
                                </TableCell>
                            </TableRow>
                        )}
                    </TableBody>
                </Table>
            </div>
            <div className="flex items-center justify-end space-x-2 py-4">
                <div className="space-x-2">
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={() => table.previousPage()}
                        disabled={!table.getCanPreviousPage()}
                        className="w-[80px]"
                    >
                        Previous
                    </Button>
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={() => table.nextPage()}
                        disabled={!table.getCanNextPage()}
                        className="w-[80px]"
                    >
                        Next
                    </Button>
                </div>
            </div>
        </div>
    );
}
