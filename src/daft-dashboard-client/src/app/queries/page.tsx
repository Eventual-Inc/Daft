"use client";

import * as React from "react";
import {
    flexRender,
    getCoreRowModel,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";

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
import { queryInfoAtom, QueryInfo, QueryInfoMap } from '@/atoms/query_info';
import { toHumanReadableDate, delta } from '@/lib/utils';
import { useRouter } from "next/navigation";

const NAME: string = "id";
const START_TIME: string = "plan-time-start";
const END_TIME: string = "plan-time-end";
const TIME_DELTA: string = "time-delta";

const columns = (queryInfoMap: QueryInfoMap) => [
    {
        accessorKey: NAME,
        header: "Name",
        cell: ({ row }: any) => row.getValue(NAME),
    },
    {
        accessorKey: START_TIME,
        header: "Start Time",
        cell: ({ row }: any) => toHumanReadableDate(row.getValue(START_TIME)),
    },
    {
        accessorKey: TIME_DELTA,
        header: "Planning Length",
        cell: ({ row }: any) => {
            return delta(
                row.getValue(START_TIME),
                queryInfoMap[row.getValue(NAME)][END_TIME as keyof QueryInfo]
            );
        },
    },
];

export default function DataTableDemo() {
    const [queryInfo, setQueryInfo] = useAtom(queryInfoAtom);
    React.useEffect(() => {
        (async () => {
            try {
                const response = await fetch('http://localhost:3239');
                const json: QueryInfo[] = await response.json();
                console.log(json);
                const queryInfoMap: QueryInfoMap = {};
                for (const queryInfo of json) {
                    queryInfoMap[queryInfo.id] = queryInfo;
                }
                setQueryInfo(queryInfoMap);
            } catch (error) { }
        })();
    }, []);
    const table = useReactTable({
        data: Object.values(queryInfo),
        columns: columns(queryInfo),
        getCoreRowModel: getCoreRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        enableSorting: true,
    });

    const router = useRouter();

    return (
        <div className="mx-[20px]">
            <div className="flex items-center py-4">
                <Input
                    placeholder="Filter queries..."
                    value={(table.getColumn(NAME)?.getFilterValue() as string) ?? ""}
                    onChange={(event) =>
                        table.getColumn(NAME)?.setFilterValue(event.target.value)
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
                                        <TableCell
                                            key={cell.id}
                                            className={`px-[20px] py-[15px] ${(cell.column.columnDef as { accessorKey: string }).accessorKey === NAME ? "w-[75%]" : undefined}`}
                                            onClick={() => router.push(`/queries/${row.id}`)}
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
