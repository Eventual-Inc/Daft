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
import { useAtom } from "jotai";
import { queryInfoAtom, QueryInfo, QueryInfoMap } from "@/atoms/queryInfo";
import { toHumanReadableDate, delta } from "@/lib/utils";
import { useRouter } from "next/navigation";

const NAME: string = "id";
const START_TIME: string = "plan_time_start";
const TIME_DELTA: string = "time_delta";

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
                queryInfoMap[row.getValue(NAME)].plan_time_end,
            );
        },
    },
];

export default function QueryList() {
    const [queryInfo, setQueryInfo] = useAtom(queryInfoAtom);
    React.useEffect(() => {
        (async () => {
            try {
                const response = await fetch("http://localhost:3238/api/queries");
                const json: QueryInfo[] = await response.json();
                const queryInfoMap: QueryInfoMap = {};
                for (const queryInfo of json) {
                    queryInfoMap[queryInfo.id] = queryInfo;
                }
                setQueryInfo(queryInfoMap);
            } catch { }
        })();
    }, []);
    const cols = columns(queryInfo);
    const table = useReactTable(React.useMemo(() => ({
        data: Object.values(queryInfo),
        columns: cols,
        getCoreRowModel: getCoreRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        enableSorting: true,
    }), [queryInfo]));
    const router = useRouter();

    const spacing = (obj: { column: { columnDef: { accessorKey: string } } }) => `px-[20px] ${obj.column.columnDef.accessorKey === NAME ? "w-[70%]" : undefined}`;

    return (
        <div className="gap-4 space-y-4">
            <div className="flex items-center">
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
                            <TableRow key={headerGroup.id} className="">
                                {headerGroup.headers.map((header) =>
                                    <TableHead key={header.id} className={`text-xs ${spacing(header as any)}`}>
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
                                            className={`py-[15px] ${spacing(cell as any)} cursor-pointer`}
                                            onClick={() => router.push(`/queries/query?id=${cell.row.original.id}`)}
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
                                    colSpan={cols.length}
                                    className="h-24 text-center"
                                >
                                    No results
                                </TableCell>
                            </TableRow>
                        )}
                    </TableBody>
                </Table>
            </div>
            <div className="flex items-center justify-end space-x-2">
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
