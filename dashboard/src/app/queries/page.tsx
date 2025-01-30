'use client';

import { Check, CircleX, LoaderCircle } from 'lucide-react';
import * as React from 'react';
import {
    ColumnDef,
    ColumnFiltersState,
    flexRender,
    getCoreRowModel,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    useReactTable,
} from '@tanstack/react-table';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from "@/components/ui/badge"
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table';

const NAME_KEY = "name";
const STATUS_KEY = "status";

type Query = {
    [NAME_KEY]: string
    [STATUS_KEY]: QueryStatus
}
type QueryStatus = 'pending' | 'success' | 'failed';

const data: Query[] = [
    {
        [NAME_KEY]: '3u1reuv4',
        [STATUS_KEY]: 'success',
    },
    {
        [NAME_KEY]: 'derv1ws0',
        [STATUS_KEY]: 'pending',
    },
    {
        [NAME_KEY]: 'bhqecj4p',
        [STATUS_KEY]: 'failed',
    },
];

const columns: ColumnDef<Query>[] = [
    {
        accessorKey: NAME_KEY,
        header: 'Name',
        cell: ({ row }) => row.getValue(NAME_KEY),
    },
    {
        accessorKey: STATUS_KEY,
        header: 'Status',
        cell: ({ row }) => {
            const status = row.getValue(STATUS_KEY);
            if (status === "success") { return Success() }
            else if (status === "pending") { return Pending() }
            else { return Failure() }
        },
    },
];

const Success = () => <Badge variant="outline" className="text-green-600">
    <Check size={13} strokeWidth={3} />
    <div className="px-[2px]" />
    Success
</Badge>;
const Pending = () => <Badge variant="outline" className="text-yellow-600">
    <LoaderCircle className="animate-spin" size={13} strokeWidth={3} />
    <div className="px-[2px]" />
    Pending
</Badge>;
const Failure = () => <Badge variant="outline" className="text-red-600">
    <CircleX size={13} strokeWidth={3} />
    <div className="px-[2px]" />
    Failure
</Badge>;

export default function DataTableDemo() {
    const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([]);
    const [rowSelection, setRowSelection] = React.useState({});

    const table = useReactTable({
        data,
        columns,
        onColumnFiltersChange: setColumnFilters,
        getCoreRowModel: getCoreRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        onRowSelectionChange: setRowSelection,
        state: {
            columnFilters,
            rowSelection,
        },
    });

    return (
        <div className="mx-[20px]">
            <div className="flex items-center py-4">
                <Input
                    placeholder="Filter queries..."
                    value={(table.getColumn(NAME_KEY)?.getFilterValue() as string) ?? ''}
                    onChange={(event) =>
                        table.getColumn(NAME_KEY)?.setFilterValue(event.target.value)
                    }
                    className="max-w-sm"
                />
            </div>
            <div className="rounded-md border">
                <Table>
                    <TableHeader>
                        {table.getHeaderGroups().map((headerGroup) => (
                            <TableRow key={headerGroup.id}>
                                {headerGroup.headers.map((header) => {
                                    return (
                                        <TableHead key={header.id} className="px-[20px] text-xs">
                                            {flexRender(
                                                header.column.columnDef.header,
                                                header.getContext()
                                            )}
                                        </TableHead>
                                    );
                                })}
                            </TableRow>
                        ))}
                    </TableHeader>
                    <TableBody>
                        {table.getRowModel().rows?.length ? (
                            table.getRowModel().rows.map((row) => (
                                <TableRow key={row.id}>
                                    {row.getAllCells().map(cell => (
                                        <TableCell key={cell.id} className={`px-[20px] py-[15px] ${(cell.column.columnDef as { accessorKey: string }).accessorKey === "name" ? "w-[75%]" : ""}`}>
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
                                    No results.
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
