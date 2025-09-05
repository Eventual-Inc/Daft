"use client";

import { useSearchParams } from "next/navigation";
import React, { useEffect } from "react";
import useSWR from "swr";
import { QueryInfo, QueryInfoMap } from "@/types/queryInfo";
import mermaid from "mermaid";
import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    CardDescription,
} from "@/components/ui/card";
import { delta, toHumanReadableDate } from "@/lib/utils";

mermaid.initialize({
    startOnLoad: true
});

// Fetcher function for SWR
const fetcher = async (url: string): Promise<QueryInfoMap> => {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error("Failed to fetch queries");
    }
    const json: QueryInfo[] = await response.json();
    const queryInfoMap: QueryInfoMap = {};
    for (const queryInfo of json) {
        queryInfoMap[queryInfo.id] = queryInfo;
    }
    return queryInfoMap;
};

function Mermaid({ chart }: { chart: string }) {
    useEffect(() => {
        mermaid.contentLoaded();
    }, []);

    return (
        <div className="mermaid w-[70%] flex justify-center">
            {chart}
        </div>
    );
}

function SuspendedQueryPage() {
    const searchParams = useSearchParams();
    const id: string = searchParams.get("id")!;
    const { data: queryInfo = {}, error, isLoading } = useSWR<QueryInfoMap>(
        "http://localhost:3238/api/queries",
        fetcher,
        {
            refreshInterval: 5000, // Refresh every 5 seconds
            revalidateOnFocus: true,
        }
    );

    if (error) {
        return (
            <div className="space-y-4">
                <div className="text-center text-red-500">
                    Failed to load query data. Please try again.
                </div>
            </div>
        );
    }

    if (isLoading) {
        return (
            <div className="space-y-4">
                <div className="text-center">
                    Loading query data...
                </div>
            </div>
        );
    }

    if (!queryInfo || !queryInfo[id]) {
        return (
            <div className="space-y-4">
                <div className="text-center">
                    Query not found.
                </div>
            </div>
        );
    }


    return (
        <div className="space-y-4">
            <Card className="w-full">
                <CardHeader>
                    <CardTitle className="text-lg">Query Statistics</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="grid grid-cols-2 gap-4">
                        <div>
                            <CardDescription>Query Id</CardDescription>
                            <p>{queryInfo[id].id}</p>
                        </div>
                        <div>
                            <CardDescription>Query Plan Duration</CardDescription>
                            <p>{delta(queryInfo[id].plan_time_start, queryInfo[id].plan_time_end)}</p>
                        </div>
                        <div>
                            <CardDescription>Query Plan Start Time</CardDescription>
                            <p>{toHumanReadableDate(queryInfo[id].plan_time_start)}</p>
                        </div>
                        <div>
                            <CardDescription>Query Plan End Time</CardDescription>
                            <p>{toHumanReadableDate(queryInfo[id].plan_time_end)}</p>
                        </div>
                    </div>
                </CardContent>
            </Card>
            <Card className="w-full">
                <CardHeader className="text-lg">
                    <CardTitle>Query Plan</CardTitle>
                </CardHeader>
                <CardContent className="justify-center flex">
                    <div>Coming Soon</div>
                </CardContent>
            </Card>
        </div>
    );
};

export default function QueryPage() {
    return (
        <React.Suspense fallback={<></>}>
            <SuspendedQueryPage />
        </React.Suspense>
    );
}
