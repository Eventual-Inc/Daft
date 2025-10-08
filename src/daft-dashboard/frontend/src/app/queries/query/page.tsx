"use client";

import { useSearchParams } from "next/navigation";
import React, { useEffect } from "react";
import { queryInfoAtom } from "@/atoms/queryInfo";
import { useAtom } from "jotai";
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
    const [queryInfo, _] = useAtom(queryInfoAtom);

    if (!queryInfo || !queryInfo[id]) {
        return (<></>);
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
