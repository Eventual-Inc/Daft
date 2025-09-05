export type QueryStatus = "Running" | "Completed" | "Failed";

export type StartTime = {
    secs_since_epoch: number,
    nanos_since_epoch: number,
};

export type QueryInfo = {
    name: string
    unoptimized_plan: string,
    optimized_plan: string,
    physical_plan: string,
};

export type QueryMetadata = {
    info: QueryInfo,
    start_time: StartTime,
    status: QueryStatus,
    duration?: string,
}

export type QueryMetadataMap = {
    [_: string]: QueryMetadata,
};

export type Keys = keyof QueryMetadataMap;
