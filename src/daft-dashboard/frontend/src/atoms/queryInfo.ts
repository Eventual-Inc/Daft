import { atomWithStorage } from "jotai/utils";

export type QueryInfo = {
    id: string
    unoptimized_plan: string,
    optimized_plan: string,
    plan_time_start: string
    plan_time_end: string
    run_id?: string
    logs?: string
};

export type QueryInfoMap = {
    [_: string]: QueryInfo,
};

export type Keys = keyof QueryInfo;

export const queryInfoAtom = atomWithStorage<QueryInfoMap>("queryInfo", {});
