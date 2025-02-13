import { atomWithStorage } from "jotai/utils";

export type QueryInfo = {
    id: string
    mermaid_plan: string
    plan_time_start: string
    plan_time_end: string
};

export type QueryInfoMap = {
    [_: string]: QueryInfo,
};

export type Keys = keyof QueryInfo;

export const queryInfoAtom = atomWithStorage<QueryInfoMap>("queryInfo", {});
