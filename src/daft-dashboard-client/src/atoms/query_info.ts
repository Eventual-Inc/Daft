import { atom } from 'jotai';
import { BadgeStatus } from "@/components/status-badge";

export const ID = "id";
export const STATUS = "status";
export const MERMAID_PLAN = "mermaid-plan";

export type QueryInfo = {
    [ID]: string
    [STATUS]: BadgeStatus
    [MERMAID_PLAN]: string
};

export type Keys = keyof QueryInfo;

export const queryInfoAtom = atom<QueryInfo[]>([]);
