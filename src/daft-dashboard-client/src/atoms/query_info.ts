import { atom } from 'jotai';

export type QueryInfo = {
    id: string
    'mermaid-plan': string
    'plan-time-start': string
    'plan-time-end': string
};

export type QueryInfoMap = {
    [_: string]: QueryInfo,
};

export type Keys = keyof QueryInfo;

export const queryInfoAtom = atom<QueryInfoMap>({});
