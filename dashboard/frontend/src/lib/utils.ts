import { clsx, ClassValue } from "clsx";
import { Geist } from "next/font/google";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs));
}

export const main = Geist({
    subsets: ["latin"],
});

export function toDate(serialized_time: string): Date {
    return new Date(Date.parse(serialized_time));
};

export function toHumanReadableDate(serialized_time: string): string {
    return toDate(serialized_time).toLocaleTimeString();
}

export function delta(start: string, end: string): string {
    const startDate = toDate(start);
    const endDate = toDate(end);

    const startTime = startDate.getTime();
    const endTime = endDate.getTime();

    /// Ideally, this case should never arise.
    if (startTime > endTime) return "n/a";

    let deltaMilliseconds = endTime - startTime;

    const hours = Math.floor(deltaMilliseconds / (3600 * 1000));
    deltaMilliseconds = deltaMilliseconds - (hours * (3600 * 1000));

    const minutes = Math.floor(deltaMilliseconds / (60 * 1000));
    deltaMilliseconds = deltaMilliseconds - (minutes * (60 * 1000));

    const seconds = deltaMilliseconds / 1000;

    return `${hours}h ${minutes}m ${seconds}s`;
}
