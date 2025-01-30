import { clsx, ClassValue } from "clsx";
import { Geist } from "next/font/google";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs));
}

export const main = Geist({
    weight: "400",
    subsets: ["latin"],
});
