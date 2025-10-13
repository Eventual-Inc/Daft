import { clsx, ClassValue } from "clsx";
import { Geist_Mono } from "next/font/google";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const main = Geist_Mono({
  subsets: ["latin"],
});

export function toDate(epoch_time_secs: number): Date {
  return new Date(epoch_time_secs * 1000);
}

export function toHumanReadableDate(epoch_time_secs: number): string {
  return toDate(epoch_time_secs).toLocaleTimeString();
}

export function toHumanReadableDuration(duration_sec: number): string {
  let working_sec = duration_sec;

  const days = Math.floor(working_sec / 86400);
  working_sec = working_sec - days * 86400;
  const hours = Math.floor(working_sec / 3600);
  working_sec = working_sec - hours * 3600;
  const minutes = Math.floor(working_sec / 60);
  working_sec = working_sec - minutes * 60;
  const seconds = working_sec;

  if (duration_sec < 60) {
    return `${seconds}s`;
  } else if (duration_sec < 3600) {
    return `${minutes}m ${seconds}s`;
  } else if (duration_sec < 86400) {
    return `${hours}h ${minutes}m ${seconds}s`;
  } else {
    return `${days}d ${hours}h ${minutes}m ${seconds}s`;
  }
}
