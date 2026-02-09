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
  const date = toDate(epoch_time_secs);
  const year = date.getFullYear();
  const month = (date.getMonth() + 1).toString().padStart(2, "0");
  const day = date.getDate().toString().padStart(2, "0");
  const hours = date.getHours().toString().padStart(2, "0");
  const minutes = date.getMinutes().toString().padStart(2, "0");
  const seconds = date.getSeconds().toString().padStart(2, "0");
  const ms = date.getMilliseconds().toString().padStart(3, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${ms}`;
}

export function toHumanReadableDuration(duration_sec: number): string {
  let working_sec = duration_sec;

  const days = Math.floor(working_sec / 86400);
  working_sec = working_sec - days * 86400;
  const hours = Math.floor(working_sec / 3600);
  working_sec = working_sec - hours * 3600;
  const minutes = Math.floor(working_sec / 60);
  working_sec = working_sec - minutes * 60;
  const seconds = Math.round(working_sec * 1000) / 1000;

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

export function getEngineName(runner: string): string {
  if (runner.includes("Native")) {
    return "Swordfish";
  }
  if (runner.includes("Ray")) {
    return "Flotilla";
  }
  return runner;
}
