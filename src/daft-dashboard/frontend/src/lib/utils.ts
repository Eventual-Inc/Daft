import { clsx, ClassValue } from "clsx";
import { Geist_Mono } from "next/font/google";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const main = Geist_Mono({
  subsets: ["latin"],
});

export function toDate(serialized_time: string): Date {
  return new Date(Date.parse(serialized_time));
};

export function toHumanReadableDate(serialized_time: number): string {
  // Convert seconds to milliseconds for Date constructor
  const timestamp = Number(serialized_time) * 1000;
  const date = new Date(timestamp);
  // Return a shorter format: "MM/DD HH:MM:SS"
  return date.toLocaleString("en-US", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true
  });
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

export function calculateDurationFromStartTime(startTimeSeconds: number, currentTime?: number): string {
  const now = currentTime || Date.now(); // milliseconds
  const startTime = startTimeSeconds * 1000; // convert to milliseconds
  
  const deltaMilliseconds = now - startTime;
  
  if (deltaMilliseconds < 0) return "0s";
  
  const hours = Math.floor(deltaMilliseconds / (3600 * 1000));
  const minutes = Math.floor((deltaMilliseconds % (3600 * 1000)) / (60 * 1000));
  const seconds = Math.floor((deltaMilliseconds % (60 * 1000)) / 1000);
  
  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  } else {
    return `${seconds}s`;
  }
}
