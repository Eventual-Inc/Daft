"use client";

import { useEffect, useState, type ReactNode } from "react";
import { main, toHumanReadableDuration, toHumanReadableDate } from "@/lib/utils";
import { AlertTriangle, Ban, CircleX, LoaderCircle, Skull, X } from "lucide-react";
import { QueryStatusName } from "@/hooks/use-queries";
import { AnimatedFish, Naruto } from "@/components/icons";
import * as Dialog from "@radix-ui/react-dialog";

const Timer = ({ start_sec }: { start_sec: number }) => {
  const [currentTime, setCurrentTime] = useState(() => Date.now());
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <span className={`${main.className} text-sm font-mono text-zinc-400`}>
      {toHumanReadableDuration(Math.round(currentTime / 1000 - start_sec))}
    </span>
  );
};

const LastHeartbeat = ({ last_heartbeat_sec }: { last_heartbeat_sec: number }) => {
  const [currentTime, setCurrentTime] = useState(() => Date.now());
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  // Note: may be inaccurate if browser and dashboard server clocks are out of sync.
  const ago = Math.round(currentTime / 1000 - last_heartbeat_sec);

  if (ago < 10) return null;

  if (ago > 30) {
    return (
      <div className={`${main.className} text-xs font-mono text-red-400`}>
        unresponsive (last heartbeat: {toHumanReadableDate(last_heartbeat_sec)})
      </div>
    );
  }

  return (
    <div className={`${main.className} text-xs font-mono text-zinc-500`}>
      heartbeat {toHumanReadableDuration(ago)} ago
    </div>
  );
};

const ErrorModal = ({ message }: { message: string }) => (
  <Dialog.Root>
    <Dialog.Trigger asChild>
      <button className="w-fit flex items-center gap-x-1.5 px-2 py-0.5 rounded border border-red-700 text-red-400 hover:bg-red-950 hover:border-red-500 transition-colors text-xs font-mono">
        <AlertTriangle size={11} />
        View Error
      </button>
    </Dialog.Trigger>
    <Dialog.Portal>
      <Dialog.Overlay className="fixed inset-0 z-50 bg-black/70 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0" />
      <Dialog.Content className="fixed left-1/2 top-1/2 z-50 w-full max-w-2xl -translate-x-1/2 -translate-y-1/2 rounded-lg border border-zinc-700 bg-zinc-900 p-6 shadow-xl data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95">
        <div className="flex items-center justify-between mb-4">
          <Dialog.Title className={`${main.className} flex items-center gap-x-2 text-red-400 font-semibold text-base`}>
            <CircleX size={16} strokeWidth={2.5} />
            Query Error
          </Dialog.Title>
          <Dialog.Close className="rounded opacity-70 hover:opacity-100 transition-opacity text-zinc-400 hover:text-white">
            <X size={16} />
            <span className="sr-only">Close</span>
          </Dialog.Close>
        </div>
        <Dialog.Description asChild>
          <pre className={`${main.className} whitespace-pre-wrap break-words text-xs text-zinc-300 bg-zinc-950 rounded border border-zinc-800 p-4 max-h-96 overflow-y-auto`}>
            {message}
          </pre>
        </Dialog.Description>
      </Dialog.Content>
    </Dialog.Portal>
  </Dialog.Root>
);

const STATUS_CONFIG: Record<QueryStatusName, { icon: ReactNode; label: string; textColor: string }> = {
  Pending:    { icon: <LoaderCircle size={15} strokeWidth={3} className="text-chart-1 animate-spin" />,   label: "Waiting to Start",   textColor: "text-chart-1" },
  Optimizing: { icon: <LoaderCircle size={15} strokeWidth={3} className="text-orange-500 animate-spin" />, label: "Optimizing",         textColor: "text-orange-500" },
  Setup:      { icon: <LoaderCircle size={15} strokeWidth={3} className="text-magenta-500 animate-spin" />,label: "Setting Up Runner",  textColor: "text-magenta-500" },
  Executing:  { icon: <AnimatedFish />,                                                                     label: "Running",            textColor: "text-(--daft-accent)" },
  Finalizing: { icon: <LoaderCircle size={15} strokeWidth={3} className="text-blue-500 animate-spin" />,   label: "Finalizing",         textColor: "text-blue-500" },
  Finished:   { icon: <Naruto />,                                                                           label: "Finished",           textColor: "text-green-500" },
  Failed:     { icon: <CircleX size={15} strokeWidth={3} className="text-red-500" />,                      label: "Failed",             textColor: "text-red-500" },
  Canceled:   { icon: <Ban size={15} strokeWidth={3} className="text-zinc-500" />,                         label: "Canceled",           textColor: "text-zinc-400" },
  Dead:       { icon: <Skull size={15} strokeWidth={3} className="text-zinc-500" />,                       label: "Dead",               textColor: "text-zinc-400" },
};

export function Status({
  status,
  start_sec,
  end_sec,
  last_heartbeat_sec,
  errorMessage,
}: {
  status: QueryStatusName;
  start_sec: number;
  end_sec: number | null;
  last_heartbeat_sec: number | null;
  errorMessage?: string;
}) {
  const { icon, label, textColor } = STATUS_CONFIG[status] ?? STATUS_CONFIG.Pending;

  return (
    <div className="flex flex-col gap-y-1.5">
      <div className="flex items-center gap-x-2 whitespace-nowrap">
        {icon}
        <span className={`${main.className} ${textColor} font-bold text-base`}>
          {label}
        </span>
        <span className="text-zinc-600">·</span>
        {end_sec != null ? (
          <span className={`${main.className} text-sm font-mono text-zinc-400`}>
            {toHumanReadableDuration(end_sec - start_sec)}
          </span>
        ) : (
          <Timer start_sec={start_sec} />
        )}
      </div>
      {last_heartbeat_sec && <LastHeartbeat last_heartbeat_sec={last_heartbeat_sec} />}
      {status === "Failed" && errorMessage && (
        <ErrorModal message={errorMessage} />
      )}
    </div>
  );
}
