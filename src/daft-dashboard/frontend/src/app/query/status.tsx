import { useEffect, useState } from "react";
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

  const currDuration = Math.round(currentTime / 1000 - start_sec);
  return (
    <div className={`${main.className} text-sm font-mono text-zinc-400`}>
      {toHumanReadableDuration(currDuration)}
    </div>
  );
};

// Custom status component that shows only status without duration text
const StatusIcon = ({ status }: { status: QueryStatusName }) => {
  const getStatusDisplay = () => {
    switch (status) {
      case "Pending":
        return {
          icon: (
            <LoaderCircle size={16} strokeWidth={3} className="text-chart-1" />
          ),
          label: "Waiting to Start",
          textColor: "text-chart-1",
        };
      case "Optimizing":
        return {
          icon: (
            <LoaderCircle
              size={16}
              strokeWidth={3}
              className="text-orange-500"
            />
          ),
          label: "Optimizing",
          textColor: "text-orange-500",
        };
      case "Setup":
        return {
          icon: (
            <LoaderCircle
              size={16}
              strokeWidth={3}
              className="text-magenta-500"
            />
          ),
          label: "Setting Up Runner",
          textColor: "text-magenta-500",
        };
      case "Executing":
        return {
          icon: <AnimatedFish />,
          label: "Running",
          textColor: "text-(--daft-accent)",
        };
      case "Finalizing":
        return {
          icon: (
            <LoaderCircle size={16} strokeWidth={3} className="text-blue-500" />
          ),
          label: "Finalizing Query",
          textColor: "text-blue-500",
        };
      case "Finished":
        return {
          icon: <Naruto />,
          label: "Finished",
          textColor: "text-green-500",
        };
      case "Failed":
        return {
          icon: <CircleX size={16} strokeWidth={3} className="text-red-500" />,
          label: "Failed",
          textColor: "text-red-500",
        };
      case "Canceled":
        return {
          icon: <Ban size={16} strokeWidth={3} className="text-gray-500" />,
          label: "Canceled",
          textColor: "text-gray-500",
        };
      case "Dead":
        return {
          icon: <Skull size={16} strokeWidth={3} className="text-gray-500" />,
          label: "Dead",
          textColor: "text-gray-500",
        };
      default:
        return {
          icon: (
            <LoaderCircle size={16} strokeWidth={3} className="text-chart-2" />
          ),
          label: "Unknown",
          textColor: "text-chart-2",
        };
    }
  };

  const statusDisplay = getStatusDisplay();

  return (
    <div className="flex items-center justify-center gap-x-4 w-full">
      <div className="scale-150">{statusDisplay.icon}</div>
      <span
        className={`${main.className} ${statusDisplay.textColor} font-bold text-4xl`}
      >
        {statusDisplay.label}
      </span>
    </div>
  );
};

const LastHeartbeat = ({
  last_heartbeat_sec,
}: {
  last_heartbeat_sec: number;
}) => {
  const [currentTime, setCurrentTime] = useState(() => Date.now());
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  // Note this is may be inaccurate if the browser's and dashboard server's
  // clocks are out of sync, since currentTime uses the browser's clock whereas
  // last_heartbeat_sec uses the server's clock.
  // However it should be unaffected by time zone differences since both timestamps
  // are just seconds-since-epoch.
  const ago = Math.round(currentTime / 1000 - last_heartbeat_sec);

  if (ago < 10) return null;

  if (ago > 30) {
    return (
      <div className={`${main.className} text-xs font-mono text-red-400`}>
        Query unresponsive (last heartbeat: {toHumanReadableDate(last_heartbeat_sec)})
      </div>
    );
  }

  return (
    <div className={`${main.className} text-xs font-mono text-zinc-500`}>
      Last heartbeat: {toHumanReadableDuration(ago)} ago
    </div>
  );
};

const ErrorModal = ({ message }: { message: string }) => {
  return (
    <Dialog.Root>
      <Dialog.Trigger asChild>
        <button className="flex items-center gap-x-1.5 px-2.5 py-1 rounded border border-red-700 text-red-400 hover:bg-red-950 hover:border-red-500 transition-colors text-xs font-mono">
          <AlertTriangle size={12} />
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
  return (
    <div className="flex flex-col items-center justify-center h-full space-y-4">
      <StatusIcon status={status} />
      {end_sec ? (
        <div
          className={`${main.className} text-md font-mono text-bold text-zinc-300`}
        >
          {toHumanReadableDuration(end_sec - start_sec)}
        </div>
      ) : (
        <Timer start_sec={start_sec} />
      )}
      {status === "Failed" && errorMessage && (
        <ErrorModal message={errorMessage} />
      )}
      {last_heartbeat_sec ? (
        <LastHeartbeat last_heartbeat_sec={last_heartbeat_sec} />
      ) : null}
    </div>
  );
}
