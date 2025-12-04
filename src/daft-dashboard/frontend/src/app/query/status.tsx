import { useEffect, useState } from "react";
import { main, toHumanReadableDuration } from "@/lib/utils";
import { Ban, CircleX, LoaderCircle } from "lucide-react";
import { QueryStatusName } from "@/hooks/use-queries";
import { AnimatedFish, Naruto } from "@/components/icons";

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

export function Status({
  status,
  start_sec,
  end_sec,
}: {
  status: QueryStatusName;
  start_sec: number;
  end_sec: number | null;
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
    </div>
  );
}
