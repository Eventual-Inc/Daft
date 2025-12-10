"use client";

import { useState, useEffect } from "react";
import { Ban, CircleX, LoaderCircle, Skull } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { main, toHumanReadableDuration } from "@/lib/utils";
import {
  QueryStatus,
  PlanningStatus,
  ExecutingStatus,
  FinishedStatus,
  FailedStatus,
  CanceledStatus,
  DeadStatus
} from "@/hooks/use-queries";
import { AnimatedFish, Naruto } from "@/components/icons";

interface StatusBadgeProps {
  state: QueryStatus;
}

const StatusBadgeInner = ({
  label,
  text,
  icon,
  textColor,
  tooltipText,
}: {
  label: string;
  text?: string;
  icon: React.ReactNode;
  textColor: string;
  tooltipText?: string | null;
}) => {
  const badge = (
    <div className="flex items-center py-2 gap-x-2">
      {icon}
      <div className="px-[0.1px]" />
      <span className={`${main.className} ${textColor} font-bold text-sm`}>
        {label}
      </span>
      <span className={`${main.className} font-semibold text-sm`}>{text}</span>
    </div>
  );

  if (!tooltipText) {
    return badge;
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>{badge}</TooltipTrigger>
        <TooltipContent
          side="bottom"
          align="center"
          sideOffset={5}
          className="max-w-xs rounded-lg bg-gray-900 px-3 py-2 text-sm text-white shadow-lg dark:bg-gray-800"
        >
          <p>{tooltipText}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

const Pending = () => (
  <StatusBadgeInner
    icon={<LoaderCircle size={16} strokeWidth={3} className="text-chart-1" />}
    label="Waiting to Start"
    textColor="text-chart-1"
  />
);

const Planning = ({ state }: { state: PlanningStatus }) => {
  const [currentTime, setCurrentTime] = useState(() => Date.now());

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const duration = Math.round(currentTime / 1000 - state.plan_start_sec);

  return (
    <StatusBadgeInner
      icon={
        <LoaderCircle size={16} strokeWidth={3} className="text-orange-500" />
      }
      label="Optimizing"
      text={` for ${toHumanReadableDuration(duration)}`}
      textColor="text-orange-500"
    />
  );
};

const Setup = () => (
  <StatusBadgeInner
    icon={
      <LoaderCircle size={16} strokeWidth={3} className="text-magenta-500" />
    }
    label="Setting Up Runner"
    textColor="text-magenta-500"
  />
);

const Running = ({ state }: { state: ExecutingStatus }) => {
  const [currentTime, setCurrentTime] = useState(() => Date.now());

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const duration = Math.round(currentTime / 1000 - state.exec_start_sec);

  return (
    <StatusBadgeInner
      icon={<AnimatedFish />}
      label="Running"
      text={` for ${toHumanReadableDuration(duration)}`}
      textColor="text-(--daft-accent)"
    />
  );
};

const Finalizing = () => (
  <StatusBadgeInner
    icon={<LoaderCircle size={16} strokeWidth={3} className="text-blue-500" />}
    label="Finalizing Query"
    textColor="text-blue-500"
  />
);

const Finished = ({ state }: { state: FinishedStatus }) => (
  <StatusBadgeInner
    icon={<Naruto />}
    label="Finished"
    text={`in ${toHumanReadableDuration(state.duration_sec)}`}
    textColor="text-green-500"
  />
);

const Canceled = ({ state }: { state: CanceledStatus }) => (
  <StatusBadgeInner
    icon={<Ban size={16} strokeWidth={3} className="text-gray-500" />}
    label="Canceled"
    textColor="text-gray-500"
    tooltipText={state.message}
  />
);

const Failed = ({ state }: { state: FailedStatus }) => (
  <StatusBadgeInner
    icon={<CircleX size={16} strokeWidth={3} className="text-red-500" />}
    label="Failed"
    textColor="text-red-500"
    tooltipText={state.message}
  />
);

const Dead = () => (
  <StatusBadgeInner
    icon={<Skull size={16} strokeWidth={3} className="text-gray-500" />}
    label="Dead"
    textColor="text-gray-500"
  />
);

const Unknown = () => (
  <StatusBadgeInner
    icon={<LoaderCircle size={16} strokeWidth={3} className="text-chart-2" />}
    label="Unknown"
    textColor="text-chart-2"
  />
);

export default function Status({ state }: StatusBadgeProps) {
  switch (state.status) {
    case "Pending":
      return <Pending />;
    case "Optimizing":
      return <Planning state={state} />;
    case "Setup":
      return <Setup />;
    case "Executing":
      return <Running state={state} />;
    case "Finalizing":
      return <Finalizing />;
    case "Finished":
      return <Finished state={state} />;
    case "Failed":
      return <Failed state={state} />;
    case "Canceled":
      return <Canceled state={state} />;
    case "Dead":
      return <Dead />;
    default:
      return <Unknown />;
  }
}
