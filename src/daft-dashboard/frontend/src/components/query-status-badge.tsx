import React from "react";
import { Check, CircleX, LoaderCircle } from "lucide-react";
import { main } from "@/lib/utils";

type QueryStatus = "Running" | "Completed" | "Failed";

interface QueryStatusBadgeProps {
  status: QueryStatus;
}

const QueryStatusBadgeInner = ({ 
  text, 
  icon: Icon, 
  textColor
}: { 
  text: string; 
  icon: React.ComponentType<{ size?: number; strokeWidth?: number; className?: string }>; 
  textColor: string;
}) => (
  <div className={`${textColor} flex items-center px-3 py-2`}>
    <Icon size={16} strokeWidth={2.5} />
    <div className="px-[4px]" />
    <p className={`${main.className} font-semibold text-sm`}>
      {text}
    </p>
  </div>
);

const Running = () => (
  <QueryStatusBadgeInner 
    icon={LoaderCircle} 
    text="Running" 
    textColor="text-chart-2"
  />
);

const Completed = () => (
  <QueryStatusBadgeInner 
    icon={Check} 
    text="Completed" 
    textColor="text-chart-3"
  />
);

const Failed = () => (
  <QueryStatusBadgeInner 
    icon={CircleX} 
    text="Failed" 
    textColor="text-destructive"
  />
);

export default function QueryStatusBadge({ status }: QueryStatusBadgeProps) {
  switch (status) {
  case "Running":
    return <Running />;
  case "Completed":
    return <Completed />;
  case "Failed":
    return <Failed />;
  default:
    return <Failed />; // Default to failed for unknown statuses
  }
}

export type { QueryStatus };
