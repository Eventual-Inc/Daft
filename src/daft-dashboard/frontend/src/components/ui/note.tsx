"use client";

import React from "react";
import { cn } from "@/lib/utils";
import { Info, AlertTriangle, CheckCircle, XCircle } from "lucide-react";

interface NoteProps {
  children: React.ReactNode;
  type?: "info" | "warning" | "success" | "error";
  className?: string;
}

const noteConfig = {
  info: {
    icon: Info,
    bgColor: "bg-blue-500/10",
    borderColor: "border-blue-500/20",
    iconColor: "text-blue-400",
    iconBg: "bg-blue-500/20",
  },
  warning: {
    icon: AlertTriangle,
    bgColor: "bg-yellow-500/10",
    borderColor: "border-yellow-500/20",
    iconColor: "text-yellow-400",
    iconBg: "bg-yellow-500/20",
  },
  success: {
    icon: CheckCircle,
    bgColor: "bg-green-500/10",
    borderColor: "border-green-500/20",
    iconColor: "text-green-400",
    iconBg: "bg-green-500/20",
  },
  error: {
    icon: XCircle,
    bgColor: "bg-red-500/10",
    borderColor: "border-red-500/20",
    iconColor: "text-red-400",
    iconBg: "bg-red-500/20",
  },
};

export function Note({ children, type = "info", className }: NoteProps) {
  const config = noteConfig[type];
  const Icon = config.icon;

  return (
    <div
      className={cn(
        "flex gap-3 rounded-lg border p-4 text-sm",
        config.bgColor,
        config.borderColor,
        className
      )}
    >
      <div
        className={cn(
          "flex h-5 w-5 shrink-0 items-center justify-center rounded-full",
          config.iconBg
        )}
      >
        <Icon className={cn("h-3 w-3", config.iconColor)} />
      </div>
      <div className="text-foreground [&>p]:leading-relaxed [&>p:not(:first-child)]:mt-2">
        {children}
      </div>
    </div>
  );
}
