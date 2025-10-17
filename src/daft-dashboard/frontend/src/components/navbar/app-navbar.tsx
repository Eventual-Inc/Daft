"use client";

import React from "react";
import { TableProperties, ExternalLink } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Button } from "@/components/ui/button";

import Logo from "./logo";
import { ConnectionStatus } from "./connection-status";
import { useActiveQueries } from "@/hooks/use-queries";
import { BookOpenTextIcon } from "../ui/BookOpenTextIcon";
import { Badge } from "../ui/badge";
import { NotificationSettings } from "./notification-settings";

export function AppNavbar() {
  const pathName = usePathname();
  const { activeQueries } = useActiveQueries();

  return (
    <nav className="bg-background border-b border-zinc-700 flex items-center justify-between h-[50px]">
      {/* Left side - Logo, All Queries, and Docs */}
      <div className="flex items-center gap-5 px-6 h-full">
        <Logo />

        <div
          className={`h-full border-(--daft-accent) ${pathName.startsWith(`/queries`) ? "border-b" : ""}`}
        >
          <Button
            asChild
            variant="ghost"
            className="h-full transition-colors rounded-none hover:bg-zinc-700"
          >
            <Link
              href="/queries"
              className="flex items-center gap-2 h-full px-4"
            >
              <TableProperties size={16} />
              <span className="font-mono font-bold">All Queries</span>
              <Badge className="bg-(--daft-accent) h- min-w-5 rounded-full px-1.5 font-mono tabular-nums">
                {activeQueries.length}
              </Badge>
            </Link>
          </Button>
        </div>

        <Button
          asChild
          variant="ghost"
          className="h-full rounded-none hover:bg-zinc-700 transition-colors"
        >
          <a
            href="https://docs.daft.ai"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2"
          >
            <BookOpenTextIcon size={16} />
            <span className="font-mono">Docs</span>
            <ExternalLink size={14} />
          </a>
        </Button>
      </div>

      {/* Right side - Connection status */}
      <div className="h-full flex items-center gap-1">
        <NotificationSettings />
        <ConnectionStatus />
      </div>
    </nav>
  );
}
