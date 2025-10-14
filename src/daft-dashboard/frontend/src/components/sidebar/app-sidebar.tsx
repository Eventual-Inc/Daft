"use client";

import React from "react";
import { TableProperties, ExternalLink } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";

import Logo from "./logo";
import { ConnectionStatus } from "./connection-status";
import { useActiveQueries } from "@/hooks/use-queries";
import { BookOpenTextIcon } from "../ui/BookOpenTextIcon";
import { AnimatedFish } from "../icons";

export function AppSidebar() {
  const pathName = usePathname();
  const { activeQueries } = useActiveQueries();

  return (
    <div
      data-slot="sidebar"
      className="bg-sidebar text-sidebar-foreground flex h-full flex-col"
    >
      <SidebarHeader className="p-5">
        <Logo />
      </SidebarHeader>

      <SidebarContent>
        <SidebarMenu>
          <SidebarMenuItem key="docs">
            <SidebarMenuButton
              asChild
              className="hover:bg-zinc-600 transition-colors rounded-none px-4"
            >
              <a
                href="https://docs.daft.ai"
                target="_blank"
                rel="noopener noreferrer"
              >
                <BookOpenTextIcon size={16} />
                <span className="font-mono">Docs</span>
                <ExternalLink />
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>

          <SidebarMenuItem key="all-queries">
            <SidebarMenuButton
              asChild
              isActive={pathName.startsWith(`/queries`)}
              className={`transition-colors rounded-none px-4 ${
                pathName.startsWith(`/queries`)
                  ? "bg-[#ff00ff]/80 text-white hover:bg-[#ff00ff]/60"
                  : "hover:bg-zinc-600"
              }`}
            >
              <Link href="/queries">
                <TableProperties />
                <span className="font-mono font-bold">All Queries</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>

        <SidebarGroup>
          <SidebarGroupLabel>Active Queries</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {activeQueries.length === 0 ? (
                <SidebarMenuItem>
                  <SidebarMenuButton disabled>
                    <span className="font-mono text-sm text-muted-foreground">
                      No active queries
                    </span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ) : (
                activeQueries.map(query => (
                  <SidebarMenuItem key={query.id} className="hover:bg-zinc-600">
                    <SidebarMenuButton
                      asChild
                      className="transition-colors rounded-none"
                    >
                      <Link href={`/query?id=${query.id}`} className="">
                        <div className="flex items-center gap-2 w-full h-[40px]">
                          <AnimatedFish />
                          <span className="font-mono text-sm truncate flex-1">
                            {query.id}
                          </span>
                        </div>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))
              )}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="p-0">
        <ConnectionStatus />
      </SidebarFooter>
    </div>
  );
}
