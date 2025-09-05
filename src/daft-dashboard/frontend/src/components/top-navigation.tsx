"use client";

import React from "react";
import { TableProperties, ExternalLink } from "lucide-react";
import { usePathname } from "next/navigation";
import Link from "next/link";

import { cn } from "@/lib/utils";
import Logo from "./logo";
import { ConnectionStatus } from "./connection-status";

const navigationItems = [
  {
    title: "Queries",
    url: "/queries",
    icon: TableProperties,
  },
  {
    title: "Docs",
    url: "https://docs.daft.ai",
    icon: ExternalLink,
    external: true,
  },
];

export function TopNavigation() {
  const pathname = usePathname();

  return (
    <header className="sticky top-0 z-50 w-full border-b border-daft-border-primary bg-daft-bg-primary/95 backdrop-blur supports-[backdrop-filter]:bg-daft-bg-primary/60">
      <div className="w-full flex h-20 items-center justify-between px-6">
        <div className="flex items-center space-x-8">
          <Logo />
          
          {/* Navigation Links */}
          <nav className="flex items-center space-x-6">
            {navigationItems.map(({ title, url, icon: Icon, external }) => (
              external ? (
                <a
                  key={title}
                  href={url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center space-x-3 text-base font-medium transition-colors hover:text-daft-accent px-4 py-3 rounded-lg text-daft-text-secondary hover:bg-daft-bg-accent"
                >
                  <Icon className="h-5 w-5" />
                  <span>{title}</span>
                </a>
              ) : (
                <Link
                  key={title}
                  href={url}
                  className={cn(
                    "flex items-center space-x-3 text-base font-medium transition-colors hover:text-daft-accent px-4 py-3 rounded-lg",
                    pathname.startsWith(url)
                      ? "text-daft-accent bg-daft-bg-accent"
                      : "text-daft-text-secondary hover:bg-daft-bg-accent"
                  )}
                >
                  <Icon className="h-5 w-5" />
                  <span>{title}</span>
                </Link>
              )
            ))}
          </nav>
        </div>

        {/* Right side - Connection Status */}
        <div className="flex items-center">
          <ConnectionStatus />
        </div>
      </div>
    </header>
  );
}
