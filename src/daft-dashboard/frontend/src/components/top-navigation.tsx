"use client";

import React from "react";
import { TableProperties } from "lucide-react";
import { usePathname } from "next/navigation";
import Link from "next/link";

import { cn } from "@/lib/utils";

const navigationItems = [
    {
        title: "Queries",
        url: "/queries",
        icon: TableProperties,
    },
];

export function TopNavigation() {
    const pathname = usePathname();

    return (
        <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
            <div className="container flex h-16 items-center justify-between px-4">
                {/* Logo/Brand */}
                <div className="flex items-center space-x-4">
                    <Link href="/" className="flex items-center space-x-2">
                        <span className="text-lg font-semibold">Daft Dashboard</span>
                    </Link>
                </div>

                {/* Navigation Links */}
                <nav className="flex items-center space-x-6">
                    {navigationItems.map(({ title, url, icon: Icon }) => (
                        <Link
                            key={title}
                            href={url}
                            className={cn(
                                "flex items-center space-x-2 text-sm font-medium transition-colors hover:text-primary",
                                pathname.startsWith(url)
                                    ? "text-foreground"
                                    : "text-muted-foreground"
                            )}
                        >
                            <Icon className="h-4 w-4" />
                            <span>{title}</span>
                        </Link>
                    ))}
                </nav>

            </div>
        </header>
    );
}
