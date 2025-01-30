"use client";

import { AppSidebar } from "@/components/app-sidebar";
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbList,
    BreadcrumbPage,
} from "@/components/ui/breadcrumb";
import { Separator } from "@/components/ui/separator";
import {
    SidebarInset,
    SidebarProvider,
    SidebarTrigger,
} from "@/components/ui/sidebar";
import "./globals.css";
import { usePathname } from "next/navigation";
import { main } from "@/lib/utils";
import React from "react";

export default function RootLayout({ children }: { children: React.ReactNode }) {
    const pathSegments = usePathname().split("/").filter(segment => segment);
    let pathSegment;
    if (pathSegments.length === 0) {
        pathSegment = "Home";
    } else if (pathSegments.length === 1) {
        pathSegment = pathSegments[0];
    } else {
        throw new Error("Invalid path structure");
    }

    return (
        <html lang="en">
            <body className={`${main.className} antialiased`}>
                <SidebarProvider>
                    <AppSidebar />
                    <SidebarInset>
                        <header className="flex h-16 shrink-0 items-center gap-2 border-b px-4">
                            <SidebarTrigger className="-ml-1" />
                            <Separator orientation="vertical" className="mr-2 h-4" />
                            <Breadcrumb>
                                <BreadcrumbList>
                                    <BreadcrumbItem>
                                        <BreadcrumbPage className="capitalize">{pathSegment}</BreadcrumbPage>
                                    </BreadcrumbItem>
                                </BreadcrumbList>
                            </Breadcrumb>
                        </header>
                        {children}
                    </SidebarInset>
                </SidebarProvider>
            </body>
        </html>
    );
}
