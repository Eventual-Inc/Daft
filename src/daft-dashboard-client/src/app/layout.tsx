"use client";

import { AppSidebar } from "@/components/app-sidebar";
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
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

    return (
        <html lang="en">
            <body className={`${main.className} font-light antialiased`}>
                <SidebarProvider>
                    <AppSidebar />
                    <SidebarInset>
                        <header className="flex h-16 shrink-0 items-center gap-2 border-b px-4">
                            <SidebarTrigger className="-ml-1" />
                            <Separator orientation="vertical" className="mr-2 h-4" />
                            <Breadcrumb>
                                <BreadcrumbList>
                                    {
                                        pathSegments.map((pathSegment, index) => (
                                            index === (pathSegments.length - 1) ?
                                                <BreadcrumbItem key={index}>
                                                    <BreadcrumbPage className="capitalize">{pathSegment}</BreadcrumbPage>
                                                </BreadcrumbItem>
                                                :
                                                <div key={index} className="flex flex-row items-center gap-3">
                                                    <BreadcrumbItem>
                                                        <BreadcrumbPage className="capitalize text-gray-500">{pathSegment}</BreadcrumbPage>
                                                    </BreadcrumbItem>
                                                    <BreadcrumbSeparator className="hidden md:block" />
                                                </div>
                                        ))
                                    }
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
