"use client";

import { TopNavigation } from "@/components/top-navigation";
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
    BreadcrumbLink,
} from "@/components/ui/breadcrumb";
import "./globals.css";
import { usePathname } from "next/navigation";
import { main } from "@/lib/utils";
import React from "react";

export default function RootLayout({ children }: { children: React.ReactNode }) {
    const pathSegments = usePathname().split("/").filter(segment => segment);

    return (
        <html lang="en">
            <body className={`${main.className} font-light antialiased`}>
                <TopNavigation />
                <main className="flex-1">
                    <div className="border-b px-4 py-3">
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
                                                    <BreadcrumbLink
                                                        className="capitalize text-gray-500"
                                                        href={`/${pathSegments.slice(0, index - 1).join("/")}`}
                                                    >
                                                        {pathSegment}
                                                    </BreadcrumbLink>
                                                </BreadcrumbItem>
                                                <BreadcrumbSeparator className="hidden md:block" />
                                            </div>
                                    ))
                                }
                            </BreadcrumbList>
                        </Breadcrumb>
                    </div>
                    <div className="p-[20px]">
                        {children}
                    </div>
                </main>
            </body>
        </html>
    );
}
