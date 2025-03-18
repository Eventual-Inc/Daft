"use client";

import React from "react";
import { SearchForm } from "@/components/search-form";
import { TableProperties } from "lucide-react";
import { usePathname } from "next/navigation";

import {
    Sidebar,
    SidebarContent,
    SidebarGroup,
    SidebarGroupContent,
    SidebarGroupLabel,
    SidebarHeader,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
} from "@/components/ui/sidebar";

const items = [
    {
        title: "Queries",
        url: "queries",
        icon: TableProperties,
    },
];

export function AppSidebar() {
    const pathName = usePathname();

    return (
        <Sidebar>
            <SidebarHeader className="py-4">
                <SearchForm />
            </SidebarHeader>
            <SidebarContent>
                <SidebarGroup>
                    <SidebarGroupLabel>Application</SidebarGroupLabel>
                    <SidebarGroupContent>
                        <SidebarMenu>
                            {items.map(({ title, url, icon: Icon }) => (
                                <SidebarMenuItem key={title}>
                                    <SidebarMenuButton
                                        asChild
                                        isActive={pathName.startsWith(`/${url}`)}
                                    >
                                        <a href={`/${url}`}>
                                            <Icon strokeWidth={1.5} />
                                            <span className="font-normal">{title}</span>
                                        </a>
                                    </SidebarMenuButton>
                                </SidebarMenuItem>
                            ))}
                        </SidebarMenu>
                    </SidebarGroupContent>
                </SidebarGroup>
            </SidebarContent>
        </Sidebar>
    );
}
