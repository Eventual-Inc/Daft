"use client";

import React from "react";
import { SearchForm } from "@/components/search-form";
import { Home, TableProperties } from "lucide-react";
import { usePathname, useRouter } from "next/navigation";

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
        title: "Home",
        url: "home",
        icon: Home,
    },
    {
        title: "Queries",
        url: "queries",
        icon: TableProperties,
    },
];

export function AppSidebar() {
    const pathName = usePathname();
    const router = useRouter();

    return (
        <Sidebar>
            <SidebarHeader>
                <SearchForm />
            </SidebarHeader>
            <SidebarContent>
                <SidebarGroup>
                    <SidebarGroupLabel>Application</SidebarGroupLabel>
                    <SidebarGroupContent>
                        <SidebarMenu>
                            {items.map(({ title, url, icon: Icon }) => {
                                console.log({ url });
                                return (
                                <SidebarMenuItem key={title}>
                                    <SidebarMenuButton
                                        asChild
                                        isActive={pathName.startsWith(`/${url}`)}
                                    >
                                        <a href={url}>
                                            <Icon strokeWidth={1.5} />
                                            <span className="font-normal">{title}</span>
                                        </a>
                                    </SidebarMenuButton>
                                </SidebarMenuItem>
                            )})}
                        </SidebarMenu>
                    </SidebarGroupContent>
                </SidebarGroup>
            </SidebarContent>
        </Sidebar>
    );
}
