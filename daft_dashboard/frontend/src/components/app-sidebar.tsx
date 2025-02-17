"use client";

import React from "react";
import { SearchForm } from "@/components/search-form";
import { Home, TableProperties } from "lucide-react";
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
    SidebarFooter,
} from "@/components/ui/sidebar";
import { Button } from "./ui/button";

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
            <SidebarFooter className="px-4 py-4">
                <Button variant="destructive" onClick={async () => {
                    await fetch("http://localhost:3238/api/shutdown", { method: "POST" });
                }}>
                    Shutdown
                </Button>
            </SidebarFooter>
        </Sidebar>
    );
}
