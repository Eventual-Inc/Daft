"use client";

import "./globals.css";

import { AppNavbar } from "@/components/navbar/app-navbar";
import { ServerProvider } from "@/components/server-provider";
import { NotificationsProvider } from "@/components/notifications-provider";
import { main } from "@/lib/utils";
import React from "react";
import { TooltipProvider } from "@/components/ui/tooltip";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body
        className={`${main.className} font-light antialiased w-screen h-screen`}
      >
        <NotificationsProvider>
          <ServerProvider>
            <TooltipProvider delayDuration={250}>
              <div className="flex flex-col h-full">
                <AppNavbar />
                <main className="flex-1 overflow-auto">
                  <div className="p-[20px]">{children}</div>
                </main>
              </div>
            </TooltipProvider>
          </ServerProvider>
        </NotificationsProvider>
      </body>
    </html>
  );
}
