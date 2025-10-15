"use client";

import { AppNavbar } from "@/components/navbar/app-navbar";
import "./globals.css";
import { ServerProvider } from "@/components/server-provider";
import { main } from "@/lib/utils";
import React from "react";

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
        <ServerProvider>
          <div className="flex flex-col h-full">
            <AppNavbar />
            <main className="flex-1 overflow-auto">
              <div className="p-[20px]">{children}</div>
            </main>
          </div>
        </ServerProvider>
      </body>
    </html>
  );
}
