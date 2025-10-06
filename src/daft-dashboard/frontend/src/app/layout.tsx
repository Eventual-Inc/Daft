"use client";

import { AppSidebar } from "@/components/sidebar/app-sidebar";
import "./globals.css";
import { ServerProvider } from "@/components/server-provider";
import { main } from "@/lib/utils";
import React from "react";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";

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
          <ResizablePanelGroup direction="horizontal">
            <ResizablePanel defaultSize={15} minSize={10} maxSize={40}>
              <AppSidebar />
            </ResizablePanel>
            <ResizableHandle withHandle className="bg-zinc-700" />
            <ResizablePanel defaultSize={85} minSize={50} maxSize={90}>
              <main className="w-full h-full overflow-auto">
                <div className="p-[20px]">{children}</div>
              </main>
            </ResizablePanel>
          </ResizablePanelGroup>
        </ServerProvider>
      </body>
    </html>
  );
}
