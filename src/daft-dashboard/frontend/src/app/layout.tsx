"use client";

import { TopNavigation } from "@/components/top-navigation";
import "./globals.css";
import { main } from "@/lib/utils";
import React from "react";

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={`${main.className} font-light antialiased bg-daft-bg-primary text-daft-text-primary`}>
        <TopNavigation />
        <main className="flex-1">
          <div className="p-8 bg-daft-bg-primary">
            {children}
          </div>
        </main>
      </body>
    </html>
  );
}
