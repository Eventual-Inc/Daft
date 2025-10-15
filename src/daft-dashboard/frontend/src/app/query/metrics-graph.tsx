"use client";

import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import { ChartContainer, ChartConfig, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { SidebarContent, SidebarHeader } from "@/components/ui/sidebar";

const data = [
  { name: "Page A", value: 100 },
  { name: "Page B", value: 200 },
  { name: "Page C", value: 300 },
  { name: "Page D", value: 400 },
  { name: "Page E", value: 500 },
];

export default function MetricsGraph() {
  return (
    <div className="h-[600px] w-full flex">
      <div
        data-slot="sidebar"
        className="bg-sidebar text-sidebar-foreground flex h-full flex-col w-1/5"
      >
        <SidebarHeader className="p-5 text-center text-lg font-bold">
          Metrics Graph
        </SidebarHeader>

        <SidebarContent>
        </SidebarContent>
      </div>

      <div className="w-[1px] bg-zinc-700" />
      
      <ChartContainer config={{}} className="h-[600px] flex-1">
        <AreaChart data={data}>
          <CartesianGrid />
          <XAxis dataKey="name" tickLine={false} />
          <YAxis dataKey="value" tickLine={false} />
          <ChartTooltip content={<ChartTooltipContent />} />
          <Area dataKey="value" fill="var(--chart-1)" stroke="var(--chart-1)" />
        </AreaChart>
      </ChartContainer>
    </div>
  );
}
