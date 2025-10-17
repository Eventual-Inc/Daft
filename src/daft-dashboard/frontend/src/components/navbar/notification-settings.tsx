"use client";

import { Button } from "../ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "../ui/dropdown-menu";
import { Bell } from "lucide-react";
import { useNotifications } from "../notifications-provider";

export function NotificationSettings() {
  const { onQueryStart, onQueryEnd, setOnQueryStart, setOnQueryEnd } =
    useNotifications();

  return (
    <div>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="destructive"
            className="w-10 h-10 hover:cursor-pointer group"
          >
            <Bell
              size={18}
              className="transition-transform duration-200 group-hover:rotate-[25deg]"
            />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56 bg-zinc-800">
          <DropdownMenuLabel className="font-mono">
            Notifications
          </DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuCheckboxItem
            className="font-mono hover:cursor-pointer hover:bg-zinc-700"
            checked={onQueryStart}
            onCheckedChange={setOnQueryStart}
          >
            On Query Start
          </DropdownMenuCheckboxItem>
          <DropdownMenuCheckboxItem
            className="font-mono hover:cursor-pointer hover:bg-zinc-700"
            checked={onQueryEnd}
            onCheckedChange={setOnQueryEnd}
          >
            On Query End
          </DropdownMenuCheckboxItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
