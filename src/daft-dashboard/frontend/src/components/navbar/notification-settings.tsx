"use client";

import { useState } from "react";
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

export function NotificationSettings() {
  const [notifyOnQueryStart, setNotifyOnQueryStart] = useState(false);
  const [notifyOnQueryEnd, setNotifyOnQueryEnd] = useState(false);

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
          <DropdownMenuLabel>Notifications</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuCheckboxItem
            checked={notifyOnQueryStart}
            onCheckedChange={setNotifyOnQueryStart}
          >
            On Query Start
          </DropdownMenuCheckboxItem>
          <DropdownMenuCheckboxItem
            checked={notifyOnQueryEnd}
            onCheckedChange={setNotifyOnQueryEnd}
          >
            On Query End
          </DropdownMenuCheckboxItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
