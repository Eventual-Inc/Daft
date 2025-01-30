import React, { forwardRef } from "react";
import { Check, CircleX, LoaderCircle, LucideIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { main } from "@/lib/utils";

type BadgeStatus = "pending" | "success" | "failed";

const StatusBadgeInner = ({ text, icon: Icon, color }: { text: string, icon: LucideIcon, color: string }) => <Badge variant="outline" className={`text-${color}-600`}>
    <Icon size={13} strokeWidth={3} />
    <div className="px-[2px]" />
    <p className={main.className}>
        {text}
    </p>
</Badge>;

const Success = () => <StatusBadgeInner icon={Check} text="Success" color="green" />;
const Pending = () => <StatusBadgeInner icon={forwardRef((props, ref) => <LoaderCircle ref={ref} className="animate-spin" {...props} />)} text="Pending" color="yellow" />;
const Failure = () => <StatusBadgeInner icon={CircleX} text="Failure" color="red" />;

export default function StatusBadge({ status }: { status: BadgeStatus }) {
    if (status === "success")
        return Success();
    else if (status === "pending")
        return Pending();
    else
        return Failure();
};

export type { BadgeStatus };
