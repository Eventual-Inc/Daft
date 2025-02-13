import React, { forwardRef } from "react";
import { Check, CircleX, LoaderCircle, LucideIcon, TriangleAlert } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { main } from "@/lib/utils";

type BadgeStatus = "pending" | "success" | "failed" | "cancelled";

const StatusBadgeInner = ({ text, icon: Icon, color }: { text: string, icon: LucideIcon, color: string }) => <Badge variant="outline" className={color}>
    <Icon size={13} strokeWidth={3} />
    <div className="px-[2px]" />
    <p className={`${main.className} font-bold`}>
        {text}
    </p>
</Badge>;

const Success = () => <StatusBadgeInner icon={Check} text="Success" color="text-green-600" />;
const Pending = () => <StatusBadgeInner icon={forwardRef((props, ref) => <LoaderCircle ref={ref} className="animate-spin" {...props} />)} text="Pending" color="text-yellow-600" />;
const Failure = () => <StatusBadgeInner icon={CircleX} text="Failure" color="text-red-600" />;
const Cancelled = () => <StatusBadgeInner icon={TriangleAlert} text="Cancelled" color="text-gray-400" />;

export default function StatusBadge({ status }: { status: BadgeStatus }) {
    if (status === "success")
        return Success();
    else if (status === "pending")
        return Pending();
    else if (status === "failed")
        return Failure();
    else
        return Cancelled();
};

export type { BadgeStatus };
