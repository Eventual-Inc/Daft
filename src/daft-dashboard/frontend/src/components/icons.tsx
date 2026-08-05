import Image from "next/image";
import { Fish } from "lucide-react";
import FishCake from "@/public/fish-cake-filled.svg";

export function AnimatedFish({ animated = true }: { animated?: boolean }) {
  return (
    <div className={animated ? "fish-container" : ""}>
      <Fish size={20} strokeWidth={2} className={animated ? "fish-swim" : ""} />
    </div>
  );
}

export const Naruto = ({ animated = true }: { animated?: boolean }) => (
  <Image
    src={FishCake}
    alt="Fish Cake"
    className={animated ? "animate-[spin_5s_linear_none]" : ""}
    height={20}
    width={20}
  />
);
