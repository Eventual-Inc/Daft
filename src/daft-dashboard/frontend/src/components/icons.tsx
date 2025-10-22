import Image from "next/image";
import { Fish } from "lucide-react";
import FishCake from "@/public/fish-cake-filled.svg";

export function AnimatedFish() {
  return (
    <div className="fish-container">
      <Fish size={20} strokeWidth={2} className="fish-swim" />
    </div>
  );
}

export const Naruto = () => (
  <Image
    src={FishCake}
    alt="Fish Cake"
    className="animate-[spin_5s_linear_none]"
    height={20}
    width={20}
  />
);
