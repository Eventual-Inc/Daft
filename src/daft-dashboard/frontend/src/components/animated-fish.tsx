import { Fish } from "lucide-react";

export default function AnimatedFish() {
  return (
    <div className="fish-container">
      <Fish size={20} strokeWidth={2} className="fish-swim" />
    </div>
  );
}
