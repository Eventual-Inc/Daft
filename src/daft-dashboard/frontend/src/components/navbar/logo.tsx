"use client";

import { useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";



interface LogoProps {
  onComplete?: () => void;
  textColor?: string;
}



const Logo = ({ onComplete, textColor = "text-white" }: LogoProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [animationKey, setAnimationKey] = useState(0);
  const isAnimatingRef = useRef(false);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleAnimationEnd = () => {
      onComplete?.();
      setTimeout(() => {
        isAnimatingRef.current = false;
      }, 100);
    };

    container.addEventListener("animationend", handleAnimationEnd);
    return () =>
      container.removeEventListener("animationend", handleAnimationEnd);
  }, [onComplete]);

  const handleMouseEnter = () => {
    if (isAnimatingRef.current) return;
    isAnimatingRef.current = true;
    setAnimationKey(prev => prev + 1);
  };

  return (
    <div
      ref={containerRef}
      onMouseEnter={handleMouseEnter}
      className="flex cursor-pointer select-none items-center font-mono text-xl  min-w-[6.5ch]"
    >
      <span
        key={animationKey}
        className={`terminal-text animate-terminal-text ${textColor} text-2xl font-semibold`}
      ></span>
      <motion.span
        animate={{ opacity: [1, 1, 0, 0] }}
        transition={{
          duration: 1.2,
          repeat: Infinity,
          ease: "linear",
          times: [0, 0.5, 0.5, 1],
          delay: 0,
        }}
        className="inline-block h-[18px] w-[12px]"
        style={{ marginLeft: "4px", backgroundColor: "#ff00ff" }}
      />
    </div>
  );
};

export default Logo;
