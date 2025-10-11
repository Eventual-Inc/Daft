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

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleAnimationEnd = () => {
      onComplete?.();
    };

    container.addEventListener("animationend", handleAnimationEnd);
    return () =>
      container.removeEventListener("animationend", handleAnimationEnd);
  }, [onComplete]);

  const handleMouseEnter = () => {
    setAnimationKey(prev => prev + 1);
  };

  return (
    <div
      ref={containerRef}
      onMouseEnter={handleMouseEnter}
      className="flex cursor-pointer select-none items-center font-mono text-xl"
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
      <style jsx global>{`
        @keyframes terminal-text {
          0% {
            --text: "{";
          }
          16.6% {
            --text: ">#";
          }
          33.2% {
            --text: "d[:";
          }
          49.8% {
            --text: "da=/";
          }
          66.4% {
            --text: 'daf"';
          }
          83% {
            --text: "daft";
          }
          100% {
            --text: "daft";
          }
        }

        .terminal-text {
          animation: terminal-text 0.3s steps(1) forwards;
        }

        .terminal-text::before {
          content: var(--text, "{");
        }
      `}</style>
    </div>
  );
};

export default Logo;
