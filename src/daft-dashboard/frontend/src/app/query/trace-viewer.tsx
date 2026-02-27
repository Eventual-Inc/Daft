"use client";

import { genApiUrl } from "@/components/server-provider";
import { main } from "@/lib/utils";
import { useCallback, useEffect, useRef, useState } from "react";

type TraceState = "loading" | "unavailable" | "ready" | "error";

export default function TraceViewer({ queryId }: { queryId: string }) {
  const [traceState, setTraceState] = useState<TraceState>("loading");
  const [traceData, setTraceData] = useState<ArrayBuffer | null>(null);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  // Fetch trace data from the dashboard server
  useEffect(() => {
    let cancelled = false;

    async function fetchTrace() {
      try {
        const resp = await fetch(
          genApiUrl(`/client/query/${queryId}/trace.json`)
        );
        if (resp.status === 404) {
          setTraceState("unavailable");
          return;
        }
        if (!resp.ok) {
          setTraceState("error");
          return;
        }
        const buffer = await resp.arrayBuffer();
        if (!cancelled) {
          setTraceData(buffer);
          setTraceState("ready");
        }
      } catch {
        if (!cancelled) {
          setTraceState("error");
        }
      }
    }

    fetchTrace();
    return () => {
      cancelled = true;
    };
  }, [queryId]);

  // Send trace data to Perfetto iframe via postMessage
  const handleIframeLoad = useCallback(() => {
    if (!traceData || !iframeRef.current?.contentWindow) return;

    const win = iframeRef.current.contentWindow;

    // Perfetto's postMessage API:
    // 1. Wait for PING from Perfetto
    // 2. Reply with PONG
    // 3. Send the trace data as an ArrayBuffer
    const handler = (event: MessageEvent) => {
      if (event.data === "PING") {
        win.postMessage("PONG", "*");
      }
      if (event.data === "PONG") {
        // Perfetto is ready, send the trace
        win.postMessage(
          {
            perfetto: {
              buffer: traceData,
              title: `Daft Query ${queryId}`,
              keepApiOpen: false,
            },
          },
          "*"
        );
        window.removeEventListener("message", handler);
      }
    };

    window.addEventListener("message", handler);

    // Also try sending immediately in case Perfetto is already ready
    win.postMessage("PING", "*");

    return () => {
      window.removeEventListener("message", handler);
    };
  }, [traceData, queryId]);

  const handleDownload = useCallback(() => {
    if (!traceData) return;
    const blob = new Blob([traceData], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `daft-trace-${queryId}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [traceData, queryId]);

  if (traceState === "loading") {
    return (
      <div className="p-8 text-center">
        <p className={`${main.className} text-zinc-400`}>
          Loading trace data...
        </p>
      </div>
    );
  }

  if (traceState === "unavailable") {
    return (
      <div className="p-8 text-center space-y-2">
        <p className={`${main.className} text-zinc-400`}>
          No trace available for this query.
        </p>
        <p className={`${main.className} text-zinc-500 text-sm`}>
          Set{" "}
          <code className="bg-zinc-800 px-1 py-0.5 rounded">
            DAFT_DEV_ENABLE_CHROME_TRACE=1
          </code>{" "}
          before running Daft to enable tracing.
        </p>
      </div>
    );
  }

  if (traceState === "error") {
    return (
      <div className="p-8 text-center">
        <p className={`${main.className} text-red-400`}>
          Failed to load trace data.
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col" style={{ height: "calc(100vh - 420px)", minHeight: "400px" }}>
      <div className="flex-shrink-0 flex items-center justify-end gap-2 mb-2">
        <button
          onClick={handleDownload}
          className="px-3 py-1 text-sm bg-zinc-800 hover:bg-zinc-700 text-zinc-300 rounded border border-zinc-700 transition-colors"
        >
          Download JSON
        </button>
      </div>
      <div className="flex-1 min-h-0">
        <iframe
          ref={iframeRef}
          src="https://ui.perfetto.dev/#!/viewer?mode=embedded"
          onLoad={handleIframeLoad}
          className="w-full h-full border border-zinc-700 rounded"
          allow="cross-origin-isolated"
          sandbox="allow-scripts allow-same-origin allow-popups"
        />
      </div>
    </div>
  );
}
