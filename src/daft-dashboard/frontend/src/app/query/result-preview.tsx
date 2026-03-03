"use client";

import { genApiUrl } from "@/components/server-provider";
import { main } from "@/lib/utils";
import useSWR from "swr";
import { ResultPreview as ResultPreviewType } from "./types";

const resultsFetcher = (url: string): Promise<ResultPreviewType> =>
  fetch(url).then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  });

export default function ResultPreview({ queryId }: { queryId: string }) {
  const { data, isLoading } = useSWR<ResultPreviewType>(
    genApiUrl(`/client/query/${queryId}/results`),
    resultsFetcher,
    { revalidateOnFocus: false, revalidateOnReconnect: false }
  );

  if (isLoading) {
    return (
      <div className="p-8 text-center">
        <p className={`${main.className} text-zinc-400`}>Loading results...</p>
      </div>
    );
  }

  if (!data || data.html === null) {
    return (
      <div className="p-8 text-center">
        <p className={`${main.className} text-zinc-400`}>
          No results available
        </p>
      </div>
    );
  }

  return (
    <div className="p-4 overflow-auto">
      <p className={`${main.className} text-sm text-zinc-400 mb-3`}>
        Showing {data.num_rows.toLocaleString()} of{" "}
        {data.total_rows !== null
          ? `${data.total_rows.toLocaleString()} row${data.total_rows !== 1 ? "s" : ""}`
          : "unknown rows"}
      </p>
      <div
        className="result-preview-table"
        dangerouslySetInnerHTML={{ __html: data.html }}
      />
    </div>
  );
}
