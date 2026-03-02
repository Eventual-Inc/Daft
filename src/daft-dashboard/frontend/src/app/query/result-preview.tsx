"use client";

import { genApiUrl } from "@/components/server-provider";
import { main } from "@/lib/utils";
import { useEffect, useState } from "react";
import { ResultPreview as ResultPreviewType } from "./types";

export default function ResultPreview({ queryId }: { queryId: string }) {
  const [data, setData] = useState<ResultPreviewType | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(genApiUrl(`/client/query/${queryId}/results`))
      .then(res => res.json())
      .then((json: ResultPreviewType) => {
        setData(json);
        setLoading(false);
      })
      .catch(() => {
        setLoading(false);
      });
  }, [queryId]);

  if (loading) {
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
        Showing {data.num_rows} row{data.num_rows !== 1 ? "s" : ""}
      </p>
      <div
        className="result-preview-table"
        dangerouslySetInnerHTML={{ __html: data.html }}
      />
      <style jsx global>{`
        .result-preview-table table {
          border-collapse: collapse;
          width: 100%;
          background: transparent;
          font-family: ui-monospace, monospace;
          font-size: 0.85rem;
        }
        .result-preview-table th {
          color: #a1a1aa;
          text-align: left;
          padding: 8px 12px;
          border-bottom: 2px solid #3f3f46;
          white-space: nowrap;
        }
        .result-preview-table td {
          color: #d4d4d8;
          padding: 6px 12px;
          border-bottom: 1px solid #3f3f46;
          max-width: 400px;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .result-preview-table tr:hover td {
          background: rgba(39, 39, 42, 0.5);
        }
        .result-preview-table img {
          max-height: 100px;
          max-width: 200px;
        }
      `}</style>
    </div>
  );
}
