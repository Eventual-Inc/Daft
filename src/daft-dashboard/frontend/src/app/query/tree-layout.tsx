"use client";

import { type ReactNode } from "react";

type EdgePosition = "single" | "branch";

type TreeLayoutProps<T> = {
  node: T;
  getChildren: (node: T) => T[];
  renderNode: (node: T) => ReactNode;
  renderEdge?: (parent: T, child: T, position: EdgePosition) => ReactNode;
};

const FALLBACK_SINGLE_STEM = <div className="w-px h-[18px] bg-zinc-600" />;
const FALLBACK_BRANCH_STEM = <div className="w-px h-6 bg-zinc-600" />;

export default function TreeLayout<T>({
  node,
  getChildren,
  renderNode,
  renderEdge,
}: TreeLayoutProps<T>) {
  const children = getChildren(node);

  return (
    <div className="flex flex-col items-center min-w-0">
      {renderNode(node)}

      {children.length > 0 && (
        <div className="flex flex-col items-center">
          {/* Arrowhead into parent (data flows up into parent) */}
          <div className="w-0 h-0 border-l-[4px] border-r-[4px] border-b-[6px] border-l-transparent border-r-transparent border-b-zinc-600" />

          {children.length > 1 ? (
            <>
              {/* Shared stem just below the arrowhead before fanning out */}
              {FALLBACK_SINGLE_STEM}
              <div className="flex items-start">
                {children.map((child, i) => {
                  const isFirst = i === 0;
                  const isLast = i === children.length - 1;
                  return (
                    <div key={i} className="flex flex-col items-center">
                      {/* Horizontal T-bar */}
                      <div className="flex w-full h-px">
                        <div
                          className={`flex-1 h-px ${isFirst ? "bg-transparent" : "bg-zinc-600"}`}
                        />
                        <div
                          className={`flex-1 h-px ${isLast ? "bg-transparent" : "bg-zinc-600"}`}
                        />
                      </div>
                      {/* Per-child vertical stem */}
                      {renderEdge
                        ? renderEdge(node, child, "branch")
                        : FALLBACK_BRANCH_STEM}
                      <div className="px-3">
                        <TreeLayout
                          node={child}
                          getChildren={getChildren}
                          renderNode={renderNode}
                          renderEdge={renderEdge}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </>
          ) : (
            <>
              {renderEdge
                ? renderEdge(node, children[0], "single")
                : FALLBACK_SINGLE_STEM}
              <TreeLayout
                node={children[0]}
                getChildren={getChildren}
                renderNode={renderNode}
                renderEdge={renderEdge}
              />
            </>
          )}
        </div>
      )}
    </div>
  );
}
