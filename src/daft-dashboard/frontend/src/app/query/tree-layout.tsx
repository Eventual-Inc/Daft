"use client";

import { type ReactNode } from "react";
import { main } from "@/lib/utils";

type EdgePosition = "single" | "branch";

type TreeLayoutProps<T> = {
  node: T;
  getChildren: (node: T) => T[];
  renderNode: (node: T) => ReactNode;
  renderEdge?: (parent: T, child: T, position: EdgePosition) => ReactNode;
  getNodeStage?: (node: T) => number | null;
  parentStage?: number | null;
};

const FALLBACK_SINGLE_STEM = <div className="w-px h-[18px] bg-zinc-600" />;
const FALLBACK_BRANCH_STEM = <div className="w-px h-6 bg-zinc-600" />;

function StageBox({
  stageId,
  children,
}: {
  stageId: number;
  children: ReactNode;
}) {
  return (
    <div className="relative border border-zinc-700/60 rounded-lg px-3 pt-6 pb-3 bg-zinc-800/20">
      <div className="absolute top-1.5 left-3">
        <span
          className={`${main.className} text-[9px] font-mono text-zinc-600 uppercase tracking-wider`}
        >
          Stage {stageId}
        </span>
      </div>
      {children}
    </div>
  );
}

export default function TreeLayout<T>({
  node,
  getChildren,
  renderNode,
  renderEdge,
  getNodeStage,
  parentStage,
}: TreeLayoutProps<T>) {
  const children = getChildren(node);
  const myStage = getNodeStage?.(node) ?? null;
  const isNewStage = getNodeStage != null && myStage !== null && myStage !== parentStage;

  const renderSubtree = (child: T, position: EdgePosition) => {
    const childStage = getNodeStage?.(child) ?? null;
    const crossesStage =
      getNodeStage != null && childStage !== null && childStage !== myStage;

    return (
      <>
        {crossesStage ? (
          <>
            {/* Edge sits between stage boxes */}
            {renderEdge ? renderEdge(node, child, position) : FALLBACK_SINGLE_STEM}
            <TreeLayout
              node={child}
              getChildren={getChildren}
              renderNode={renderNode}
              renderEdge={renderEdge}
              getNodeStage={getNodeStage}
              parentStage={myStage}
            />
          </>
        ) : (
          <>
            {/* Same stage — edge + child stay inside the box */}
            {renderEdge ? renderEdge(node, child, position) : FALLBACK_SINGLE_STEM}
            <TreeLayout
              node={child}
              getChildren={getChildren}
              renderNode={renderNode}
              renderEdge={renderEdge}
              getNodeStage={getNodeStage}
              parentStage={parentStage}
            />
          </>
        )}
      </>
    );
  };

  const inner = (
    <div className="flex flex-col items-center min-w-0">
      {renderNode(node)}

      {children.length > 0 && (
        <div className="flex flex-col items-center">
          <div className="w-0 h-0 border-l-[4px] border-r-[4px] border-b-[6px] border-l-transparent border-r-transparent border-b-zinc-600" />

          {children.length > 1 ? (
            <>
              {FALLBACK_SINGLE_STEM}
              <div className="flex items-start">
                {children.map((child, i) => {
                  const isFirst = i === 0;
                  const isLast = i === children.length - 1;
                  const childStage = getNodeStage?.(child) ?? null;
                  const crossesStage =
                    getNodeStage != null && childStage !== null && childStage !== myStage;

                  return (
                    <div key={i} className="flex flex-col items-center">
                      <div className="flex w-full h-px">
                        <div
                          className={`flex-1 h-px ${isFirst ? "bg-transparent" : "bg-zinc-600"}`}
                        />
                        <div
                          className={`flex-1 h-px ${isLast ? "bg-transparent" : "bg-zinc-600"}`}
                        />
                      </div>
                      {crossesStage ? (
                        <div className="px-3 flex flex-col items-center">
                          {renderEdge
                            ? renderEdge(node, child, "branch")
                            : FALLBACK_BRANCH_STEM}
                          <TreeLayout
                            node={child}
                            getChildren={getChildren}
                            renderNode={renderNode}
                            renderEdge={renderEdge}
                            getNodeStage={getNodeStage}
                            parentStage={myStage}
                          />
                        </div>
                      ) : (
                        <>
                          {renderEdge
                            ? renderEdge(node, child, "branch")
                            : FALLBACK_BRANCH_STEM}
                          <div className="px-3">
                            <TreeLayout
                              node={child}
                              getChildren={getChildren}
                              renderNode={renderNode}
                              renderEdge={renderEdge}
                              getNodeStage={getNodeStage}
                              parentStage={parentStage}
                            />
                          </div>
                        </>
                      )}
                    </div>
                  );
                })}
              </div>
            </>
          ) : (
            <>{renderSubtree(children[0], "single")}</>
          )}
        </div>
      )}
    </div>
  );

  if (isNewStage) {
    return <StageBox stageId={myStage}>{inner}</StageBox>;
  }

  return inner;
}
