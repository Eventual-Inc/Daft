"use client";

import { type ReactNode } from "react";

type TreeLayoutProps<T> = {
  node: T;
  getChildren: (node: T) => T[];
  renderNode: (node: T) => ReactNode;
};

export default function TreeLayout<T>({ node, getChildren, renderNode }: TreeLayoutProps<T>) {
  const children = getChildren(node);

  return (
    <div className="flex flex-col items-center min-w-0">
      {renderNode(node)}

      {children.length > 0 && (
        <div className="flex flex-col items-center">
          {/* Vertical line from parent */}
          <div className="w-px h-6 bg-zinc-600" />

          {children.length > 1 ? (
            <div className="flex items-start">
              {children.map((child, i) => {
                const isFirst = i === 0;
                const isLast = i === children.length - 1;
                return (
                  <div key={i} className="flex flex-col items-center">
                    {/* Horizontal + vertical connector */}
                    <div className="flex w-full h-px">
                      <div
                        className={`flex-1 h-px ${isFirst ? "bg-transparent" : "bg-zinc-600"}`}
                      />
                      <div
                        className={`flex-1 h-px ${isLast ? "bg-transparent" : "bg-zinc-600"}`}
                      />
                    </div>
                    <div className="w-px h-6 bg-zinc-600" />
                    <div className="px-3">
                      <TreeLayout node={child} getChildren={getChildren} renderNode={renderNode} />
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <TreeLayout node={children[0]} getChildren={getChildren} renderNode={renderNode} />
          )}
        </div>
      )}
    </div>
  );
}
