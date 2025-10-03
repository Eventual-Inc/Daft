"use client";

import * as React from "react";
import { useState } from "react";
import { ReactFlow, Position, Controls, Background } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const nodeDefaults = {
  sourcePosition: Position.Right,
  targetPosition: Position.Left,
};

const initialNodes = [
  {
    id: "1-header",
    type: "input",
    position: { x: 0, y: 150 },
    data: { label: "Parquet Scan" },
    ...nodeDefaults,
  },
  {
    id: "1-body",
    type: "group",
    position: { x: 0, y: 190 },
    data: { label: "Parquet Scan Body" },
    style: {
      width: 150,
      height: 90,
    },
    ...nodeDefaults,
  },
  {
    id: "1-1",
    type: "input",
    position: { x: 25, y: 10 },
    data: { label: "rand_dt" },
    parentId: "1-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 10,
    },
    ...nodeDefaults,
  },
  {
    id: "1-2",
    type: "input",
    position: { x: 25, y: 50 },
    data: { label: "rand_str" },
    parentId: "1-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 10,
    },
    ...nodeDefaults,
  },
  {
    id: "2-header",
    position: { x: 250, y: 150 },
    data: { label: "Reorder (Project)" },
    ...nodeDefaults,
  },
  {
    id: "2-body",
    type: "group",
    position: { x: 250, y: 190 },
    data: { label: "Reorder (Project) Body" },
    style: {
      width: 150,
      height: 90,
    },
    ...nodeDefaults,
  },
  {
    id: "2-1",
    position: { x: 25, y: 10 },
    data: { label: "rand_str" },
    parentId: "2-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 10,
    },
    ...nodeDefaults,
  },
  {
    id: "2-2",
    position: { x: 25, y: 50 },
    data: { label: "rand_dt" },
    parentId: "2-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 10,
    },
    ...nodeDefaults,
  },
  {
    id: "3-header",
    position: { x: 500, y: 150 },
    data: { label: "Group By" },
    ...nodeDefaults,
  },
  {
    id: "3-body",
    type: "group",
    position: { x: 500, y: 190 },
    data: { label: "Group By Body" },
    style: {
      width: 150,
      height: 90,
    },
    ...nodeDefaults,
  },
  {
    id: "3-1",
    position: { x: 25, y: 10 },
    data: { label: "Group By: rand_str" },
    parentId: "3-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 8,
    },
    ...nodeDefaults,
  },
  {
    id: "3-2",
    position: { x: 25, y: 50 },
    data: { label: "count(rand_dt)" },
    parentId: "3-body",
    style: {
      width: 100,
      height: 30,
      fontSize: 8,
    },
    ...nodeDefaults,
  },
];

const initialEdges = [
  {
    id: "e1-2",
    source: "1-header",
    target: "2-header",
    animated: true,
  },
  {
    id: "e2-3",
    source: "2-header",
    target: "3-header",
    animated: true,
  },
  {
    id: "e11-22",
    source: "1-1",
    target: "2-2",
    animated: false,
  },
  {
    id: "e12-21",
    source: "1-2",
    target: "2-1",
    animated: false,
  },
  {
    id: "e22-33",
    source: "2-1",
    target: "3-1",
    animated: false,
  },
  {
    id: "e33-11",
    source: "2-2",
    target: "3-2",
    animated: false,
  },
];

/**
 *  Main Component to display the queries in a table
 */
export default function Example() {
  const [nodes, _] = useState(initialNodes);
  const [edges, __] = useState(initialEdges);

  return (
    <div
      id="flow-test"
      className="gap-4 space-y-4 text-black bg-white w-full h-full"
    >
      <ReactFlow nodes={nodes} edges={edges} colorMode="dark" fitView>
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  );
}
