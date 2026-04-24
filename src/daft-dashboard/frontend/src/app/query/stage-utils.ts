import { PhysicalPlanNode } from "./types";

export type DisplayNode = {
  operators: PhysicalPlanNode[];
  stageIndex: number | null;
  children: DisplayNode[];
};

export function buildStageMap(stageGroups: number[][]): Map<number, number> {
  const map = new Map<number, number>();
  for (let i = 0; i < stageGroups.length; i++) {
    for (const nodeId of stageGroups[i]) {
      map.set(nodeId, i);
    }
  }
  return map;
}

export function buildDisplayTree(
  node: PhysicalPlanNode,
  stageMap: Map<number, number>,
): DisplayNode {
  const myStage = stageMap.get(node.id) ?? null;
  const operators: PhysicalPlanNode[] = [node];

  let current = node;
  while (current.children?.length === 1) {
    const child = current.children[0];
    const childStage = stageMap.get(child.id) ?? null;
    if (childStage !== null && childStage === myStage) {
      operators.push(child);
      current = child;
    } else {
      break;
    }
  }

  const lastNode = operators[operators.length - 1];
  const children = (lastNode.children ?? []).map((child) =>
    buildDisplayTree(child, stageMap),
  );

  return {
    operators,
    stageIndex: operators.length > 1 ? myStage : null,
    children,
  };
}
