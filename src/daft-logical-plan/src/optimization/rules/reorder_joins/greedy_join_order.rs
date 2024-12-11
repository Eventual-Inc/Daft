use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{col, ExprRef};

use super::join_graph::{JoinCondition, JoinGraph};
use crate::{LogicalPlanBuilder, LogicalPlanRef};

// This is an implementation of the Greedy Operator Ordering algorithm (GOO) [1] for join selection. This algorithm
// selects join edges greedily by picking the edge with the smallest cost at each step. This is similar to Kruskal's
// minimum spanning tree algorithm, with the caveat that edge costs update at each step, due to changing cardinalities
// and selectivities between join nodes.
//
// Compared to DP-based algorithms, GOO is not always optimal. However, GOO has a complexity of O(n^3) and is more viable
// than DP-based algorithms when performing join ordering on many relations. DP Connected subgraph Complement Pairs (DPccp) [2]
// is the DP-based algorithm widely used in database systems today and has a O(3^n) complexity, although the latest
// literature does offer a super-polynomially faster DP-algorithm but that still has a O(2^n) to O(2^n * n^3) complexity [3].
//
// For this reason, we maintain a greedy-based join ordering algorithm to use when the number of relations is large, and default
// to DP-based algorithms otherwise.
//
// [1]: Fegaras, L. (1998). A New Heuristic for Optimizing Large Queries. International Conference on Database and Expert Systems Applications.
// [2]: Moerkotte, G., & Neumann, T. (2006). Analysis of two existing and one new dynamic programming algorithm for the generation of optimal bushy join trees without cross products. Very Large Data Bases Conference.
// [3]: Stoian, M., & Kipf, A. (2024). DPconv: Super-Polynomially Faster Join Ordering. ArXiv, abs/2409.08013.
pub(crate) struct GreedyJoinOrderer {}

impl GreedyJoinOrderer {
    pub(crate) fn compute_join_order(join_graph: &mut JoinGraph) -> DaftResult<LogicalPlanRef> {
        // TODO(desmond): we need to handle projections.
        while join_graph.adj_list.0.len() > 1 {
            let selected_pair = GreedyJoinOrderer::find_minimum_cost_join(&join_graph.adj_list.0);
            if let Some((left, right, join_conds)) = selected_pair {
                let (left_on, right_on) = join_conds
                    .iter()
                    .map(|join_cond| {
                        (
                            col(join_cond.left_on.clone()),
                            col(join_cond.right_on.clone()),
                        )
                    })
                    .collect::<(Vec<ExprRef>, Vec<ExprRef>)>();
                let left_builder = LogicalPlanBuilder::from(left.clone());
                let join = left_builder
                    .inner_join(right.clone(), left_on, right_on)?
                    .build();
                let join = Arc::new(Arc::unwrap_or_clone(join).with_materialized_stats());
                let left_neighbors = join_graph.adj_list.0.remove(&left).unwrap();
                let right_neighbors = join_graph.adj_list.0.remove(&right).unwrap();
                let mut new_join_edges = HashMap::new();

                // Helper function to collapse the left and right node
                let mut update_neighbors =
                    |neighbors: HashMap<LogicalPlanRef, Vec<JoinCondition>>| {
                        for (neighbor, _) in neighbors {
                            if neighbor == right || neighbor == left {
                                // Skip the nodes that we just joined.
                                continue;
                            }
                            let mut join_conditions = Vec::new();
                            // If this neighbor was connected to left or right nodes, collect the join conditions.
                            let neighbor_edges = join_graph
                                .adj_list
                                .0
                                .get_mut(&neighbor)
                                .expect("The neighbor should still be in the join graph");
                            if let Some(left_conds) = neighbor_edges.remove(&left) {
                                join_conditions.extend(left_conds);
                            }
                            if let Some(right_conds) = neighbor_edges.remove(&right) {
                                join_conditions.extend(right_conds);
                            }
                            // If this neighbor had any connections to left or right, create a new edge to the new join node.
                            if !join_conditions.is_empty() {
                                neighbor_edges.insert(join.clone(), join_conditions.clone());
                                new_join_edges.insert(
                                    neighbor.clone(),
                                    join_conditions.iter().map(|cond| cond.flip()).collect(),
                                );
                            }
                        }
                    };

                // Process all neighbors from both the left and right sides.
                update_neighbors(left_neighbors);
                update_neighbors(right_neighbors);

                // Add the new join node and its edges to the graph
                join_graph.adj_list.0.insert(join, new_join_edges);
            } else {
                panic!(
                    "No valid join edge selected despite join graph containing more than one relation"
                );
            }
        }
        // Apply projections and filters on top of the fully joined plan.
        if let Some(joined_plan) = join_graph.adj_list.0.drain().map(|(plan, _)| plan).last() {
            join_graph.apply_projections_and_filters_to_plan(joined_plan)
        } else {
            panic!("No valid logical plan after join reordering")
        }
    }

    fn find_minimum_cost_join(
        adj_list: &HashMap<LogicalPlanRef, HashMap<LogicalPlanRef, Vec<JoinCondition>>>,
    ) -> Option<(LogicalPlanRef, LogicalPlanRef, Vec<JoinCondition>)> {
        let mut min_cost = None;
        let mut selected_pair = None;

        for (candidate_left, neighbors) in adj_list {
            for (candidate_right, join_conds) in neighbors {
                let left_stats = candidate_left.materialized_stats();
                let right_stats = candidate_right.materialized_stats();

                // Assume primary key foreign key join which would have a size bounded by the foreign key relation,
                // which is typically larger.
                let cur_cost = left_stats
                    .approx_stats
                    .upper_bound_bytes
                    .max(right_stats.approx_stats.upper_bound_bytes);

                if let Some(existing_min) = min_cost {
                    if let Some(current) = cur_cost {
                        if current < existing_min {
                            min_cost = Some(current);
                            selected_pair = Some((
                                candidate_left.clone(),
                                candidate_right.clone(),
                                join_conds.clone(),
                            ));
                        }
                    }
                } else {
                    min_cost = cur_cost;
                    selected_pair = Some((
                        candidate_left.clone(),
                        candidate_right.clone(),
                        join_conds.clone(),
                    ));
                }
            }
        }

        selected_pair
    }
}
