use super::join_graph::{JoinGraph, JoinOrderTree, JoinOrderer};

// The brute force join orderer is a simple algorithm that recursively enumerates all possible join
// orders (including deep and bushy joins) and picks the one with the lowest summed cardinality.
//
// This algorithm is easy to understand and implement, but is not efficient because it takes more than
// O(n!) time to run. It is meant as an oracle to verify that more efficient algorithms produce the
// optimal join order.
pub(crate) struct BruteForceJoinOrderer {}

impl BruteForceJoinOrderer {
    // Takes the following arguments:
    // - graph: The join graph we're evaluating on.
    // - elements: The list of elements to choose from.
    // - cur_idx: The current index (out of the `k_to_pick` elements we're choosing) to swap into.
    // - pick_idx: The current index in the list of elements to pick from.
    // - k_to_pick: The number of elements to pick.
    // - min_cost: The current minimum cost (if any) of all the generated combinations.
    // - chosen_plan: The current join order tree (if any) that corresponds to the minimum cost.
    //
    // Loops through all (n choose k) combinations of elements, where n = length of elements and k = k_to_pick, and computes
    // the minimum cost order where the chosen elements go into the left subtree and the non-chosen elements go into the right
    // subtree. Updates min_cost and chosen_plan if some combination produces a plan with a lower cost than the current minimum cost.
    fn evaluate_combinations(
        graph: &JoinGraph,
        elements: &mut [usize],
        cur_idx: usize,
        pick_idx: usize,
        k_to_pick: usize,
        min_cost: &mut Option<usize>,
        chosen_plan: &mut Option<JoinOrderTree>,
    ) {
        if cur_idx >= k_to_pick {
            let (left, right) = elements.split_at_mut(cur_idx);
            if let Some((left_cost, left_join_order_tree)) = Self::find_min_cost_order(graph, left)
                && let Some((right_cost, right_join_order_tree)) =
                    Self::find_min_cost_order(graph, right)
            {
                let (connections, total_domain) = graph
                    .adj_list
                    .get_connections(&left_join_order_tree, &right_join_order_tree);
                if !connections.is_empty() {
                    // If there is a connection between the left and right subgraphs, we compute the cardinality of the
                    // joined graph as the product of all the cardinalities of the relations in the left and right subgraphs,
                    // divided by the selectivity of the join.
                    // Assuming that join keys are uniformly distributed and independent, the selectivity is computed as the reciprocal
                    // of the product of the largest total domains that form a minimum spanning tree of the relations.
                    let cardinality = left_join_order_tree.get_cardinality()
                        * right_join_order_tree.get_cardinality()
                        / total_domain;
                    // The cost of the join is the sum of the cardinalities of the left and right subgraphs, plus the cardinality of the joined graph.
                    let cur_cost = cardinality + left_cost + right_cost;
                    // Take the join with the lowest summed cardinality.
                    if let Some(ref mut cur_min_cost) = min_cost {
                        if *cur_min_cost > cur_cost {
                            *cur_min_cost = cur_cost;
                            *chosen_plan = Some(left_join_order_tree.join(
                                right_join_order_tree,
                                connections,
                                cardinality,
                            ));
                        }
                    } else {
                        *min_cost = Some(cur_cost);
                        *chosen_plan = Some(left_join_order_tree.join(
                            right_join_order_tree,
                            connections,
                            cardinality,
                        ));
                    }
                }
            }
            return;
        }
        if pick_idx >= elements.len() {
            return;
        }
        // Case 1: Include the current element.
        elements.swap(cur_idx, pick_idx);
        Self::evaluate_combinations(
            graph,
            elements,
            cur_idx + 1,
            pick_idx + 1,
            k_to_pick,
            min_cost,
            chosen_plan,
        );
        elements.swap(cur_idx, pick_idx); // Backtrack.

        // Case 2: Exclude the current element in the chosen set.
        Self::evaluate_combinations(
            graph,
            elements,
            cur_idx,
            pick_idx + 1,
            k_to_pick,
            min_cost,
            chosen_plan,
        );
    }

    // Enumerates all possible join orders and returns the one with the lowest summed cardinality.
    fn find_min_cost_order(
        graph: &JoinGraph,
        available: &mut [usize],
    ) -> Option<(usize, JoinOrderTree)> {
        if available.len() == 1 {
            // Base case: if there is only one element, we return the cost of the relation and the join order tree.
            let id = available[0];
            let plan = graph
                .adj_list
                .id_to_plan
                .get(&id)
                .expect("Got non-existent ID in join graph");
            let stats = plan.materialized_stats();
            let cost = stats.approx_stats.num_rows;
            return Some((cost, JoinOrderTree::Relation(id, cost)));
        }
        // Recursive case: we split the available elements into two groups and recursively find the minimum cost join order for each group.
        // We only need to consider splits where the left group has at most half the number of elements as the right group, because the
        // cardinality of the join is commutative. Determining probe/build sides happens later in the physical planner.
        let max_left_size = available.len() / 2;
        let mut min_cost = None;
        let mut chosen_plan = None;
        for left_split_size in 1..=max_left_size {
            // Evaluate the cost of all possible combinations and keep the plan with the lowest cost.
            Self::evaluate_combinations(
                graph,
                available,
                0,
                0,
                left_split_size,
                &mut min_cost,
                &mut chosen_plan,
            );
        }
        if let Some(min_cost) = min_cost
            && let Some(chosen_plan) = chosen_plan
        {
            Some((min_cost, chosen_plan))
        } else {
            None
        }
    }
}

impl JoinOrderer for BruteForceJoinOrderer {
    fn order(&self, graph: &JoinGraph) -> JoinOrderTree {
        let mut available: Vec<usize> = (0..graph.adj_list.max_id).collect();
        if let Some((_cost, join_order_tree)) = Self::find_min_cost_order(graph, &mut available[..])
        {
            join_order_tree
        } else {
            panic!("Tried to get join order from non-fully connected join graph")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_scan_info::Pushdowns;
    use common_treenode::TransformedResult;
    use daft_schema::{dtype::DataType, field::Field};

    use super::{BruteForceJoinOrderer, JoinGraph, JoinOrderTree, JoinOrderer};
    use crate::{
        optimization::rules::{
            reorder_joins::join_graph::{JoinAdjList, JoinNode},
            rule::OptimizerRule,
            EnrichWithStats, MaterializeScans,
        },
        test::{dummy_scan_node_with_pushdowns, dummy_scan_operator_with_size},
        LogicalPlanRef,
    };

    const PLACEHOLDER_CARDINALITY: usize = 0;

    // Helper functions to create test trees with placeholder values.
    fn test_relation(id: usize) -> JoinOrderTree {
        JoinOrderTree::Relation(id, PLACEHOLDER_CARDINALITY)
    }

    fn test_join(left: JoinOrderTree, right: JoinOrderTree) -> JoinOrderTree {
        JoinOrderTree::Join(
            Box::new(left),
            Box::new(right),
            vec![], // Empty join conditions.
            PLACEHOLDER_CARDINALITY,
        )
    }

    fn create_scan_node(name: &str, size: Option<usize>) -> LogicalPlanRef {
        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(vec![Field::new(name, DataType::Int64)], size),
            Pushdowns::default(),
        )
        .build();
        let scan_materializer = MaterializeScans::new();
        let plan = scan_materializer.try_optimize(plan).data().unwrap();
        let stats_enricher = EnrichWithStats::new();
        stats_enricher.try_optimize(plan).data().unwrap()
    }

    fn create_join_graph_with_edges(
        plans: Vec<LogicalPlanRef>,
        edges: Vec<(usize, String, usize, String, usize)>,
    ) -> JoinGraph {
        let mut adj_list = JoinAdjList::empty();
        for (from, from_rel_name, to, to_rel_name, td) in edges {
            adj_list.add_bidirectional_edge_with_total_domain(
                JoinNode::new(from_rel_name, plans[from].clone()),
                JoinNode::new(to_rel_name, plans[to].clone()),
                td,
            );
        }
        JoinGraph::new(adj_list, vec![])
    }

    macro_rules! create_and_test_join_order {
        ($nodes:expr, $edges:expr, $orderer:expr, $optimal_order:expr) => {
            let plans: Vec<LogicalPlanRef> = $nodes
                .iter()
                .map(|(name, size)| create_scan_node(name, Some(*size)))
                .collect();
            let graph = create_join_graph_with_edges(plans.clone(), $edges);
            let order = $orderer.order(&graph);
            assert!(JoinOrderTree::order_eq(&order, &$optimal_order));
        };
    }

    fn node_to_id_map(nodes: Vec<(&str, usize)>) -> HashMap<String, usize> {
        nodes
            .into_iter()
            .enumerate()
            .map(|(id, (name, _))| (name.to_string(), id))
            .collect()
    }

    #[test]
    fn test_brute_force_order_minimal() {
        let nodes = vec![("medium", 1_000), ("large", 50_000), ("small", 500)];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            (
                name_to_id["medium"],
                "m_medium".to_string(),
                name_to_id["large"],
                "l_medium".to_string(),
                1_000,
            ),
            (
                name_to_id["large"],
                "l_small".to_string(),
                name_to_id["small"],
                "s_small".to_string(),
                500,
            ),
            (
                name_to_id["medium"],
                "m_small".to_string(),
                name_to_id["small"],
                "s_small".to_string(),
                500,
            ),
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["large"]),
            test_join(
                test_relation(name_to_id["medium"]),
                test_relation(name_to_id["small"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_mock_tpch_q5() {
        let nodes = vec![
            ("region", 1),
            ("nation", 25),
            ("customer", 1_500_000),
            ("orders", 3_000_000),
            ("lineitem", 60_000_000),
            ("supplier", 100_000),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            (
                name_to_id["region"],
                "r_regionkey".to_string(),
                name_to_id["nation"],
                "n_regionkey".to_string(),
                10,
            ),
            (
                name_to_id["nation"],
                "n_nationkey".to_string(),
                name_to_id["customer"],
                "c_nationkey".to_string(),
                25,
            ),
            (
                name_to_id["customer"],
                "c_custkey".to_string(),
                name_to_id["orders"],
                "o_custkey".to_string(),
                1_500_000,
            ),
            (
                name_to_id["orders"],
                "o_orderkey".to_string(),
                name_to_id["lineitem"],
                "l_orderkey".to_string(),
                15_000_000,
            ),
            (
                name_to_id["lineitem"],
                "l_suppkey".to_string(),
                name_to_id["supplier"],
                "s_suppkey".to_string(),
                100_000,
            ),
            (
                name_to_id["supplier"],
                "s_nationkey".to_string(),
                name_to_id["nation"],
                "n_nationkey".to_string(),
                25,
            ),
            (
                name_to_id["customer"],
                "c_nationkey".to_string(),
                name_to_id["supplier"],
                "s_nationkey".to_string(),
                25,
            ),
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["supplier"]),
            test_join(
                test_relation(name_to_id["lineitem"]),
                test_join(
                    test_relation(name_to_id["orders"]),
                    test_join(
                        test_relation(name_to_id["customer"]),
                        test_join(
                            test_relation(name_to_id["nation"]),
                            test_relation(name_to_id["region"]),
                        ),
                    ),
                ),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_star_schema() {
        let nodes = vec![
            ("fact", 10_000_000),
            ("dim1", 100),
            ("dim2", 500),
            ("dim3", 50),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            (
                name_to_id["fact"],
                "f_dim1".to_string(),
                name_to_id["dim1"],
                "d_dim1".to_string(),
                10_000,
            ), // Pretend there was a large filter on dim1.
            (
                name_to_id["fact"],
                "f_dim2".to_string(),
                name_to_id["dim2"],
                "d_dim2".to_string(),
                500,
            ),
            (
                name_to_id["fact"],
                "f_dim3".to_string(),
                name_to_id["dim3"],
                "d_dim3".to_string(),
                500,
            ), // Pretend there was a small filter on dim3.
        ];
        let optimal_order = test_join(
            test_join(
                test_join(
                    test_relation(name_to_id["fact"]),
                    test_relation(name_to_id["dim1"]),
                ),
                test_relation(name_to_id["dim3"]),
            ),
            test_relation(name_to_id["dim2"]),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_snowflake_schema() {
        let nodes = vec![
            ("fact", 50_000_000),
            ("dim1", 50_000),
            ("dim2", 500),
            ("dim3", 500),
            ("dim4", 25),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            (
                name_to_id["fact"],
                "f_dim1".to_string(),
                name_to_id["dim1"],
                "d_dim1".to_string(),
                500_000,
            ), // Pretend there was a filter on dim1.
            (
                name_to_id["fact"],
                "f_dim2".to_string(),
                name_to_id["dim2"],
                "d_dim2".to_string(),
                500,
            ),
            (
                name_to_id["dim2"],
                "d_dim3".to_string(),
                name_to_id["dim3"],
                "d_dim3".to_string(),
                500,
            ),
            (
                name_to_id["dim2"],
                "d_dim4".to_string(),
                name_to_id["dim4"],
                "d_dim4".to_string(),
                250,
            ), // Pretend there was a filter on dim4.
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["dim1"]),
            test_join(
                test_relation(name_to_id["fact"]),
                test_join(
                    test_relation(name_to_id["dim3"]),
                    test_join(
                        test_relation(name_to_id["dim2"]),
                        test_relation(name_to_id["dim4"]),
                    ),
                ),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_bushy_join() {
        let nodes = vec![
            ("table1", 10_000),
            ("table2", 1_000_000),
            ("table3", 10_000),
            ("table4", 1_000_000),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            (
                name_to_id["table1"],
                "t1_t2".to_string(),
                name_to_id["table2"],
                "t2_t2".to_string(),
                10,
            ),
            (
                name_to_id["table2"],
                "t2_t3".to_string(),
                name_to_id["table3"],
                "t3_t3".to_string(),
                2,
            ),
            (
                name_to_id["table3"],
                "t3_t4".to_string(),
                name_to_id["table4"],
                "t4_t4".to_string(),
                20,
            ),
        ];
        let optimal_order = test_join(
            test_join(
                test_relation(name_to_id["table1"]),
                test_relation(name_to_id["table2"]),
            ),
            test_join(
                test_relation(name_to_id["table3"]),
                test_relation(name_to_id["table4"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }
}
