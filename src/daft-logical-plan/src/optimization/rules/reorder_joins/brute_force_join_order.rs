use super::join_graph::{JoinGraph, JoinOrderTree, JoinOrderer};

// The brute force join orderer is a simple algorithm that recursively enumerates all possible join
// orders (including deep and bushy joins) and picks the one with the lowest summed cardinality.
//
// This algorithm is easy to understand and implement, but is not efficient because it takes more than
// O(n!) time to run. It is meant as an oracle to verify that more efficient algorithms produce the
// optimal join order.
pub(crate) struct BruteForceJoinOrderer {}

// Takes the following arguments:
// - elements: The list of elements to choose from.
// - cur_idx: The current index in the list of elements to choose from.
// - remaining: The number of elements to choose.
// - chosen: The list of elements that have already been chosen.
// - unchosen: The list of elements that have not yet been chosen.
//
// Returns a list of tuples, where each tuple contains two lists:
// - The first list is the list of elements that have been chosen.
// - The second list is the list of elements that have not been chosen.
fn generate_combinations(
    elements: &[usize],
    cur_idx: usize,
    remaining: usize,
    chosen: Vec<usize>,
    mut unchosen: Vec<usize>,
) -> Vec<(Vec<usize>, Vec<usize>)> {
    if remaining == 0 {
        for &element in elements.iter().skip(cur_idx) {
            unchosen.push(element);
        }
        return vec![(chosen, unchosen)];
    }
    if cur_idx >= elements.len() {
        return vec![];
    }
    let mut chosen_clone = chosen.clone();
    chosen_clone.push(elements[cur_idx]);
    let mut unchosen_clone = unchosen.clone();
    unchosen_clone.push(elements[cur_idx]);
    let mut results =
        generate_combinations(elements, cur_idx + 1, remaining, chosen, unchosen_clone);
    results.extend(generate_combinations(
        elements,
        cur_idx + 1,
        remaining - 1,
        chosen_clone,
        unchosen,
    ));
    results
}

impl BruteForceJoinOrderer {
    // Enumerates all possible join orders and returns the one with the lowest summed cardinality.
    fn find_min_cost_order(
        graph: &JoinGraph,
        available: Vec<usize>,
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
            for (chosen, unchosen) in
                generate_combinations(&available, 0, left_split_size, vec![], vec![])
            {
                if let Some((left_cost, left_join_order_tree)) =
                    Self::find_min_cost_order(graph, chosen)
                    && let Some((right_cost, right_join_order_tree)) =
                        Self::find_min_cost_order(graph, unchosen)
                {
                    let connections = graph
                        .adj_list
                        .get_connections(&left_join_order_tree, &right_join_order_tree);
                    if !connections.is_empty() {
                        // If there is a connection between the left and right subgraphs, we compute the cardinality of the
                        // joined graph as the product of all the cardinalities of the relations in the left and right subgraphs,
                        // divided by the selectivity of the join.
                        // Assuming that join keys are uniformly distributed and independent, the selectivity is computed as the reciprocal
                        // of the product of the largest total domains that form a minimum spanning tree of the relations.

                        // TODO(desmond): This is a hack to get the selectivity of the join. We should expand this to take the minimum spanning tree
                        // of edges to connect the relations. For simple graphs (e.g. the queries in the TPCH benchmark), taking the max of the total
                        // domains is a good proxy.
                        let denominator = connections
                            .iter()
                            .map(|conn| conn.total_domain)
                            .max()
                            .expect("There should be at least one total domain");
                        let cardinality = left_join_order_tree.get_cardinality()
                            * right_join_order_tree.get_cardinality()
                            / denominator;
                        // The cost of the join is the sum of the cardinalities of the left and right subgraphs, plus the cardinality of the joined graph.
                        let cur_cost = cardinality + left_cost + right_cost;
                        // Take the join with the lowest summed cardinality.
                        if let Some(cur_min_cost) = min_cost {
                            if cur_min_cost > cur_cost {
                                min_cost = Some(cur_cost);
                                chosen_plan = Some(left_join_order_tree.join(
                                    right_join_order_tree,
                                    connections,
                                    cardinality,
                                ));
                            }
                        } else {
                            min_cost = Some(cur_cost);
                            chosen_plan = Some(left_join_order_tree.join(
                                right_join_order_tree,
                                connections,
                                cardinality,
                            ));
                        }
                    }
                }
            }
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
        let available: Vec<usize> = (0..graph.adj_list.max_id).collect();
        if let Some((_cost, join_order_tree)) = Self::find_min_cost_order(graph, available) {
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
        nodes: Vec<JoinNode>,
        edges: Vec<(usize, usize, usize)>,
    ) -> JoinGraph {
        let mut adj_list = JoinAdjList::empty();
        for (from, to, td) in edges {
            adj_list.add_bidirectional_edge_with_total_domain(
                nodes[from].clone(),
                nodes[to].clone(),
                td,
            );
        }
        JoinGraph::new(adj_list, vec![])
    }

    macro_rules! create_and_test_join_order {
        ($nodes:expr, $edges:expr, $orderer:expr, $optimal_order:expr) => {
            let nodes: Vec<JoinNode> = $nodes
                .iter()
                .map(|(name, size)| {
                    let scan_node = create_scan_node(name, Some(*size));
                    JoinNode::new(name.to_string(), scan_node)
                })
                .collect();
            let graph = create_join_graph_with_edges(nodes.clone(), $edges);
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
            (name_to_id["medium"], name_to_id["large"], 1_000),
            (name_to_id["large"], name_to_id["small"], 500),
            (name_to_id["medium"], name_to_id["small"], 500),
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
            (name_to_id["region"], name_to_id["nation"], 5),
            (name_to_id["nation"], name_to_id["customer"], 25),
            (name_to_id["customer"], name_to_id["orders"], 1_500_000),
            (name_to_id["orders"], name_to_id["lineitem"], 15_000_000),
            (name_to_id["lineitem"], name_to_id["supplier"], 100_000),
            (name_to_id["supplier"], name_to_id["nation"], 25),
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
            (name_to_id["fact"], name_to_id["dim1"], 10_000), // Pretend there was a large filter on dim1.
            (name_to_id["fact"], name_to_id["dim2"], 500),
            (name_to_id["fact"], name_to_id["dim3"], 500), // Pretend there was a small filter on dim3.
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
            (name_to_id["fact"], name_to_id["dim1"], 500_000), // Pretend there was a filter on dim1.
            (name_to_id["fact"], name_to_id["dim2"], 500),
            (name_to_id["dim2"], name_to_id["dim3"], 500),
            (name_to_id["dim2"], name_to_id["dim4"], 250), // Pretend there was a filter on dim4.
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
            (name_to_id["table1"], name_to_id["table2"], 10),
            (name_to_id["table2"], name_to_id["table3"], 2),
            (name_to_id["table3"], name_to_id["table4"], 20),
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
