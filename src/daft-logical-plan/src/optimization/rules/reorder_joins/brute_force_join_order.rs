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
                let left_cardinality = left_join_order_tree.get_cardinality();
                let right_cardinality = right_join_order_tree.get_cardinality();
                // Ensure that the left subtree always has the smaller cardinality.
                let (left, right, left_cardinality, right_cardinality) =
                    if left_cardinality > right_cardinality {
                        (
                            right_join_order_tree,
                            left_join_order_tree,
                            right_cardinality,
                            left_cardinality,
                        )
                    } else {
                        (
                            left_join_order_tree,
                            right_join_order_tree,
                            left_cardinality,
                            right_cardinality,
                        )
                    };
                let (connections, total_domain) = graph.adj_list.get_connections(&left, &right);
                if !connections.is_empty() {
                    // If there is a connection between the left and right subgraphs, we compute the cardinality of the
                    // joined graph as the product of all the cardinalities of the relations in the left and right subgraphs,
                    // divided by the selectivity of the join.
                    // Assuming that join keys are uniformly distributed and independent, the selectivity is computed as the reciprocal
                    // of the product of the largest total domains that form a minimum spanning tree of the relations.
                    let cardinality = left_cardinality * right_cardinality / total_domain;
                    // The cost of the join is the sum of the cardinalities of the left and right subgraphs, plus the cardinality of the joined graph.
                    let cur_cost = cardinality + left_cost + right_cost;
                    // Take the join with the lowest summed cardinality.
                    if let Some(cur_min_cost) = min_cost {
                        if *cur_min_cost > cur_cost {
                            *cur_min_cost = cur_cost;
                            *chosen_plan = Some(left.join(right, connections, cardinality));
                        }
                    } else {
                        *min_cost = Some(cur_cost);
                        *chosen_plan = Some(left.join(right, connections, cardinality));
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
        LogicalPlanRef,
        optimization::rules::{
            EnrichWithStats, MaterializeScans,
            reorder_joins::join_graph::{JoinAdjList, JoinNode},
            rule::OptimizerRule,
        },
        test::{dummy_scan_node_with_pushdowns, dummy_scan_operator_with_size},
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

    fn create_scan_node(size: Option<usize>, columns: &Vec<&str>) -> LogicalPlanRef {
        let fields = columns
            .iter()
            .map(|&col_name| Field::new(col_name, DataType::Int64))
            .collect();
        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(fields, size),
            Pushdowns::default(),
        )
        .build();
        let scan_materializer = MaterializeScans::new();
        let plan = scan_materializer.try_optimize(plan).data().unwrap();
        let stats_enricher = EnrichWithStats::new(None);
        stats_enricher.try_optimize(plan).data().unwrap()
    }

    // A helper struct to represent a join edge between two nodes with the total domain of the join columns.
    // i.e. node1.node1_col_name = node2.node2_col_name.
    struct JoinEdge {
        node1: usize,
        node1_col_name: String,
        node2: usize,
        node2_col_name: String,
        total_domain: usize,
    }

    fn create_join_graph_with_edges(plans: Vec<LogicalPlanRef>, edges: Vec<JoinEdge>) -> JoinGraph {
        let mut adj_list = JoinAdjList::empty();
        // Immediately create plan ids so that they match the ids in the test cases.
        for plan in &plans {
            adj_list.get_or_create_plan_id(&plan);
        }
        for edge in edges {
            adj_list.add_bidirectional_edge_with_total_domain(
                JoinNode::new(edge.node1_col_name, plans[edge.node1].clone()),
                JoinNode::new(edge.node2_col_name, plans[edge.node2].clone()),
                edge.total_domain,
            );
        }
        JoinGraph::new(adj_list, vec![])
    }

    macro_rules! create_and_test_join_order {
        ($nodes:expr, $edges:expr, $orderer:expr, $optimal_order:expr) => {
            let plans: Vec<LogicalPlanRef> = $nodes
                .iter()
                .map(|(_, size, columns)| create_scan_node(Some(*size), columns))
                .collect();
            let num_edges = $edges.len();
            let graph = create_join_graph_with_edges(plans.clone(), $edges);
            let order = $orderer.order(&graph);
            assert!(JoinOrderTree::order_eq(&order, &$optimal_order));
            // Check that the number of join conditions does not increase due to join edge inference.
            assert!(JoinOrderTree::num_join_conditions(&order) <= num_edges);
            let build_result = graph.build_joins_from_join_order(&order);
            assert!(build_result.is_ok(), "Failed to build joins from join order: {:?}", build_result);
        };
    }

    fn node_to_id_map(nodes: Vec<(&str, usize, Vec<&str>)>) -> HashMap<String, usize> {
        nodes
            .into_iter()
            .enumerate()
            .map(|(id, (name, _, _))| (name.to_string(), id))
            .collect()
    }

    #[test]
    fn test_brute_force_order_minimal() {
        let nodes = vec![
            ("medium", 1_000, vec!["m_medium", "m_small"]),
            ("large", 500_000, vec!["l_medium", "l_small"]),
            ("small", 500, vec!["s_small"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["medium"],
                node1_col_name: "m_medium".to_string(),
                node2: name_to_id["large"],
                node2_col_name: "l_medium".to_string(),
                total_domain: 1_000,
            },
            JoinEdge {
                node1: name_to_id["large"],
                node1_col_name: "l_small".to_string(),
                node2: name_to_id["small"],
                node2_col_name: "s_small".to_string(),
                total_domain: 500,
            },
            JoinEdge {
                node1: name_to_id["medium"],
                node1_col_name: "m_small".to_string(),
                node2: name_to_id["small"],
                node2_col_name: "s_small".to_string(),
                total_domain: 500,
            },
        ];
        let optimal_order = test_join(
            test_join(
                test_relation(name_to_id["small"]),
                test_relation(name_to_id["medium"]),
            ),
            test_relation(name_to_id["large"]),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_mock_tpch_q5() {
        let nodes = vec![
            ("region", 1, vec!["r_regionkey"]),
            ("nation", 25, vec!["n_regionkey", "n_nationkey"]),
            ("customer", 1_500_000, vec!["c_custkey", "c_nationkey"]),
            ("orders", 3_000_000, vec!["o_custkey", "o_orderkey"]),
            ("lineitem", 60_000_000, vec!["l_orderkey", "l_suppkey"]),
            ("supplier", 100_000, vec!["s_suppkey", "s_nationkey"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["region"],
                node1_col_name: "r_regionkey".to_string(),
                node2: name_to_id["nation"],
                node2_col_name: "n_regionkey".to_string(),
                total_domain: 10,
            },
            JoinEdge {
                node1: name_to_id["customer"],
                node1_col_name: "c_custkey".to_string(),
                node2: name_to_id["orders"],
                node2_col_name: "o_custkey".to_string(),
                total_domain: 1_500_000,
            },
            JoinEdge {
                node1: name_to_id["orders"],
                node1_col_name: "o_orderkey".to_string(),
                node2: name_to_id["lineitem"],
                node2_col_name: "l_orderkey".to_string(),
                total_domain: 15_000_000,
            },
            JoinEdge {
                node1: name_to_id["lineitem"],
                node1_col_name: "l_suppkey".to_string(),
                node2: name_to_id["supplier"],
                node2_col_name: "s_suppkey".to_string(),
                total_domain: 100_000,
            },
            JoinEdge {
                node1: name_to_id["supplier"],
                node1_col_name: "s_nationkey".to_string(),
                node2: name_to_id["nation"],
                node2_col_name: "n_nationkey".to_string(),
                total_domain: 25,
            },
            JoinEdge {
                node1: name_to_id["customer"],
                node1_col_name: "c_nationkey".to_string(),
                node2: name_to_id["supplier"],
                node2_col_name: "s_nationkey".to_string(),
                total_domain: 25,
            },
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["supplier"]),
            test_join(
                test_join(
                    test_join(
                        test_join(
                            test_relation(name_to_id["region"]),
                            test_relation(name_to_id["nation"]),
                        ),
                        test_relation(name_to_id["customer"]),
                    ),
                    test_relation(name_to_id["orders"]),
                ),
                test_relation(name_to_id["lineitem"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_mock_tpch_sub_q9() {
        let nodes = vec![
            ("nation", 25, vec!["n_nationkey"]),
            ("supplier", 100_000, vec!["s_nationkey", "s_suppkey"]),
            ("part", 100_000, vec!["p_partkey"]),
            ("partsupp", 8_000_000, vec!["ps_partkey", "ps_suppkey"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_partkey".to_string(),
                node2: name_to_id["part"],
                node2_col_name: "p_partkey".to_string(),
                total_domain: 2_000_000,
            },
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_suppkey".to_string(),
                node2: name_to_id["supplier"],
                node2_col_name: "s_suppkey".to_string(),
                total_domain: 100_000,
            },
            JoinEdge {
                node1: name_to_id["supplier"],
                node1_col_name: "s_nationkey".to_string(),
                node2: name_to_id["nation"],
                node2_col_name: "n_nationkey".to_string(),
                total_domain: 25,
            },
        ];
        let optimal_order = test_join(
            test_join(
                test_relation(name_to_id["nation"]),
                test_relation(name_to_id["supplier"]),
            ),
            test_join(
                test_relation(name_to_id["part"]),
                test_relation(name_to_id["partsupp"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_mock_tpch_q9() {
        let nodes = vec![
            ("nation", 22, vec!["n_nationkey"]),
            ("orders", 1_350_000, vec!["o_custkey", "o_orderkey"]),
            (
                "lineitem",
                4_374_885,
                vec!["l_orderkey", "l_suppkey", "l_partkey"],
            ),
            ("supplier", 8_100, vec!["s_suppkey", "s_nationkey"]),
            ("part", 18_000, vec!["p_partkey"]),
            ("partsupp", 648_000, vec!["ps_partkey", "ps_suppkey"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_partkey".to_string(),
                node2: name_to_id["part"],
                node2_col_name: "p_partkey".to_string(),
                total_domain: 200_000,
            },
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_partkey".to_string(),
                node2: name_to_id["lineitem"],
                node2_col_name: "l_partkey".to_string(),
                total_domain: 200_000,
            },
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_suppkey".to_string(),
                node2: name_to_id["lineitem"],
                node2_col_name: "l_suppkey".to_string(),
                total_domain: 10_000,
            },
            JoinEdge {
                node1: name_to_id["partsupp"],
                node1_col_name: "ps_suppkey".to_string(),
                node2: name_to_id["supplier"],
                node2_col_name: "s_suppkey".to_string(),
                total_domain: 10_000,
            },
            JoinEdge {
                node1: name_to_id["orders"],
                node1_col_name: "o_orderkey".to_string(),
                node2: name_to_id["lineitem"],
                node2_col_name: "l_orderkey".to_string(),
                total_domain: 1_500_000,
            },
            JoinEdge {
                node1: name_to_id["lineitem"],
                node1_col_name: "l_partkey".to_string(),
                node2: name_to_id["part"],
                node2_col_name: "p_partkey".to_string(),
                total_domain: 200_000,
            },
            JoinEdge {
                node1: name_to_id["lineitem"],
                node1_col_name: "l_suppkey".to_string(),
                node2: name_to_id["supplier"],
                node2_col_name: "s_suppkey".to_string(),
                total_domain: 10_000,
            },
            JoinEdge {
                node1: name_to_id["supplier"],
                node1_col_name: "s_nationkey".to_string(),
                node2: name_to_id["nation"],
                node2_col_name: "n_nationkey".to_string(),
                total_domain: 25,
            },
        ];
        let optimal_order = test_join(
            test_join(
                test_join(
                    test_join(
                        test_relation(name_to_id["nation"]),
                        test_relation(name_to_id["supplier"]),
                    ),
                    test_join(
                        test_relation(name_to_id["part"]),
                        test_relation(name_to_id["partsupp"]),
                    ),
                ),
                test_relation(name_to_id["lineitem"]),
            ),
            test_relation(name_to_id["orders"]),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }
    #[test]
    fn test_brute_force_order_star_schema() {
        let nodes = vec![
            ("fact", 10_000_000, vec!["f_dim1", "f_dim2", "f_dim3"]),
            ("dim1", 100, vec!["d_dim1"]),
            ("dim2", 500, vec!["d_dim2"]),
            ("dim3", 50, vec!["d_dim3"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["fact"],
                node1_col_name: "f_dim1".to_string(),
                node2: name_to_id["dim1"],
                node2_col_name: "d_dim1".to_string(),
                total_domain: 10_000,
            }, // Pretend there was a large filter on dim1.
            JoinEdge {
                node1: name_to_id["fact"],
                node1_col_name: "f_dim2".to_string(),
                node2: name_to_id["dim2"],
                node2_col_name: "d_dim2".to_string(),
                total_domain: 500,
            },
            JoinEdge {
                node1: name_to_id["fact"],
                node1_col_name: "f_dim3".to_string(),
                node2: name_to_id["dim3"],
                node2_col_name: "d_dim3".to_string(),
                total_domain: 500,
            }, // Pretend there was a small filter on dim3.
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["dim2"]),
            test_join(
                test_relation(name_to_id["dim3"]),
                test_join(
                    test_relation(name_to_id["dim1"]),
                    test_relation(name_to_id["fact"]),
                ),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_snowflake_schema() {
        let nodes = vec![
            ("fact", 50_000_000, vec!["f_dim1", "f_dim2"]),
            ("dim1", 50_000, vec!["d_dim1"]),
            ("dim2", 500, vec!["d_dim2", "d_dim3", "d_dim4"]),
            ("dim3", 500, vec!["d_dim3"]),
            ("dim4", 25, vec!["d_dim4"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["fact"],
                node1_col_name: "f_dim1".to_string(),
                node2: name_to_id["dim1"],
                node2_col_name: "d_dim1".to_string(),
                total_domain: 500_000,
            }, // Pretend there was a filter on dim1.
            JoinEdge {
                node1: name_to_id["fact"],
                node1_col_name: "f_dim2".to_string(),
                node2: name_to_id["dim2"],
                node2_col_name: "d_dim2".to_string(),
                total_domain: 500,
            },
            JoinEdge {
                node1: name_to_id["dim2"],
                node1_col_name: "d_dim3".to_string(),
                node2: name_to_id["dim3"],
                node2_col_name: "d_dim3".to_string(),
                total_domain: 500,
            },
            JoinEdge {
                node1: name_to_id["dim2"],
                node1_col_name: "d_dim4".to_string(),
                node2: name_to_id["dim4"],
                node2_col_name: "d_dim4".to_string(),
                total_domain: 250,
            }, // Pretend there was a filter on dim4.
        ];
        let optimal_order = test_join(
            test_relation(name_to_id["dim1"]),
            test_join(
                test_join(
                    test_join(
                        test_relation(name_to_id["dim4"]),
                        test_relation(name_to_id["dim2"]),
                    ),
                    test_relation(name_to_id["dim3"]),
                ),
                test_relation(name_to_id["fact"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }

    #[test]
    fn test_brute_force_order_bushy_join() {
        let nodes = vec![
            ("table1", 10_000, vec!["t1_t2"]),
            ("table2", 1_000_000, vec!["t2_t2", "t2_t3"]),
            ("table3", 10_000, vec!["t3_t3", "t3_t4"]),
            ("table4", 1_000_000, vec!["t4_t4"]),
        ];
        let name_to_id = node_to_id_map(nodes.clone());
        let edges = vec![
            JoinEdge {
                node1: name_to_id["table1"],
                node1_col_name: "t1_t2".to_string(),
                node2: name_to_id["table2"],
                node2_col_name: "t2_t2".to_string(),
                total_domain: 10,
            },
            JoinEdge {
                node1: name_to_id["table2"],
                node1_col_name: "t2_t3".to_string(),
                node2: name_to_id["table3"],
                node2_col_name: "t3_t3".to_string(),
                total_domain: 2,
            },
            JoinEdge {
                node1: name_to_id["table3"],
                node1_col_name: "t3_t4".to_string(),
                node2: name_to_id["table4"],
                node2_col_name: "t4_t4".to_string(),
                total_domain: 20,
            },
        ];
        let optimal_order = test_join(
            test_join(
                test_relation(name_to_id["table3"]),
                test_relation(name_to_id["table4"]),
            ),
            test_join(
                test_relation(name_to_id["table1"]),
                test_relation(name_to_id["table2"]),
            ),
        );
        create_and_test_join_order!(nodes, edges, BruteForceJoinOrderer {}, optimal_order);
    }
}
