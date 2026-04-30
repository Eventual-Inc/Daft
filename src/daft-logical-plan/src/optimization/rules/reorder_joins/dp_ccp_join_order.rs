use std::collections::HashMap;

use super::{
    join_graph::{JoinGraph, JoinOrderTree, JoinOrderer},
    relation_set::RelationSet,
};

/// DP-ccp (Dynamic Programming via Connected subgraph Complement Pairs) join orderer.
///
/// Based on Moerkotte & Neumann 2006. Only enumerates connected subgraph complement
/// pairs, making it efficient for sparse join graphs (chain, star, snowflake schemas).
/// Scales to ~20 relations vs brute force's ~7.
///
/// The implementation separates pair enumeration from DP evaluation: first we collect
/// all (S1, S2) pairs via the csg-cmp-pair enumeration, then process them in order of
/// increasing |S1 ∪ S2| to ensure dp[S1] and dp[S2] are optimal before computing
/// dp[S1 ∪ S2].
pub(crate) struct DpCcpJoinOrderer {}

impl DpCcpJoinOrderer {
    /// Computes the neighborhood of a set `s` in the join graph: union of all
    /// adjacency list neighbors of relations in `s`, minus `s` itself.
    fn neighborhood(graph: &JoinGraph, s: RelationSet) -> RelationSet {
        let mut neighbors = RelationSet::EMPTY;
        for id in s.iter() {
            if let Some(adj) = graph.adj_list.edges.get(&id) {
                for &neighbor_id in adj.keys() {
                    neighbors = neighbors.union(RelationSet::singleton(neighbor_id));
                }
            }
        }
        neighbors.difference(s)
    }

    /// Expand complement s2 of s1 by adding neighbors, collecting pairs.
    fn enumerate_cmp_rec(
        graph: &JoinGraph,
        pairs: &mut Vec<(RelationSet, RelationSet)>,
        s1: RelationSet,
        s2: RelationSet,
        mut excluded: RelationSet,
    ) {
        let neighbors = Self::neighborhood(graph, s2)
            .difference(excluded)
            .difference(s1);
        for v in neighbors.iter() {
            let v_set = RelationSet::singleton(v);
            let s2_ext = s2.union(v_set);
            pairs.push((s1, s2_ext));
            Self::enumerate_cmp_rec(graph, pairs, s1, s2_ext, excluded.union(v_set));
            excluded = excluded.union(v_set);
        }
    }

    /// Find connected complements of s1, collecting pairs.
    fn enumerate_cmp(
        graph: &JoinGraph,
        pairs: &mut Vec<(RelationSet, RelationSet)>,
        s1: RelationSet,
        mut excluded: RelationSet,
    ) {
        let complement_neighbors = Self::neighborhood(graph, s1).difference(excluded);
        for v in complement_neighbors.iter() {
            let v_set = RelationSet::singleton(v);
            pairs.push((s1, v_set));
            Self::enumerate_cmp_rec(graph, pairs, s1, v_set, excluded.union(v_set));
            excluded = excluded.union(v_set);
        }
    }

    /// Expand s1 by adding neighbors, enumerating complements at each step.
    ///
    /// `b_i` is the set of relations with index < i (the starting vertex from the main loop).
    /// Complement enumeration always excludes `b_i ∪ s1`, not the accumulated CSG exclusions.
    fn enumerate_csg_rec(
        graph: &JoinGraph,
        pairs: &mut Vec<(RelationSet, RelationSet)>,
        s1: RelationSet,
        mut excluded: RelationSet,
        b_i: RelationSet,
    ) {
        let neighbors = Self::neighborhood(graph, s1).difference(excluded);
        for v in neighbors.iter() {
            let v_set = RelationSet::singleton(v);
            let s1_ext = s1.union(v_set);
            Self::enumerate_cmp(graph, pairs, s1_ext, b_i.union(s1_ext));
            Self::enumerate_csg_rec(graph, pairs, s1_ext, excluded.union(v_set), b_i);
            excluded = excluded.union(v_set);
        }
    }

    /// Process a single pair, updating the DP table if the result is cheaper.
    fn process_pair(
        graph: &JoinGraph,
        dp: &mut HashMap<RelationSet, (usize, JoinOrderTree)>,
        s1: RelationSet,
        s2: RelationSet,
    ) {
        let entry1 = dp.get(&s1).cloned();
        let entry2 = dp.get(&s2).cloned();
        if let (Some((left_cost, left_tree)), Some((right_cost, right_tree))) = (entry1, entry2) {
            let left_card = left_tree.get_cardinality();
            let right_card = right_tree.get_cardinality();
            // Normalize: smaller cardinality on left.
            let (left_tree, right_tree, left_card, right_card, left_cost, right_cost) =
                if left_card > right_card {
                    (
                        right_tree, left_tree, right_card, left_card, right_cost, left_cost,
                    )
                } else {
                    (
                        left_tree, right_tree, left_card, right_card, left_cost, right_cost,
                    )
                };
            let (connections, total_domain) =
                graph.adj_list.get_connections(&left_tree, &right_tree);
            if !connections.is_empty() {
                let cardinality = left_card * right_card / total_domain;
                // C_out: sum of intermediate cardinalities (System R / DuckDB).
                let cost = cardinality + left_cost + right_cost;
                let combined = s1.union(s2);
                if dp.get(&combined).is_none_or(|(c, _)| cost < *c) {
                    dp.insert(
                        combined,
                        (cost, left_tree.join(right_tree, connections, cardinality)),
                    );
                }
            }
        }
    }
}

impl JoinOrderer for DpCcpJoinOrderer {
    fn order(&self, graph: &JoinGraph) -> JoinOrderTree {
        let n = graph.adj_list.max_id;
        let mut dp: HashMap<RelationSet, (usize, JoinOrderTree)> = HashMap::new();

        // Initialize singletons.
        for i in 0..n {
            let plan = graph
                .adj_list
                .id_to_plan
                .get(&i)
                .expect("Got non-existent ID in join graph");
            let stats = plan.materialized_stats();
            let num_rows = stats.approx_stats.num_rows;
            let s = RelationSet::singleton(i);
            dp.insert(s, (num_rows, JoinOrderTree::Relation(i, num_rows)));
        }

        // Phase 1: Enumerate all connected subgraph complement pairs.
        let mut pairs = Vec::new();
        for i in (0..n).rev() {
            let v_i = RelationSet::singleton(i);
            let b_i = if i == 0 {
                RelationSet::EMPTY
            } else {
                RelationSet::from_range(i)
            };

            Self::enumerate_cmp(graph, &mut pairs, v_i, b_i.union(v_i));
            Self::enumerate_csg_rec(graph, &mut pairs, v_i, b_i, b_i);
        }

        // Phase 2: Sort pairs by combined set size (ascending) so dp[S1] and dp[S2]
        // are optimal before we compute dp[S1 ∪ S2].
        pairs.sort_unstable_by_key(|(s1, s2)| s1.union(*s2).len());

        // Phase 3: Process pairs in order.
        for (s1, s2) in pairs {
            Self::process_pair(graph, &mut dp, s1, s2);
        }

        let full_set = RelationSet::from_range(n);
        if let Some((_cost, tree)) = dp.remove(&full_set) {
            tree
        } else {
            panic!(
                "DP-ccp failed to find a join order for the full relation set; graph may not be fully connected"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use daft_common::treenode::TransformedResult;
    use daft_scan::Pushdowns;
    use daft_schema::{dtype::DataType, field::Field};

    use super::{DpCcpJoinOrderer, JoinOrderTree, JoinOrderer};
    use crate::{
        LogicalPlanRef,
        optimization::rules::{
            EnrichWithStats, MaterializeScans,
            reorder_joins::{
                brute_force_join_order::BruteForceJoinOrderer,
                join_graph::{JoinAdjList, JoinGraph, JoinNode},
            },
            rule::OptimizerRule,
        },
        test::{dummy_scan_node_with_pushdowns, dummy_scan_operator_with_size},
    };

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

    struct JoinEdge {
        node1: usize,
        node1_col_name: String,
        node2: usize,
        node2_col_name: String,
        total_domain: usize,
    }

    fn create_join_graph_with_edges(plans: Vec<LogicalPlanRef>, edges: Vec<JoinEdge>) -> JoinGraph {
        let mut adj_list = JoinAdjList::empty();
        for plan in &plans {
            adj_list.get_or_create_plan_id(plan);
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

    fn node_to_id_map(nodes: Vec<(&str, usize, Vec<&str>)>) -> HashMap<String, usize> {
        nodes
            .into_iter()
            .enumerate()
            .map(|(id, (name, _, _))| (name.to_string(), id))
            .collect()
    }

    /// Cross-validate DP-ccp against brute force: both must produce the same cumulative cost.
    fn cross_validate(nodes: Vec<(&str, usize, Vec<&str>)>, edges: Vec<JoinEdge>) {
        let plans: Vec<LogicalPlanRef> = nodes
            .iter()
            .map(|(_, size, columns)| create_scan_node(Some(*size), columns))
            .collect();
        let num_edges = edges.len();
        let graph = create_join_graph_with_edges(plans, edges);

        let bf_order = BruteForceJoinOrderer {}.order(&graph);
        let dp_order = DpCcpJoinOrderer {}.order(&graph);

        // Both must produce valid join plans.
        assert!(
            JoinOrderTree::num_join_conditions(&bf_order) <= num_edges,
            "Brute force produced too many join conditions"
        );
        assert!(
            JoinOrderTree::num_join_conditions(&dp_order) <= num_edges,
            "DP-ccp produced too many join conditions"
        );

        // Both must build successfully.
        let bf_build = graph.build_joins_from_join_order(&bf_order);
        assert!(bf_build.is_ok(), "Brute force build failed: {:?}", bf_build);
        let dp_build = graph.build_joins_from_join_order(&dp_order);
        assert!(dp_build.is_ok(), "DP-ccp build failed: {:?}", dp_build);

        // Compute cumulative costs (must match the cost formula in process_pair / find_min_cost_order).
        fn cumulative_cost(tree: &JoinOrderTree) -> usize {
            match tree {
                JoinOrderTree::Relation(_, card) => *card,
                JoinOrderTree::Join(left, right, _, card) => {
                    *card + cumulative_cost(left) + cumulative_cost(right)
                }
            }
        }

        let bf_cost = cumulative_cost(&bf_order);
        let dp_cost = cumulative_cost(&dp_order);
        assert_eq!(
            bf_cost, dp_cost,
            "DP-ccp cost ({dp_cost}) differs from brute force cost ({bf_cost})"
        );
    }

    // --- Mirror all brute force test cases, cross-validating costs ---

    #[test]
    fn test_dp_ccp_order_minimal() {
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
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_mock_tpch_q5() {
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
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_mock_tpch_sub_q9() {
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
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_mock_tpch_q9() {
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
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_star_schema() {
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
            },
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
            },
        ];
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_snowflake_schema() {
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
            },
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
            },
        ];
        cross_validate(nodes, edges);
    }

    #[test]
    fn test_dp_ccp_order_bushy_join() {
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
        cross_validate(nodes, edges);
    }

    // --- Larger graph tests beyond brute force's range ---

    #[test]
    fn test_dp_ccp_chain_10() {
        // 10-relation chain: R0 - R1 - R2 - ... - R9
        let nodes: Vec<(&str, usize, Vec<&str>)> = vec![
            ("r0", 1_000, vec!["r0_r1"]),
            ("r1", 5_000, vec!["r1_r0", "r1_r2"]),
            ("r2", 500, vec!["r2_r1", "r2_r3"]),
            ("r3", 10_000, vec!["r3_r2", "r3_r4"]),
            ("r4", 200, vec!["r4_r3", "r4_r5"]),
            ("r5", 50_000, vec!["r5_r4", "r5_r6"]),
            ("r6", 3_000, vec!["r6_r5", "r6_r7"]),
            ("r7", 100, vec!["r7_r6", "r7_r8"]),
            ("r8", 20_000, vec!["r8_r7", "r8_r9"]),
            ("r9", 800, vec!["r9_r8"]),
        ];
        let plans: Vec<LogicalPlanRef> = nodes
            .iter()
            .map(|(_, size, columns)| create_scan_node(Some(*size), columns))
            .collect();
        let col_pairs = [
            (0, "r0_r1", 1, "r1_r0", 1_000),
            (1, "r1_r2", 2, "r2_r1", 500),
            (2, "r2_r3", 3, "r3_r2", 500),
            (3, "r3_r4", 4, "r4_r3", 200),
            (4, "r4_r5", 5, "r5_r4", 200),
            (5, "r5_r6", 6, "r6_r5", 3_000),
            (6, "r6_r7", 7, "r7_r6", 100),
            (7, "r7_r8", 8, "r8_r7", 100),
            (8, "r8_r9", 9, "r9_r8", 800),
        ];
        let mut edges = vec![];
        for (n1, c1, n2, c2, td) in col_pairs {
            edges.push(JoinEdge {
                node1: n1,
                node1_col_name: c1.to_string(),
                node2: n2,
                node2_col_name: c2.to_string(),
                total_domain: td,
            });
        }
        let num_edges = edges.len();
        let graph = create_join_graph_with_edges(plans, edges);
        let dp_order = DpCcpJoinOrderer {}.order(&graph);
        assert!(JoinOrderTree::num_join_conditions(&dp_order) <= num_edges);
        let build = graph.build_joins_from_join_order(&dp_order);
        assert!(build.is_ok(), "DP-ccp chain-10 build failed: {:?}", build);
    }

    #[test]
    fn test_dp_ccp_star_8() {
        // Star: central fact table connected to 7 dimension tables.
        let nodes: Vec<(&str, usize, Vec<&str>)> = vec![
            (
                "fact",
                10_000_000,
                vec!["f_d1", "f_d2", "f_d3", "f_d4", "f_d5", "f_d6", "f_d7"],
            ),
            ("d1", 100, vec!["d1_k"]),
            ("d2", 500, vec!["d2_k"]),
            ("d3", 50, vec!["d3_k"]),
            ("d4", 1_000, vec!["d4_k"]),
            ("d5", 200, vec!["d5_k"]),
            ("d6", 10, vec!["d6_k"]),
            ("d7", 5_000, vec!["d7_k"]),
        ];
        let plans: Vec<LogicalPlanRef> = nodes
            .iter()
            .map(|(_, size, columns)| create_scan_node(Some(*size), columns))
            .collect();
        let star_edges = [
            ("f_d1", 1, "d1_k", 100),
            ("f_d2", 2, "d2_k", 500),
            ("f_d3", 3, "d3_k", 50),
            ("f_d4", 4, "d4_k", 1_000),
            ("f_d5", 5, "d5_k", 200),
            ("f_d6", 6, "d6_k", 10),
            ("f_d7", 7, "d7_k", 5_000),
        ];
        let mut edges = vec![];
        for (fact_col, dim_id, dim_col, td) in star_edges {
            edges.push(JoinEdge {
                node1: 0,
                node1_col_name: fact_col.to_string(),
                node2: dim_id,
                node2_col_name: dim_col.to_string(),
                total_domain: td,
            });
        }
        let num_edges = edges.len();
        let graph = create_join_graph_with_edges(plans, edges);
        let dp_order = DpCcpJoinOrderer {}.order(&graph);
        assert!(JoinOrderTree::num_join_conditions(&dp_order) <= num_edges);
        let build = graph.build_joins_from_join_order(&dp_order);
        assert!(build.is_ok(), "DP-ccp star-8 build failed: {:?}", build);
    }

    #[test]
    fn test_dp_ccp_clique_8() {
        // Fully connected 8-node graph.
        let n = 8;
        let col_names: Vec<Vec<&str>> = (0..n)
            .map(|i| {
                (0..n)
                    .filter(|&j| j != i)
                    .map(|j| Box::leak(format!("c{i}_{j}").into_boxed_str()) as &str)
                    .collect()
            })
            .collect();
        let sizes = [1000, 2000, 500, 3000, 100, 4000, 1500, 800];
        let nodes: Vec<(&str, usize, Vec<&str>)> = (0..n)
            .map(|i| {
                let name: &str = Box::leak(format!("t{i}").into_boxed_str());
                (name, sizes[i], col_names[i].clone())
            })
            .collect();
        let plans: Vec<LogicalPlanRef> = nodes
            .iter()
            .map(|(_, size, columns)| create_scan_node(Some(*size), columns))
            .collect();
        let mut edges = vec![];
        for i in 0..n {
            for j in (i + 1)..n {
                let td = sizes[i].min(sizes[j]);
                edges.push(JoinEdge {
                    node1: i,
                    node1_col_name: format!("c{i}_{j}"),
                    node2: j,
                    node2_col_name: format!("c{j}_{i}"),
                    total_domain: td,
                });
            }
        }
        let num_edges = edges.len();
        let graph = create_join_graph_with_edges(plans, edges);
        let dp_order = DpCcpJoinOrderer {}.order(&graph);
        assert!(JoinOrderTree::num_join_conditions(&dp_order) <= num_edges);
        let build = graph.build_joins_from_join_order(&dp_order);
        assert!(build.is_ok(), "DP-ccp clique-8 build failed: {:?}", build);
    }
}
