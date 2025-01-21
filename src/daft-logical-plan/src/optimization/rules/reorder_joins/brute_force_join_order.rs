use super::join_graph::{JoinGraph, JoinOrderTree, JoinOrderer};

pub(crate) struct BruteForceJoinOrderer {}

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
    fn find_min_cost_order(
        graph: &JoinGraph,
        available: Vec<usize>,
    ) -> Option<(usize, JoinOrderTree)> {
        if available.len() == 1 {
            let id = available[0];
            let plan = graph
                .adj_list
                .id_to_plan
                .get(&id)
                .expect("Got non-existent ID in join graph");
            let stats = plan.materialized_stats();
            let cost = stats.approx_stats.num_rows;
            return Some((cost, JoinOrderTree::Relation(id)));
        }
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
                        // TODO(desmond): This is a hack to get the total domain of the join. Technically, we should
                        // take the product of total domains from the minimum spanning tree of equivalence relations.
                        let mut total_domains = connections
                            .iter()
                            .map(|conn| conn.total_domain)
                            .collect::<Vec<_>>();
                        total_domains.sort_unstable_by(|a, b| b.cmp(a));
                        let denominator: usize =
                            total_domains.iter().take(connections.len()).product();
                        let cur_cost =
                            (left_cost * right_cost / denominator) + left_cost + right_cost;
                        if let Some(cur_min_cost) = min_cost {
                            if cur_min_cost > cur_cost {
                                min_cost = Some(cur_cost);
                                chosen_plan = Some(
                                    left_join_order_tree.join(right_join_order_tree, connections),
                                );
                            }
                        } else {
                            min_cost = Some(cur_cost);
                            chosen_plan =
                                Some(left_join_order_tree.join(right_join_order_tree, connections));
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

    fn assert_order_contains_all_nodes(order: &JoinOrderTree, graph: &JoinGraph) {
        for id in 0..graph.adj_list.max_id {
            assert!(
                order.contains(id),
                "Graph id {} not found in order {:?}.\n{}",
                id,
                order,
                graph.adj_list
            );
        }
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

    macro_rules! create_and_test_join_graph {
        ($nodes:expr, $edges:expr, $orderer:expr) => {
            let nodes: Vec<JoinNode> = $nodes
                .iter()
                .map(|(name, size)| {
                    let scan_node = create_scan_node(name, Some(*size));
                    JoinNode::new(name.to_string(), scan_node)
                })
                .collect();
            let graph = create_join_graph_with_edges(nodes.clone(), $edges);
            let order = $orderer.order(&graph);
            assert_order_contains_all_nodes(&order, &graph);
        };
    }

    #[test]
    fn test_brute_force_order_mock_tpch() {
        let nodes = vec![
            ("region", 1),
            ("nation", 25),
            ("customer", 1_500_000),
            ("orders", 3_750_000),
            ("lineitem", 60_000_000),
            ("supplier", 100_000),
        ];
        let edges = vec![
            (0, 1, 1),         // region <-> nation
            (1, 2, 25),        // nation <-> customer
            (2, 3, 1_500_000), // customer <-> orders
            (3, 4, 3_750_000), // orders <-> lineitem
            (4, 5, 1),         // lineitem <-> supplier
            (5, 1, 25),        // supplier <-> nation
        ];
        create_and_test_join_graph!(nodes, edges, BruteForceJoinOrderer {});
    }
}
