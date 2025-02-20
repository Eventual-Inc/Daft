use super::join_graph::{JoinGraph, JoinOrderTree, JoinOrderer};

pub(crate) struct NaiveLeftDeepJoinOrderer {}

impl NaiveLeftDeepJoinOrderer {
    fn extend_order(
        graph: &JoinGraph,
        current_order: JoinOrderTree,
        mut available: Vec<usize>,
    ) -> JoinOrderTree {
        if available.is_empty() {
            return current_order;
        }
        for (index, candidate_node_id) in available.iter().enumerate() {
            let right = JoinOrderTree::Relation(*candidate_node_id, 0);
            let (connections, _) = graph.adj_list.get_connections(&current_order, &right);
            if !connections.is_empty() {
                let new_order = current_order.join(right, connections, 0);
                available.remove(index);
                return Self::extend_order(graph, new_order, available);
            }
        }
        panic!("There should be at least one naive join order.");
    }
}

impl JoinOrderer for NaiveLeftDeepJoinOrderer {
    fn order(&self, graph: &JoinGraph) -> JoinOrderTree {
        let available: Vec<usize> = (1..graph.adj_list.max_id).collect();
        // Take a starting order of the node with id 0.
        let starting_order = JoinOrderTree::Relation(0, 0);
        Self::extend_order(graph, starting_order, available)
    }
}

#[cfg(test)]
mod tests {
    use common_scan_info::Pushdowns;
    use common_treenode::TransformedResult;
    use daft_schema::{dtype::DataType, field::Field};
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use super::{JoinGraph, JoinOrderTree, JoinOrderer, NaiveLeftDeepJoinOrderer};
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

    fn create_join_graph_with_edges(nodes: Vec<JoinNode>, edges: Vec<(usize, usize)>) -> JoinGraph {
        let mut adj_list = JoinAdjList::empty();
        for (from, to) in edges {
            adj_list.add_bidirectional_edge(nodes[from].clone(), nodes[to].clone());
        }
        JoinGraph::new(adj_list, vec![])
    }

    macro_rules! create_and_test_join_graph {
        ($nodes:expr, $edges:expr, $orderer:expr) => {
            let nodes: Vec<JoinNode> = $nodes
                .iter()
                .map(|name| {
                    let scan_node = create_scan_node(name, Some(100));
                    JoinNode::new(name.to_string(), scan_node)
                })
                .collect();
            let graph = create_join_graph_with_edges(nodes.clone(), $edges);
            let order = $orderer.order(&graph);
            assert_order_contains_all_nodes(&order, &graph);
        };
    }

    #[test]
    fn test_order_basic_join_graph() {
        let nodes = vec!["a", "b", "c", "d"];
        let edges = vec![
            (0, 2), // node_a <-> node_c
            (1, 2), // node_b <-> node_c
            (2, 3), // node_c <-> node_d
        ];
        create_and_test_join_graph!(nodes, edges, NaiveLeftDeepJoinOrderer {});
    }

    pub struct UnionFind {
        parent: Vec<usize>,
        size: Vec<usize>,
    }

    impl UnionFind {
        pub fn create(num_nodes: usize) -> Self {
            UnionFind {
                parent: (0..num_nodes).collect(),
                size: vec![1; num_nodes],
            }
        }

        pub fn find(&mut self, node: usize) -> usize {
            if self.parent[node] != node {
                self.parent[node] = self.find(self.parent[node]);
            }
            self.parent[node]
        }

        pub fn union(&mut self, node1: usize, node2: usize) {
            let root1 = self.find(node1);
            let root2 = self.find(node2);

            if root1 != root2 {
                let (small, big) = if self.size[root1] < self.size[root2] {
                    (root1, root2)
                } else {
                    (root2, root1)
                };
                self.parent[small] = big;
                self.size[big] += self.size[small];
            }
        }
    }

    fn create_random_connected_graph(num_nodes: usize) -> Vec<(usize, usize)> {
        let mut rng = StdRng::seed_from_u64(0);
        let mut edges = Vec::new();
        let mut uf = UnionFind::create(num_nodes);

        // Get a random order of all possible edges.
        let mut all_edges: Vec<(usize, usize)> = (0..num_nodes)
            .flat_map(|i| (0..i).chain(i + 1..num_nodes).map(move |j| (i, j)))
            .collect();
        all_edges.shuffle(&mut rng);

        // Select edges to form a minimum spanning tree + a random number of extra edges.
        for (a, b) in all_edges {
            if uf.find(a) != uf.find(b) {
                uf.union(a, b);
                edges.push((a, b));
            }
            // Check if we have a minimum spanning tree.
            if edges.len() >= num_nodes - 1 {
                // Once we have a minimum spanning tree, we let a random number of extra edges be added to the graph.
                if rng.gen_bool(0.3) {
                    break;
                }
                edges.push((a, b));
            }
        }

        edges
    }

    const NUM_RANDOM_NODES: usize = 10;

    #[test]
    fn test_order_random_join_graph() {
        let nodes: Vec<String> = (0..NUM_RANDOM_NODES)
            .map(|i| format!("node_{}", i))
            .collect();
        let edges = create_random_connected_graph(NUM_RANDOM_NODES);
        create_and_test_join_graph!(nodes, edges, NaiveLeftDeepJoinOrderer {});
    }
}
