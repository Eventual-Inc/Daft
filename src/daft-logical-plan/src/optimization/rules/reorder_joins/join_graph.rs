use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use daft_core::join::JoinType;
use daft_dsl::{col, ExprRef};

use crate::{
    ops::{Filter, Join, Project},
    LogicalPlan, LogicalPlanRef,
};

/// JoinEdges currently represent relations that have an equi-join condition between each other.
#[derive(Debug)]
struct JoinEdge {
    l_relation_name: String,
    lnode: LogicalPlanRef,
    l_final_name: String,
    r_relation_name: String,
    rnode: LogicalPlanRef,
    r_final_name: String,
}

impl JoinEdge {
    /// Helper function that summarizes join edge information.
    fn simple_repr(&self) -> String {
        format!(
            "l_{}#{}({}) <-> r_{}#{}({})",
            self.l_final_name,
            self.lnode.name(),
            self.l_relation_name,
            self.r_final_name,
            self.rnode.name(),
            self.r_relation_name
        )
    }
}

impl Display for JoinEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.simple_repr())
    }
}

#[derive(Debug)]
enum ProjectionOrFilter {
    Projection(Vec<ExprRef>),
    Filter(ExprRef),
}

/// Representation of a logical plan as edges between relations, along with additional information needed to
/// reconstruct a logcial plan that's equivalent to the plan that produced this graph.
struct JoinGraph {
    // TODO(desmond): Instead of simply storing edges, we might want to maintain adjacency lists between
    // relations. We can make this decision later when we implement join order selection.
    edges: Vec<JoinEdge>,
    // List of projections and filters that should be applied after join reordering. This list respects
    // pre-order traversal of projections and filters in the query tree, so we should apply these operators
    // starting from the back of the list.
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraph {
    #[allow(dead_code)]
    pub(crate) fn new(plan: LogicalPlanRef) -> Self {
        let mut join_conds_to_resolve = vec![];
        let output_schema = plan.schema();
        // During join reordering, we might produce an output schema that differs from the initial output schema. For example,
        // columns might be rearranged, or columns that were not originally selected might now be in the output schema.
        // Hence, we take the original output schema and turn it into a projection that we should apply after all other join
        // ordering projections have been applied.
        let output_projection = output_schema
            .fields
            .iter()
            .map(|(name, _)| col(name.clone()))
            .collect();
        let mut graph = Self {
            edges: vec![],
            final_projections_and_filters: vec![ProjectionOrFilter::Projection(output_projection)],
        };
        let mut final_name_map = HashMap::new();
        graph.process_node(&plan, &mut join_conds_to_resolve, &mut final_name_map);
        graph
    }

    /// `process_node` goes down the query tree finding reorderable nodes (e.g. inner joins, filters, certain projects etc)
    /// and extracting join edges. It stops recursing down each branch of the query tree once it hits an unreorderable node
    /// (e.g. an aggregation, an outer join, a source node etc).
    /// It keeps track of the following state:
    /// - join_conds_to_resolve: Join conditions (left_on/right_on) from downstream joins that need to be resolved by
    ///                          linking to some upstream relation.
    /// - final_name_map: Map from a column's current name in the query plan to its name in the final output schema.
    ///
    /// Joins that added conditions to `join_conds_to_resolve` will pop them off the stack after they have been resolved.
    /// Combining each of their resolved `left_on` conditions with their respective resolved `right_on` conditions produces
    /// a join edge between the relation used in the left condition and the relation used in the right condition.
    fn process_node<'a>(
        &mut self,
        plan: &'a LogicalPlanRef,
        join_conds_to_resolve: &mut Vec<(String, LogicalPlanRef, bool)>,
        final_name_map: &mut HashMap<String, String>,
    ) {
        let schema = plan.schema();
        let mut schema_names: HashSet<_> = schema.names().into_iter().collect();
        for (name, node, done) in join_conds_to_resolve.iter_mut() {
            if !*done && schema_names.contains(name) {
                *node = plan.clone();
            }
        }
        match &**plan {
            LogicalPlan::Project(Project {
                input, projection, ..
            }) => {
                // Get the mapping from input->output for projections that don't need computation.
                let projection_input_mapping = projection
                    .iter()
                    .filter_map(|e| e.input_mapping().map(|s| (e.name(), s)))
                    .collect::<HashMap<&'a str, _>>();
                // To be able to reorder through the current projection, all unresolved columns must either have a
                // zero-computation projection, or must not be projected by the current Project node (i.e. it should be
                // resolved from some other branch in the query tree).
                let reorderable_project = join_conds_to_resolve.iter().all(|(name, _, done)| {
                    *done
                        || !schema_names.contains(name)
                        || projection_input_mapping.contains_key(name.as_str())
                });
                if reorderable_project {
                    for (name, _, done) in join_conds_to_resolve.iter_mut() {
                        if !*done {
                            if let Some(new_name) = projection_input_mapping.get(name.as_str()) {
                                // Remove the current name from the list of schema names so that we can produce
                                // a set of non-join-key names for the current Project's schema.
                                schema_names.remove(name);
                                // If we haven't updated the corresponding entry in the final name map, do so now.
                                if let Some(final_name) = final_name_map.remove(name) {
                                    final_name_map.insert(new_name.to_string(), final_name);
                                }
                                *name = new_name.to_string();
                            }
                        }
                    }
                    // Keep track of non-join-key projections so that we can reapply them once we've reordered the query tree.
                    let non_join_key_projections = projection
                        .iter()
                        .filter(|e| schema_names.contains(e.name()))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !non_join_key_projections.is_empty() {
                        self.final_projections_and_filters
                            .push(ProjectionOrFilter::Projection(non_join_key_projections));
                    }
                    // Continue to children.
                    self.process_node(input, join_conds_to_resolve, final_name_map);
                } else {
                    for (name, _, done) in join_conds_to_resolve.iter_mut() {
                        if schema_names.contains(name) {
                            *done = true;
                        }
                    }
                }
            }
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                self.final_projections_and_filters
                    .push(ProjectionOrFilter::Filter(predicate.clone()));
                self.process_node(input, join_conds_to_resolve, final_name_map);
            }
            // Only reorder inner joins with non-empty join conditions.
            LogicalPlan::Join(Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
                ..
            }) if *join_type == JoinType::Inner && !left_on.is_empty() => {
                for l in left_on {
                    let name = l.name();
                    if final_name_map.get(name).is_none() {
                        final_name_map.insert(name.to_string(), name.to_string());
                    }
                    join_conds_to_resolve.push((name.to_string(), plan.clone(), false));
                }
                self.process_node(left, join_conds_to_resolve, final_name_map);
                let mut ready_left = vec![];
                for _ in 0..left_on.len() {
                    ready_left.push(join_conds_to_resolve.pop().unwrap());
                }
                for r in right_on {
                    let name = r.name();
                    if final_name_map.get(name).is_none() {
                        final_name_map.insert(name.to_string(), name.to_string());
                    }
                    join_conds_to_resolve.push((name.to_string(), plan.clone(), false));
                }
                self.process_node(right, join_conds_to_resolve, final_name_map);
                let mut ready_right = vec![];
                for _ in 0..right_on.len() {
                    ready_right.push(join_conds_to_resolve.pop().unwrap());
                }
                for ((lname, lnode, ldone), (rname, rnode, rdone)) in
                    ready_left.into_iter().zip(ready_right.into_iter())
                {
                    if ldone && rdone {
                        self.edges.push(JoinEdge {
                            l_relation_name: lname.clone(),
                            lnode: lnode.clone(),
                            l_final_name: final_name_map.get(&lname).unwrap().to_string(),
                            r_relation_name: rname.clone(),
                            rnode: rnode.clone(),
                            r_final_name: final_name_map.get(&rname).unwrap().to_string(),
                        });
                    }
                }
            }
            // TODO(desmond): There are potentially more reorderable nodes. For example, we can move repartitions around.
            _ => {
                // This is an unreorderable node. All unresolved columns coming out of this node should be marked as resolved.
                for (name, _, done) in join_conds_to_resolve.iter_mut() {
                    if schema_names.contains(name) {
                        *done = true;
                    }
                }
                // TODO(desmond): At this point we should perform a fresh join reorder optimization starting from this
                // node as the root node. We can do this once we add the optimizer rule.
            }
        }
    }

    /// Test helper function to get the number of edges that the current graph contains.
    #[allow(dead_code)]
    pub(crate) fn num_edges(&self) -> usize {
        self.edges.len()
    }

    /// Test helper function to check that all relations in this graph are connected.
    #[allow(dead_code)]
    pub(crate) fn fully_connected(&self) -> bool {
        let relation_set: HashSet<_> = self
            .edges
            .iter()
            .flat_map(|edge| [&edge.lnode, &edge.rnode])
            .collect();
        let num_relations = relation_set.len();
        let relation_map: HashMap<_, _> = relation_set
            .into_iter()
            .enumerate()
            .map(|(id, node)| (node.clone(), id))
            .collect();
        // Perform union between relations using all edges.
        let mut parent: Vec<usize> = (0..num_relations).collect();
        let mut size = vec![1usize; num_relations];
        fn find_set(idx: usize, parent: &mut Vec<usize>) -> usize {
            if idx == parent[idx] {
                idx
            } else {
                parent[idx] = find_set(parent[idx], parent);
                parent[idx]
            }
        }
        for edge in &self.edges {
            let l = relation_map.get(&edge.lnode).unwrap();
            let r = relation_map.get(&edge.rnode).unwrap();
            let lset = find_set(*l, &mut parent);
            let rset = find_set(*r, &mut parent);
            if lset != rset {
                let (small, big) = if size[lset] < size[rset] {
                    (lset, rset)
                } else {
                    (rset, lset)
                };
                parent[small] = big;
                size[big] += size[small];
            }
        }
        let relation_to_test = &self.edges.first().unwrap().lnode;
        let index_to_test = relation_map.get(relation_to_test).unwrap();
        let set_size = size[parent[*index_to_test]];
        set_size == num_relations
    }

    /// Test helper function that checks if the graph contains the given projection/filter expressions
    /// in the given order.
    #[allow(dead_code)]
    pub(crate) fn contains_projections_and_filters(&self, to_check: Vec<&ExprRef>) -> bool {
        let all_exprs: Vec<_> = self
            .final_projections_and_filters
            .iter()
            .flat_map(|p| match p {
                ProjectionOrFilter::Projection(projs) => projs.iter(),
                ProjectionOrFilter::Filter(f) => std::slice::from_ref(f).iter(),
            })
            .collect();
        let mut check_idx = 0;
        for expr in all_exprs {
            if expr == to_check[check_idx] {
                check_idx += 1;
                if check_idx == to_check.len() {
                    return true;
                }
            }
        }
        false
    }

    /// Helper function that loosely checks if a given edge (represented by a simple string)
    /// exists in the current graph.
    #[allow(dead_code)]
    pub(crate) fn contains_edge(&self, edge_string: &str) -> bool {
        for edge in &self.edges {
            if edge.simple_repr() == edge_string {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_scan_info::Pushdowns;
    use daft_core::prelude::CountMode;
    use daft_dsl::{col, AggExpr, Expr, LiteralValue};
    use daft_schema::{dtype::DataType, field::Field};

    use super::JoinGraph;
    use crate::test::{dummy_scan_node_with_pushdowns, dummy_scan_operator};

    #[test]
    fn test_create_join_graph_basic_1() {
        //                InnerJoin (a = d)
        //                 /            \
        //     InnerJoin (a = b)    InnerJoin (c = d)
        //        /        \           /         \
        // Scan(a)      Scan(b)   Project       Scan(d)
        //                       (c <- c_prime)
        //                            |
        //                       Scan(c_prime)
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .test_inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .test_inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_c#Source(c_prime) <-> r_d#Source(d)"));
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_d#Source(d)"));
    }

    #[test]
    fn test_create_join_graph_basic_2() {
        //                InnerJoin (b = d)
        //                 /            \
        //     InnerJoin (a = b)    InnerJoin (c = d)
        //        /        \           /         \
        // Scan(a)      Scan(b)   Project       Scan(d)
        //                        (c_prime)
        //                           |
        //                        Scan(c_prime)
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .test_inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .test_inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("b")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - b <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_c#Source(c_prime) <-> r_d#Source(d)"));
        assert!(join_graph.contains_edge("l_b#Source(b) <-> r_d#Source(d)"));
    }

    #[test]
    fn test_create_join_graph_multiple_renames() {
        //                InnerJoin (a_beta = b)
        //                 /          \
        //            Project        Scan(c)
        //            (a_beta <- a_alpha)
        //             /
        //     InnerJoin (a = c)
        //        /             \
        //    Project            Scan(b)
        //    (a_alpha <- a)
        //       |
        //    Scan(a)
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("a").alias("a_alpha")])
        .unwrap();
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_1 = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a_alpha")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap()
            .select(vec![col("a_alpha").alias("a_beta"), col("b")])
            .unwrap();
        let join_plan_2 = join_plan_1
            .test_inner_join(
                scan_c,
                vec![Arc::new(Expr::Column(Arc::from("a_beta")))],
                vec![Arc::new(Expr::Column(Arc::from("c")))],
            )
            .unwrap();
        let plan = join_plan_2.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - a <-> c
        assert!(join_graph.num_edges() == 2);
        assert!(join_graph.contains_edge("l_a_beta#Source(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_a_beta#Source(a) <-> r_c#Source(c)"));
    }

    #[test]
    fn test_create_join_graph_with_non_join_projections() {
        //                InnerJoin (a = d)
        //                 /            \
        //     InnerJoin (a = b)    InnerJoin (c = d)
        //        /        \           /         \
        // Scan(a)      Scan(b)   Project       Scan(d)
        //                        (c <- c_prime, double <- c_prime + c_prime)
        //                           |
        //                        Scan(c_prime)
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let double_proj = col("c_prime").add(col("c_prime")).alias("double");
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c"), double_proj.clone()])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .test_inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .test_inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_c#Source(c_prime) <-> r_d#Source(d)"));
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_d#Source(d)"));
        // Check for non-join projections at the end.
        assert!(join_graph.contains_projections_and_filters(vec![&double_proj]));
    }

    #[test]
    fn test_create_join_graph_with_non_join_projections_and_filters() {
        //                InnerJoin (a = d)
        //                    /        \
        //                   /       Project
        //                  /        (d, quad <- double + double)
        //                 /               \
        //     InnerJoin (a = b)    InnerJoin (c = d)
        //        /        \           /         \
        // Scan(a)      Scan(b)   Filter        Scan(d)
        //                        (c < 5)
        //                           |
        //                        Project
        //                        (c <- c_prime, double <- c_prime + c_prime)
        //                           |
        //                        Scan(c_prime)
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let double_proj = col("c_prime").add(col("c_prime")).alias("double");
        let filter_c = col("c").lt(Arc::new(Expr::Literal(LiteralValue::Int64(5))));
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c"), double_proj.clone()])
        .unwrap()
        .filter(filter_c.clone())
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let quad_proj = col("double").add(col("double")).alias("quad");
        let join_plan_r = scan_c
            .test_inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap()
            .select(vec![col("d"), quad_proj.clone()])
            .unwrap();
        let join_plan = join_plan_l
            .test_inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_c#Source(c_prime) <-> r_d#Source(d)"));
        assert!(join_graph.contains_edge("l_a#Source(a) <-> r_d#Source(d)"));
        // Check for non-join projections and filters at the end.
        assert!(join_graph.contains_projections_and_filters(vec![
            &quad_proj,
            &filter_c,
            &double_proj,
        ]));
    }

    #[test]
    fn test_create_join_graph_with_agg() {
        //                InnerJoin (a = d)
        //                 /            \
        //     InnerJoin (a = b)    InnerJoin (c = d)
        //        /        \           /         \
        //      Agg      Scan(b)   Project       Scan(d)
        //      (Count(a))         (c_prime)
        //       |                    |
        //      Project            Scan(c_prime)
        //      (a <- a_prime)
        //       |
        //      Scan(a_prime)
        let a_proj = col("a_prime").alias("a");
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![a_proj.clone()])
        .unwrap()
        .aggregate(
            vec![Arc::new(Expr::Agg(AggExpr::Count(
                col("a"),
                CountMode::All,
            )))],
            vec![],
        )
        .unwrap();
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("b", DataType::Int64)]),
            Pushdowns::default(),
        );
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .test_inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .test_inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .test_inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraph::new(plan);
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("l_a#Aggregate(a) <-> r_b#Source(b)"));
        assert!(join_graph.contains_edge("l_c#Source(c_prime) <-> r_d#Source(d)"));
        assert!(join_graph.contains_edge("l_a#Aggregate(a) <-> r_d#Source(d)"));
        // Projections below the aggregation should not be part of the final projections.
        assert!(!join_graph.contains_projections_and_filters(vec![&a_proj]));
    }
}
