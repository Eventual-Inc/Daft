use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use daft_core::join::JoinType;
use daft_dsl::{col, optimization::replace_columns_with_expressions, ExprRef};

use crate::{
    ops::{Filter, Join, Project},
    LogicalPlan, LogicalPlanRef,
};

#[derive(Debug)]
struct JoinNode {
    relation_name: String,
    plan: LogicalPlanRef,
    final_name: String,
}

/// JoinNodes represent a relation (i.e. a non-reorderable logical plan node), the column
/// that's being accessed from the relation, and the final name of the column in the output.
impl JoinNode {
    fn new(relation_name: String, plan: LogicalPlanRef, final_name: String) -> Self {
        Self {
            relation_name,
            plan,
            final_name,
        }
    }

    fn simple_repr(&self) -> String {
        format!(
            "{}#{}({})",
            self.final_name,
            self.plan.name(),
            self.relation_name
        )
    }
}

impl Display for JoinNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.simple_repr())
    }
}

/// JoinEdges currently represent a bidirectional edge between two relations that have
/// an equi-join condition between each other.
#[derive(Debug)]
struct JoinEdge(JoinNode, JoinNode);

impl JoinEdge {
    /// Helper function that summarizes join edge information.
    fn simple_repr(&self) -> String {
        format!("{} <-> {}", self.0, self.1)
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
    pub(crate) fn new(
        edges: Vec<JoinEdge>,
        final_projections_and_filters: Vec<ProjectionOrFilter>,
    ) -> Self {
        Self {
            edges,
            final_projections_and_filters,
        }
    }

    /// Test helper function to get the number of edges that the current graph contains.
    pub(crate) fn num_edges(&self) -> usize {
        self.edges.len()
    }

    /// Test helper function to check that all relations in this graph are connected.
    pub(crate) fn fully_connected(&self) -> bool {
        // Assuming that we're not testing an empty graph, there should be at least one edge in a connected graph.
        if self.edges.is_empty() {
            return false;
        }
        let mut adj_list: HashMap<*const _, Vec<*const _>> = HashMap::new();
        for edge in &self.edges {
            let l_ptr = Arc::as_ptr(&edge.0.plan);
            let r_ptr = Arc::as_ptr(&edge.1.plan);

            adj_list.entry(l_ptr).or_default().push(r_ptr);
            adj_list.entry(r_ptr).or_default().push(l_ptr);
        }
        let start_ptr = Arc::as_ptr(&self.edges[0].0.plan);
        let mut seen = HashSet::new();
        let mut stack = vec![start_ptr];

        while let Some(current) = stack.pop() {
            if seen.insert(current) {
                // If this is a new node, add all its neighbors to the stack.
                if let Some(neighbors) = adj_list.get(&current) {
                    stack.extend(neighbors.iter().filter(|&&n| !seen.contains(&n)));
                }
            }
        }
        seen.len() == adj_list.len()
    }

    /// Test helper function that checks if the graph contains the given projection/filter expressions
    /// in the given order.
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
    pub(crate) fn contains_edge(&self, edge_string: &str) -> bool {
        for edge in &self.edges {
            if edge.simple_repr() == edge_string {
                return true;
            }
        }
        false
    }
}

/// JoinGraphBuilder takes in a logical plan. On .build(), it returns a JoinGraph that represents the given logical plan.
struct JoinGraphBuilder {
    plan: LogicalPlanRef,
    join_conds_to_resolve: Vec<(String, LogicalPlanRef, bool)>,
    final_name_map: HashMap<String, ExprRef>,
    edges: Vec<JoinEdge>,
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraphBuilder {
    pub(crate) fn build(mut self) -> JoinGraph {
        self.process_node(&self.plan.clone());
        JoinGraph::new(self.edges, self.final_projections_and_filters)
    }

    pub(crate) fn from_logical_plan(plan: LogicalPlanRef) -> Self {
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
        Self {
            plan,
            join_conds_to_resolve: vec![],
            final_name_map: HashMap::new(),
            edges: vec![],
            final_projections_and_filters: vec![ProjectionOrFilter::Projection(output_projection)],
        }
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
    fn process_node<'a>(&mut self, plan: &'a LogicalPlanRef) {
        let schema = plan.schema();
        for (name, node, done) in self.join_conds_to_resolve.iter_mut() {
            if !*done && schema.has_field(name) {
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
                    .filter_map(|e| e.input_mapping().map(|s| (e.name().to_string(), col(s))))
                    .collect::<HashMap<String, _>>();
                // To be able to reorder through the current projection, all unresolved columns must either have a
                // zero-computation projection, or must not be projected by the current Project node (i.e. it should be
                // resolved from some other branch in the query tree).
                let reorderable_project =
                    self.join_conds_to_resolve.iter().all(|(name, _, done)| {
                        *done
                            || !schema.has_field(name)
                            || projection_input_mapping.contains_key(name.as_str())
                    });
                if reorderable_project {
                    let mut non_join_names: HashSet<String> = schema.names().into_iter().collect();
                    for (name, _, done) in self.join_conds_to_resolve.iter_mut() {
                        if !*done {
                            if let Some(new_expr) = projection_input_mapping.get(name) {
                                // Remove the current name from the list of schema names so that we can produce
                                // a set of non-join-key names for the current Project's schema.
                                non_join_names.remove(name);
                                // If we haven't updated the corresponding entry in the final name map, do so now.
                                if let Some(final_name) = self.final_name_map.remove(name) {
                                    self.final_name_map
                                        .insert(new_expr.name().to_string(), final_name);
                                }
                                *name = new_expr.name().to_string();
                            }
                        }
                    }
                    // Keep track of non-join-key projections so that we can reapply them once we've reordered the query tree.
                    let non_join_key_projections = projection
                        .iter()
                        .filter(|e| non_join_names.contains(e.name()))
                        .map(|e| replace_columns_with_expressions(e.clone(), &self.final_name_map))
                        .collect::<Vec<_>>();
                    if !non_join_key_projections.is_empty() {
                        self.final_projections_and_filters
                            .push(ProjectionOrFilter::Projection(non_join_key_projections));
                    }
                    // Continue to children.
                    self.process_node(input);
                } else {
                    for (name, _, done) in self.join_conds_to_resolve.iter_mut() {
                        if schema.has_field(name) {
                            *done = true;
                        }
                    }
                }
            }
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let new_predicate =
                    replace_columns_with_expressions(predicate.clone(), &self.final_name_map);
                self.final_projections_and_filters
                    .push(ProjectionOrFilter::Filter(new_predicate));
                self.process_node(input);
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
                    if self.final_name_map.get(name).is_none() {
                        self.final_name_map.insert(name.to_string(), col(name));
                    }
                    self.join_conds_to_resolve
                        .push((name.to_string(), plan.clone(), false));
                }
                self.process_node(left);
                let mut ready_left = vec![];
                for _ in 0..left_on.len() {
                    ready_left.push(self.join_conds_to_resolve.pop().unwrap());
                }
                for r in right_on {
                    let name = r.name();
                    if self.final_name_map.get(name).is_none() {
                        self.final_name_map.insert(name.to_string(), col(name));
                    }
                    self.join_conds_to_resolve
                        .push((name.to_string(), plan.clone(), false));
                }
                self.process_node(right);
                let mut ready_right = vec![];
                for _ in 0..right_on.len() {
                    ready_right.push(self.join_conds_to_resolve.pop().unwrap());
                }
                for ((lname, lnode, ldone), (rname, rnode, rdone)) in
                    ready_left.into_iter().zip(ready_right.into_iter())
                {
                    if ldone && rdone {
                        let node1 = JoinNode::new(
                            lname.clone(),
                            lnode.clone(),
                            self.final_name_map.get(&lname).unwrap().name().to_string(),
                        );
                        let node2 = JoinNode::new(
                            rname.clone(),
                            rnode.clone(),
                            self.final_name_map.get(&rname).unwrap().name().to_string(),
                        );
                        self.edges.push(JoinEdge(node1, node2));
                    } else {
                        panic!("Join conditions were unresolved");
                    }
                }
            }
            // TODO(desmond): There are potentially more reorderable nodes. For example, we can move repartitions around.
            _ => {
                // This is an unreorderable node. All unresolved columns coming out of this node should be marked as resolved.
                for (name, _, done) in self.join_conds_to_resolve.iter_mut() {
                    if schema.has_field(name) {
                        *done = true;
                    }
                }
                // TODO(desmond): At this point we should perform a fresh join reorder optimization starting from this
                // node as the root node. We can do this once we add the optimizer rule.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_scan_info::Pushdowns;
    use daft_core::prelude::CountMode;
    use daft_dsl::{col, AggExpr, Expr, LiteralValue};
    use daft_schema::{dtype::DataType, field::Field};

    use super::JoinGraphBuilder;
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
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("a#Source(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("c#Source(c_prime) <-> d#Source(d)"));
        assert!(join_graph.contains_edge("a#Source(a) <-> d#Source(d)"));
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
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("b")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - b <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("a#Source(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("c#Source(c_prime) <-> d#Source(d)"));
        assert!(join_graph.contains_edge("b#Source(b) <-> d#Source(d)"));
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
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a_alpha")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap()
            .select(vec![col("a_alpha").alias("a_beta"), col("b")])
            .unwrap();
        let join_plan_2 = join_plan_1
            .inner_join(
                scan_c,
                vec![Arc::new(Expr::Column(Arc::from("a_beta")))],
                vec![Arc::new(Expr::Column(Arc::from("c")))],
            )
            .unwrap();
        let plan = join_plan_2.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - a <-> c
        assert!(join_graph.num_edges() == 2);
        assert!(join_graph.contains_edge("a_beta#Source(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("a_beta#Source(a) <-> c#Source(c)"));
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
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("a#Source(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("c#Source(c_prime) <-> d#Source(d)"));
        assert!(join_graph.contains_edge("a#Source(a) <-> d#Source(d)"));
        // Check for non-join projections at the end.
        // `c_prime` gets renamed to `c` in the final projection
        let double_proj = col("c").add(col("c")).alias("double");
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
        //                        Filter
        //                        (c_prime > 0)
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
        let filter_c_prime = col("c_prime").gt(Arc::new(Expr::Literal(LiteralValue::Int64(0))));
        let filter_c = col("c").lt(Arc::new(Expr::Literal(LiteralValue::Int64(5))));
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .filter(filter_c_prime.clone())
        .unwrap()
        .select(vec![col("c_prime").alias("c"), double_proj.clone()])
        .unwrap()
        .filter(filter_c.clone())
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let quad_proj = col("double").add(col("double")).alias("quad");
        let join_plan_r = scan_c
            .inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap()
            .select(vec![col("d"), quad_proj.clone()])
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("a#Source(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("c#Source(c_prime) <-> d#Source(d)"));
        assert!(join_graph.contains_edge("a#Source(a) <-> d#Source(d)"));
        // Check for non-join projections and filters at the end.
        // `c_prime` gets renamed to `c` in the final projection
        let double_proj = col("c").add(col("c")).alias("double");
        let filter_c_prime = col("c").gt(Arc::new(Expr::Literal(LiteralValue::Int64(0))));
        assert!(join_graph.contains_projections_and_filters(vec![
            &quad_proj,
            &filter_c,
            &double_proj,
            &filter_c_prime,
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
            .inner_join(
                scan_b,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("b")))],
            )
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(
                scan_d,
                vec![Arc::new(Expr::Column(Arc::from("c")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(
                join_plan_r,
                vec![Arc::new(Expr::Column(Arc::from("a")))],
                vec![Arc::new(Expr::Column(Arc::from("d")))],
            )
            .unwrap();
        let plan = join_plan.build();
        let join_graph = JoinGraphBuilder::from_logical_plan(plan).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c_prime <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edge("a#Aggregate(a) <-> b#Source(b)"));
        assert!(join_graph.contains_edge("c#Source(c_prime) <-> d#Source(d)"));
        assert!(join_graph.contains_edge("a#Aggregate(a) <-> d#Source(d)"));
        // Projections below the aggregation should not be part of the final projections.
        assert!(!join_graph.contains_projections_and_filters(vec![&a_proj]));
    }
}
