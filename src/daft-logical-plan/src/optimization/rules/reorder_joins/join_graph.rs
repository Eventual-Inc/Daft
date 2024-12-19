use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_dsl::{col, optimization::replace_columns_with_expressions, ExprRef};

use crate::{
    ops::{Filter, Join, Project},
    LogicalPlan, LogicalPlanBuilder, LogicalPlanRef,
};

#[derive(Debug)]
struct JoinNode {
    relation_name: String,
    plan: LogicalPlanRef,
    final_name: String,
}

// TODO(desmond): We should also take into account user provided values for:
// - null equals null
// - join strategy

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

#[derive(Clone, Debug)]
pub(crate) struct JoinCondition {
    pub left_on: String,
    pub right_on: String,
}

impl JoinCondition {
    pub(crate) fn flip(&self) -> Self {
        JoinCondition {
            left_on: self.right_on.clone(),
            right_on: self.left_on.clone(),
        }
    }
}

pub(crate) struct JoinAdjList(
    pub HashMap<LogicalPlanRef, HashMap<LogicalPlanRef, Vec<JoinCondition>>>,
);

impl std::fmt::Display for JoinAdjList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Join Graph Adjacency List:")?;
        for (node, neighbors) in &self.0 {
            writeln!(f, "Node {}:", node.name())?;
            for (neighbor, join_conds) in neighbors {
                writeln!(f, "  -> {} with conditions:", neighbor.name())?;
                for (i, cond) in join_conds.iter().enumerate() {
                    writeln!(f, "    {}: {} = {}", i, cond.left_on, cond.right_on)?;
                }
            }
        }
        Ok(())
    }
}

impl JoinAdjList {
    fn add_unidirectional_edge(&mut self, left: &JoinNode, right: &JoinNode) {
        // TODO(desmond): We should also keep track of projections that we need to do.
        let join_condition = JoinCondition {
            left_on: left.final_name.clone(),
            right_on: right.final_name.clone(),
        };
        if let Some(neighbors) = self.0.get_mut(&left.plan) {
            if let Some(join_conditions) = neighbors.get_mut(&right.plan) {
                join_conditions.push(join_condition);
            } else {
                neighbors.insert(right.plan.clone(), vec![join_condition]);
            }
        } else {
            let mut neighbors = HashMap::new();
            neighbors.insert(right.plan.clone(), vec![join_condition]);
            self.0.insert(left.plan.clone(), neighbors);
        }
    }
    fn add_bidirectional_edge(&mut self, node1: JoinNode, node2: JoinNode) {
        self.add_unidirectional_edge(&node1, &node2);
        self.add_unidirectional_edge(&node2, &node1);
    }
}

#[derive(Debug)]
pub(crate) enum ProjectionOrFilter {
    Projection(Vec<ExprRef>),
    Filter(ExprRef),
}

/// Representation of a logical plan as edges between relations, along with additional information needed to
/// reconstruct a logcial plan that's equivalent to the plan that produced this graph.
pub(crate) struct JoinGraph {
    pub adj_list: JoinAdjList,
    // List of projections and filters that should be applied after join reordering. This list respects
    // pre-order traversal of projections and filters in the query tree, so we should apply these operators
    // starting from the back of the list.
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraph {
    pub(crate) fn new(
        adj_list: JoinAdjList,
        final_projections_and_filters: Vec<ProjectionOrFilter>,
    ) -> Self {
        Self {
            adj_list,
            final_projections_and_filters,
        }
    }

    pub(crate) fn apply_projections_and_filters_to_plan(
        &mut self,
        plan: LogicalPlanRef,
    ) -> DaftResult<LogicalPlanRef> {
        let mut plan = LogicalPlanBuilder::from(plan);
        // Apply projections and filters in post-traversal order.
        let mut reversed_items = self
            .final_projections_and_filters
            .drain(..)
            .rev()
            .peekable();
        while let Some(projection_or_filter) = reversed_items.next() {
            let is_last = reversed_items.peek().is_none();

            match projection_or_filter {
                ProjectionOrFilter::Projection(projections) => {
                    if is_last {
                        // The final projection is the output projection, so here we select the final projection.
                        plan = plan.select(projections)?;
                    } else {
                        // Intermediate projections might only transform a subset of columns, so we use `with_columns()` instead of `select()`.
                        plan = plan.with_columns(projections)?;
                    }
                }
                ProjectionOrFilter::Filter(predicate) => {
                    plan = plan.filter(predicate)?;
                }
            }
        }
        Ok(plan.build())
    }

    /// Test helper function to get the number of edges that the current graph contains.
    pub(crate) fn num_edges(&self) -> usize {
        let mut num_edges = 0;
        for (_, edges) in &self.adj_list.0 {
            num_edges += edges.len();
        }
        // Each edge is bidirectional, so we divide by 2 to get the correct number of edges.
        num_edges / 2
    }

    /// Test helper function to check that all relations in this graph are connected.
    pub(crate) fn fully_connected(&self) -> bool {
        let start = if let Some((node, _)) = self.adj_list.0.iter().next() {
            node
        } else {
            // There are no nodes. The empty graph is fully connected.
            return true;
        };
        // let start_ptr = Arc::as_ptr(&self.edges[0].0.plan);
        let mut seen = HashSet::new();
        let mut stack = vec![start];

        while let Some(current) = stack.pop() {
            if seen.insert(current) {
                // If this is a new node, add all its neighbors to the stack.
                if let Some(neighbors) = self.adj_list.0.get(current) {
                    stack.extend(neighbors.iter().filter_map(|(neighbor, _)| {
                        if !seen.contains(neighbor) {
                            Some(neighbor)
                        } else {
                            None
                        }
                    }));
                }
            }
        }
        seen.len() == self.adj_list.0.len()
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
    pub(crate) fn contains_edges(&self, to_check: Vec<&str>) -> bool {
        let mut edge_strings = HashSet::new();
        for (left, neighbors) in &self.adj_list.0 {
            for (right, join_conds) in neighbors {
                for join_cond in join_conds {
                    edge_strings.insert(format!(
                        "{}({}) <-> {}({})",
                        left.name(),
                        join_cond.left_on,
                        right.name(),
                        join_cond.right_on
                    ));
                }
            }
        }
        for cur_check in to_check {
            if !edge_strings.contains(cur_check) {
                return false;
            }
        }
        true
    }
}

/// JoinGraphBuilder takes in a logical plan. On .build(), it returns a JoinGraph that represents the given logical plan.
struct JoinGraphBuilder {
    plan: LogicalPlanRef,
    join_conds_to_resolve: Vec<(String, LogicalPlanRef, bool)>,
    final_name_map: HashMap<String, ExprRef>,
    adj_list: JoinAdjList,
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraphBuilder {
    pub(crate) fn build(mut self) -> JoinGraph {
        self.process_node(&self.plan.clone());
        JoinGraph::new(self.adj_list, self.final_projections_and_filters)
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
            adj_list: JoinAdjList(HashMap::new()),
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
                        self.adj_list.add_bidirectional_edge(node1, node2);
                    } else {
                        panic!("Join conditions were unresolved");
                    }
                }
            }
            // TODO(desmond): There are potentially more reorderable nodes. For example, we can move repartitions around.
            _ => {
                // This is an unreorderable node. All unresolved columns coming out of this node should be marked as resolved.
                // TODO(desmond): At this point we should perform a fresh join reorder optimization starting from this
                // node as the root node. We can do this once we add the optimizer rule.
                let mut projections = vec![];
                let mut needs_projection = false;
                let mut seen_names = HashSet::new();
                for (name, _, done) in self.join_conds_to_resolve.iter_mut() {
                    if schema.has_field(name) && !*done && !seen_names.contains(name) {
                        if let Some(final_name) = self.final_name_map.get(name) {
                            let final_name = final_name.name().to_string();
                            if final_name != *name {
                                needs_projection = true;
                                projections.push(col(name.clone()).alias(final_name));
                            } else {
                                projections.push(col(name.clone()));
                            }
                        } else {
                            projections.push(col(name.clone()));
                        }
                        seen_names.insert(name);
                    }
                }
                // Apply projections and return the new plan as the relation for the appropriate join conditions.
                let projected_plan = if needs_projection {
                    let projected_plan = LogicalPlanBuilder::from(plan.clone())
                        .select(projections)
                        .expect("Computed projections could not be applied to relation")
                        .build();
                    Arc::new(Arc::unwrap_or_clone(projected_plan).with_materialized_stats())
                } else {
                    plan.clone()
                };
                for (name, node, done) in self.join_conds_to_resolve.iter_mut() {
                    if schema.has_field(name) && !*done {
                        *done = true;
                        *node = projected_plan.clone();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_scan_info::Pushdowns;
    use common_treenode::TransformedResult;
    use daft_core::prelude::CountMode;
    use daft_dsl::{col, AggExpr, Expr, LiteralValue};
    use daft_schema::{dtype::DataType, field::Field};

    use super::JoinGraphBuilder;
    use crate::{
        optimization::rules::{
            reorder_joins::greedy_join_order::GreedyJoinOrderer, EnrichWithStats, MaterializeScans,
            OptimizerRule,
        },
        test::{
            dummy_scan_node_with_pushdowns, dummy_scan_operator, dummy_scan_operator_with_size,
        },
    };

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
            dummy_scan_operator_with_size(vec![Field::new("a", DataType::Int64)], Some(100)),
            Pushdowns::default(),
        );
        let scan_b = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(vec![Field::new("b", DataType::Int64)], Some(10_000)),
            Pushdowns::default(),
        );
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(vec![Field::new("c_prime", DataType::Int64)], Some(100)),
            Pushdowns::default(),
        )
        .select(vec![col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(vec![Field::new("d", DataType::Int64)], Some(100)),
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
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(a) <-> Source(d)"
        ]));
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
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
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - b <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(b) <-> Source(d)",
        ]));
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
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
        let original_plan = join_plan_2.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a_beta <-> b
        // - a_beta <-> c
        assert!(join_graph.num_edges() == 2);
        assert!(join_graph.contains_edges(vec![
            "Project(a_beta) <-> Source(b)",
            "Project(a_beta) <-> Source(c)",
        ]));
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
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
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(a) <-> Source(d)"
        ]));
        // Check for non-join projections at the end.
        // `c_prime` gets renamed to `c` in the final projection
        let double_proj = col("c").add(col("c")).alias("double");
        assert!(join_graph.contains_projections_and_filters(vec![&double_proj]));
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
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
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(a) <-> Source(d)",
        ]));
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
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
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
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let stats_enricher = EnrichWithStats::new();
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let mut join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Aggregate(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Aggregate(a) <-> Source(d)"
        ]));
        // Projections below the aggregation should not be part of the final projections.
        assert!(!join_graph.contains_projections_and_filters(vec![&a_proj]));
        // Test greedy join reordering.
        let reordered_plan = GreedyJoinOrderer::compute_join_order(&mut join_graph).unwrap();
        assert!(reordered_plan.schema() == original_plan.schema());
    }
}
