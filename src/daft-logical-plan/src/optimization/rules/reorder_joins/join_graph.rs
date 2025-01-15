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

/// A JoinOrderTree is a tree that describes a join order between relations, which can range from left deep trees
/// to bushy trees. A relations in a JoinOrderTree contain IDs instead of logical plan references. An ID's
/// corresponding logical plan reference can be found by consulting the JoinAdjList that was used to produce the
/// given JoinOrderTree.
///
/// TODO(desmond): In the future these trees should keep track of current cost estimates.
#[derive(Clone, Debug)]
pub(super) enum JoinOrderTree {
    Relation(usize),                                                  // (ID).
    Join(Box<JoinOrderTree>, Box<JoinOrderTree>, Vec<JoinCondition>), // (subtree, subtree, join conditions).
}

impl JoinOrderTree {
    pub(super) fn join(self, right: Self, conds: Vec<JoinCondition>) -> Self {
        Self::Join(Box::new(self), Box::new(right), conds)
    }

    // Helper function that checks if the join order tree contains a given id.
    #[cfg(test)]
    pub(super) fn contains(&self, target_id: usize) -> bool {
        match self {
            Self::Relation(id) => *id == target_id,
            Self::Join(left, right, _) => left.contains(target_id) || right.contains(target_id),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Relation(id) => Box::new(std::iter::once(*id)),
            Self::Join(left, right, _) => Box::new(left.iter().chain(right.iter())),
        }
    }
}

pub(super) trait JoinOrderer {
    fn order(&self, graph: &JoinGraph) -> JoinOrderTree;
}

#[derive(Clone, Debug)]
pub(super) struct JoinNode {
    relation_name: String,
    plan: LogicalPlanRef,
}

// TODO(desmond): We should also take into account user provided values for:
// - null equals null
// - join strategy

/// JoinNodes represent a relation (i.e. a non-reorderable logical plan node), the column
/// that's being accessed from the relation, and the final name of the column in the output.
impl JoinNode {
    pub(super) fn new(relation_name: String, plan: LogicalPlanRef) -> Self {
        Self {
            relation_name,
            plan,
        }
    }

    fn simple_repr(&self) -> String {
        format!("{}({})", self.plan.name(), self.relation_name)
    }
}

impl Display for JoinNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.simple_repr())
    }
}

#[derive(Clone, Debug)]
pub(super) struct JoinCondition {
    pub left_on: String,
    pub right_on: String,
}

pub(super) struct JoinAdjList {
    pub max_id: usize,
    plan_to_id: HashMap<*const LogicalPlan, usize>,
    id_to_plan: HashMap<usize, LogicalPlanRef>,
    pub edges: HashMap<usize, HashMap<usize, Vec<JoinCondition>>>,
}

impl std::fmt::Display for JoinAdjList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Join Graph Adjacency List:")?;
        for (node_id, neighbors) in &self.edges {
            let node = self.id_to_plan.get(node_id).unwrap();
            writeln!(f, "Node {} (id = {node_id}):", node.name())?;
            for (neighbor_id, join_conds) in neighbors {
                let neighbor = self.id_to_plan.get(neighbor_id).unwrap();
                writeln!(
                    f,
                    "  -> {} (id = {neighbor_id}) with conditions:",
                    neighbor.name()
                )?;
                for (i, cond) in join_conds.iter().enumerate() {
                    writeln!(f, "    {}: {} = {}", i, cond.left_on, cond.right_on)?;
                }
            }
        }
        Ok(())
    }
}

impl JoinAdjList {
    pub(super) fn empty() -> Self {
        Self {
            max_id: 0,
            plan_to_id: HashMap::new(),
            id_to_plan: HashMap::new(),
            edges: HashMap::new(),
        }
    }

    pub(super) fn get_or_create_plan_id(&mut self, plan: &LogicalPlanRef) -> usize {
        let ptr = Arc::as_ptr(plan);
        if let Some(id) = self.plan_to_id.get(&ptr) {
            *id
        } else {
            let id = self.max_id;
            self.max_id += 1;
            self.plan_to_id.insert(ptr, id);
            self.id_to_plan.insert(id, plan.clone());
            id
        }
    }

    fn add_join_condition(
        &mut self,
        left_id: usize,
        right_id: usize,
        join_condition: JoinCondition,
    ) {
        if let Some(neighbors) = self.edges.get_mut(&left_id) {
            if let Some(join_conditions) = neighbors.get_mut(&right_id) {
                join_conditions.push(join_condition);
            } else {
                neighbors.insert(right_id, vec![join_condition]);
            }
        } else {
            let mut neighbors = HashMap::new();
            neighbors.insert(right_id, vec![join_condition]);
            self.edges.insert(left_id, neighbors);
        }
    }

    fn add_unidirectional_edge(&mut self, left: &JoinNode, right: &JoinNode) {
        let join_condition = JoinCondition {
            left_on: left.relation_name.clone(),
            right_on: right.relation_name.clone(),
        };
        let left_id = self.get_or_create_plan_id(&left.plan);
        let right_id = self.get_or_create_plan_id(&right.plan);
        self.add_join_condition(left_id, right_id, join_condition);
    }

    pub(super) fn add_bidirectional_edge(&mut self, node1: JoinNode, node2: JoinNode) {
        self.add_unidirectional_edge(&node1, &node2);
        self.add_unidirectional_edge(&node2, &node1);
    }

    pub(super) fn get_connections(
        &self,
        left: &JoinOrderTree,
        right: &JoinOrderTree,
    ) -> Vec<JoinCondition> {
        let mut conds = vec![];
        for left_node in left.iter() {
            if let Some(neighbors) = self.edges.get(&left_node) {
                for right_node in right.iter() {
                    if let Some(edges) = neighbors.get(&right_node) {
                        conds.extend(edges.iter().cloned());
                    }
                }
            }
        }
        conds
    }
}

#[derive(Debug)]
pub(super) enum ProjectionOrFilter {
    Projection(Vec<ExprRef>),
    Filter(ExprRef),
}

/// Representation of a logical plan as edges between relations, along with additional information needed to
/// reconstruct a logcial plan that's equivalent to the plan that produced this graph.
pub(super) struct JoinGraph {
    pub adj_list: JoinAdjList,
    // List of projections and filters that should be applied after join reordering. This list respects
    // pre-order traversal of projections and filters in the query tree, so we should apply these operators
    // starting from the back of the list.
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraph {
    pub(super) fn new(
        adj_list: JoinAdjList,
        final_projections_and_filters: Vec<ProjectionOrFilter>,
    ) -> Self {
        Self {
            adj_list,
            final_projections_and_filters,
        }
    }

    fn apply_projections_and_filters_to_plan_builder(
        &mut self,
        mut plan_builder: LogicalPlanBuilder,
    ) -> DaftResult<LogicalPlanBuilder> {
        // Apply projections and filters in post-traversal order.
        while let Some(projection_or_filter) = self.final_projections_and_filters.pop() {
            let is_last = self.final_projections_and_filters.is_empty();

            match projection_or_filter {
                ProjectionOrFilter::Projection(projections) => {
                    if is_last {
                        // The final projection is the output projection, so here we select the final projection.
                        plan_builder = plan_builder.select(projections)?;
                    } else {
                        // Intermediate projections might only transform a subset of columns, so we use `with_columns()` instead of `select()`.
                        plan_builder = plan_builder.with_columns(projections)?;
                    }
                }
                ProjectionOrFilter::Filter(predicate) => {
                    plan_builder = plan_builder.filter(predicate)?;
                }
            }
        }
        Ok(plan_builder)
    }

    /// Converts a `JoinOrderTree` into a tree of inner joins.
    /// Returns a tuple of the logical plan builder consisting of joins, and a bitmask indicating the plan IDs
    /// that are contained within the current logical plan builder. The bitmask is used for determining join
    /// conditions to use when logical plan builders are joined together.
    fn build_joins_from_join_order(
        &self,
        join_order: &JoinOrderTree,
    ) -> DaftResult<LogicalPlanBuilder> {
        match join_order {
            JoinOrderTree::Relation(id) => {
                let relation = self
                    .adj_list
                    .id_to_plan
                    .get(id)
                    .expect("Join order contains non-existent plan id 1");
                Ok(LogicalPlanBuilder::from(relation.clone()))
            }
            JoinOrderTree::Join(left_tree, right_tree, conds) => {
                let left_builder = self.build_joins_from_join_order(left_tree)?;
                let right_builder = self.build_joins_from_join_order(right_tree)?;
                let mut left_cols = vec![];
                let mut right_cols = vec![];
                for cond in conds {
                    left_cols.push(col(cond.left_on.clone()));
                    right_cols.push(col(cond.right_on.clone()));
                }
                Ok(left_builder.inner_join(right_builder, left_cols, right_cols)?)
            }
        }
    }

    /// Takes a `JoinOrderTree` and creates a logical plan from the current join graph.
    /// Takes in `&mut self` because it drains the projections and filters to apply from the current join graph.
    pub(super) fn build_logical_plan(
        &mut self,
        join_order: JoinOrderTree,
    ) -> DaftResult<LogicalPlanRef> {
        let mut plan_builder = self.build_joins_from_join_order(&join_order)?;
        plan_builder = self.apply_projections_and_filters_to_plan_builder(plan_builder)?;
        Ok(plan_builder.build())
    }

    pub(super) fn could_reorder(&self) -> bool {
        // For this join graph to reorder joins, there must be at least 3 relations to join. Otherwise
        // there is only one join to perform and no reordering is needed.
        self.adj_list.max_id >= 3
    }

    /// Test helper function to get the number of edges that the current graph contains.
    #[cfg(test)]
    fn num_edges(&self) -> usize {
        let mut num_edges = 0;
        for edges in self.adj_list.edges.values() {
            num_edges += edges.len();
        }
        // Each edge is bidirectional, so we divide by 2 to get the correct number of edges.
        num_edges / 2
    }

    /// Test helper function to check that all relations in this graph are connected.
    #[cfg(test)]
    fn fully_connected(&self) -> bool {
        let start = if let Some((node, _)) = self.adj_list.edges.iter().next() {
            node
        } else {
            // There are no nodes. The empty graph is fully connected.
            return true;
        };
        let mut seen = HashSet::new();
        let mut stack = vec![start];

        while let Some(current) = stack.pop() {
            if seen.insert(current) {
                // If this is a new node, add all its neighbors to the stack.
                if let Some(neighbors) = self.adj_list.edges.get(current) {
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
        seen.len() == self.adj_list.max_id
    }

    /// Test helper function that checks if the graph contains the given projection/filter expressions
    /// in the given order.
    #[cfg(test)]
    fn contains_projections_and_filters(&self, to_check: Vec<&ExprRef>) -> bool {
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

    #[cfg(test)]
    fn get_node_by_id(&self, id: usize) -> &LogicalPlanRef {
        self.adj_list
            .id_to_plan
            .get(&id)
            .expect("Tried to retrieve a plan from the join graph with an invalid ID")
    }

    /// Helper function that loosely checks if a given edge (represented by a simple string)
    /// exists in the current graph.
    #[cfg(test)]
    fn contains_edges(&self, to_check: Vec<&str>) -> bool {
        let mut edge_strings = HashSet::new();
        for (left_id, neighbors) in &self.adj_list.edges {
            for (right_id, join_conds) in neighbors {
                let left = self.get_node_by_id(*left_id);
                let right = self.get_node_by_id(*right_id);
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
pub(super) struct JoinGraphBuilder {
    plan: LogicalPlanRef,
    join_conds_to_resolve: Vec<(String, LogicalPlanRef, bool)>,
    final_name_map: HashMap<String, ExprRef>,
    adj_list: JoinAdjList,
    final_projections_and_filters: Vec<ProjectionOrFilter>,
}

impl JoinGraphBuilder {
    pub(super) fn build(mut self) -> JoinGraph {
        self.process_node(&self.plan.clone());
        JoinGraph::new(self.adj_list, self.final_projections_and_filters)
    }

    pub(super) fn from_logical_plan(plan: LogicalPlanRef) -> Self {
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
            adj_list: JoinAdjList::empty(),
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
    fn process_node(&mut self, mut plan: &LogicalPlanRef) {
        // Go down the linear chain of Projects and Filters until we hit a join or an unreorderable operator.
        // If we hit a join, we should process all the Projects and Filters that we encountered before the join.
        // If we hit an unreorderable operator, the root plan at the top of this linear chain becomes a relation for
        // join ordering.
        //
        // For example, consider this query tree:
        //
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
        //
        // In between InnerJoin(c=d) and Scan(c_prime) there are Filter and Project nodes. Since there is no join below InnerJoin(c=d),
        // we take the Filter(c<5) operator as the relation to pass into the join (as opposed to using Scan(c_prime) and pushing up
        // the Projects and Filters above it).
        let root_plan = plan;
        loop {
            match &**plan {
                // TODO(desmond): There are potentially more reorderable nodes. For example, we can move repartitions around.
                LogicalPlan::Project(Project {
                    input, projection, ..
                }) => {
                    let projection_input_mapping = projection
                        .iter()
                        .filter_map(|e| e.input_mapping().map(|s| (e.name().to_string(), s)))
                        .collect::<HashMap<String, _>>();
                    // To be able to reorder through the current projection, all unresolved columns must either have a
                    // zero-computation projection, or must not be projected by the current Project node (i.e. it should be
                    // resolved from some other branch in the query tree).
                    let schema = plan.schema();
                    let reorderable_project =
                        self.join_conds_to_resolve.iter().all(|(name, _, done)| {
                            *done
                                || !schema.has_field(name)
                                || projection_input_mapping.contains_key(name.as_str())
                        });
                    if reorderable_project {
                        plan = input;
                    } else {
                        // Encountered a non-reorderable Project. Add the root plan at the top of the current linear chain as a relation to join.
                        self.add_relation(root_plan);
                        break;
                    }
                }
                LogicalPlan::Filter(Filter { input, .. }) => plan = input,
                // Since we hit a join, we need to process the linear chain of Projects and Filters that were encountered starting
                // from the plan at the root of the linear chain to the current plan.
                LogicalPlan::Join(Join {
                    left_on, join_type, ..
                }) if *join_type == JoinType::Inner && !left_on.is_empty() => {
                    self.process_linear_chain(root_plan, plan);
                    break;
                }
                _ => {
                    // Encountered a non-reorderable node. Add the root plan at the top of the current linear chain as a relation to join.
                    self.add_relation(root_plan);
                    break;
                }
            }
        }
    }

    /// `process_linear_chain` is a helper function that pushes up the Projects and Filters from `starting_node` to
    /// `ending_node`. `ending_node` MUST be an inner join.
    ///
    /// After pushing up Projects and Filters, `process_linear_chain` will call `process_node` on the left and right
    /// children of the Join node in `ending_node`.
    fn process_linear_chain(
        &mut self,
        starting_node: &LogicalPlanRef,
        ending_node: &LogicalPlanRef,
    ) {
        let mut cur_node = starting_node;
        while !Arc::ptr_eq(cur_node, ending_node) {
            match &**cur_node {
                LogicalPlan::Project(Project {
                    input, projection, ..
                }) => {
                    // Get the mapping from input->output for projections that don't need computation.
                    let mut compute_projections = vec![];
                    let projection_input_mapping = projection
                        .iter()
                        .filter_map(|e| {
                            let input_mapping = e.input_mapping();
                            if input_mapping.is_none() {
                                compute_projections.push(e.clone());
                            }
                            input_mapping.map(|s| (e.name().to_string(), s))
                        })
                        .collect::<HashMap<String, _>>();
                    for (output, input) in &projection_input_mapping {
                        if let Some(final_name) = self.final_name_map.remove(output) {
                            self.final_name_map.insert(input.clone(), final_name);
                        } else {
                            self.final_name_map
                                .insert(input.clone(), col(output.clone()));
                        }
                    }
                    if !compute_projections.is_empty() {
                        self.final_projections_and_filters
                            .push(ProjectionOrFilter::Projection(compute_projections.clone()));
                    }
                    // Continue to children.
                    cur_node = input;
                }
                LogicalPlan::Filter(Filter {
                    input, predicate, ..
                }) => {
                    let new_predicate =
                        replace_columns_with_expressions(predicate.clone(), &self.final_name_map);
                    self.final_projections_and_filters
                        .push(ProjectionOrFilter::Filter(new_predicate));
                    // Continue to children.
                    cur_node = input;
                }
                _ => unreachable!("process_linear_chain is only called with a linear chain of Project and Filters that end with a Join"),
            }
        }
        match &**cur_node {
            // The cur_node is now at the ending_node which MUST be a join node.
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
                    let final_name = if let Some(final_name) = self.final_name_map.get(name) {
                        final_name.name()
                    } else {
                        self.final_name_map.insert(name.to_string(), col(name));
                        name
                    };
                    self.join_conds_to_resolve.push((
                        final_name.to_string(),
                        cur_node.clone(),
                        false,
                    ));
                }
                self.process_node(left);
                let mut ready_left = vec![];
                for _ in 0..left_on.len() {
                    ready_left.push(self.join_conds_to_resolve.pop().unwrap());
                }
                for r in right_on {
                    let name = r.name();
                    let final_name = if let Some(final_name) = self.final_name_map.get(name) {
                        final_name.name()
                    } else {
                        self.final_name_map.insert(name.to_string(), col(name));
                        name
                    };
                    self.join_conds_to_resolve.push((
                        final_name.to_string(),
                        cur_node.clone(),
                        false,
                    ));
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
                        let node1 = JoinNode::new(lname.clone(), lnode.clone());
                        let node2 = JoinNode::new(rname.clone(), rnode.clone());
                        self.adj_list.add_bidirectional_edge(node1, node2);
                    } else {
                        panic!("Join conditions were unresolved");
                    }
                }
            }
            _ => {
                panic!("Expected a join node")
            }
        }
    }

    /// `process_leaf_relation` is a helper function that processes an unreorderable node that sits below some
    /// Join node(s). `plan` will become one of the relations involved in join ordering.
    fn add_relation(&mut self, plan: &LogicalPlanRef) {
        // All unresolved columns coming out of this node should be marked as resolved.
        // TODO(desmond): At this point we should perform a fresh join reorder optimization starting from this
        // node as the root node. We can do this once we add the optimizer rule.
        let schema = plan.schema();

        let mut projections = vec![];
        let names = schema.names();
        let mut seen_names = HashSet::new();
        let mut needs_projection = false;
        for (input, final_name) in &self.final_name_map {
            if names.contains(input) {
                seen_names.insert(input);
                let final_name = final_name.name().to_string();
                if final_name != *input {
                    projections.push(col(input.clone()).alias(final_name));
                    needs_projection = true;
                } else {
                    projections.push(col(input.clone()));
                }
            }
        }
        // Apply projections and return the new plan as the relation for the appropriate join conditions.
        let projected_plan = if needs_projection {
            // Add the non-join-key columns to the projection.
            for name in &schema.names() {
                if !seen_names.contains(name) {
                    projections.push(col(name.clone()));
                }
            }
            let projected_plan = LogicalPlanBuilder::from(plan.clone())
                .select(projections)
                .expect("Computed projections could not be applied to relation")
                .build();
            Arc::new(Arc::unwrap_or_clone(projected_plan).with_materialized_stats())
        } else {
            plan.clone()
        };
        let projected_schema = projected_plan.schema();
        for (name, node, done) in &mut self.join_conds_to_resolve {
            if projected_schema.has_field(name) && !*done {
                *done = true;
                *node = projected_plan.clone();
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
        optimization::rules::{EnrichWithStats, MaterializeScans, OptimizerRule},
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
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
    }

    #[test]
    fn test_create_join_graph_multiple_renames() {
        //                InnerJoin (a_beta = c)
        //                 /          \
        //            Project        Scan(c)
        //            (a_beta <- a_alpha)
        //             /
        //     InnerJoin (a_alpha = b)
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a_beta <-> b
        // - a_beta <-> c
        assert!(join_graph.num_edges() == 2);
        assert!(join_graph.contains_edges(vec![
            "Project(a_beta) <-> Source(b)",
            "Project(a_beta) <-> Source(c)",
        ]));
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Filter(c) <-> Source(d)",
            "Source(a) <-> Source(d)",
        ]));
        // Check for non-join projections and filters at the end.
        // The join graph should only keep track of projections and filters that sit between joins.
        assert!(join_graph.contains_projections_and_filters(vec![&quad_proj,]));
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
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone()).build();
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
    }
}
