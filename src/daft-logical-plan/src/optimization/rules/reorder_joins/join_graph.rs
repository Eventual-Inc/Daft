use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_algebra::boolean::combine_conjunction;
use daft_core::join::JoinType;
use daft_dsl::{
    Expr, ExprRef, left_col, optimization::replace_columns_with_expressions, resolved_col,
    right_col,
};

use crate::{
    LogicalPlan, LogicalPlanBuilder, LogicalPlanRef,
    ops::{Filter, Join, Project, join::JoinPredicate},
};

/// A JoinOrderTree is a tree that describes a join order between relations, which can range from left deep trees
/// to bushy trees. A relations in a JoinOrderTree contain IDs instead of logical plan references. An ID's
/// corresponding logical plan reference can be found by consulting the JoinAdjList that was used to produce the
/// given JoinOrderTree.
///
/// TODO(desmond): In the future these trees should keep track of current cost estimates.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub(super) enum JoinOrderTree {
    Relation(usize, usize), // (ID, cardinality).
    Join(
        Box<JoinOrderTree>,
        Box<JoinOrderTree>,
        Vec<JoinCondition>,
        usize,
    ), // (subtree, subtree, join conditions, cardinality).
}

impl JoinOrderTree {
    pub(super) fn join(self, right: Self, conds: Vec<JoinCondition>, card: usize) -> Self {
        Self::Join(Box::new(self), Box::new(right), conds, card)
    }

    // Helper function that checks if the join order tree contains a given id.
    #[cfg(test)]
    pub(super) fn contains(&self, target_id: usize) -> bool {
        match self {
            Self::Relation(id, ..) => *id == target_id,
            Self::Join(left, right, ..) => left.contains(target_id) || right.contains(target_id),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Relation(id, ..) => Box::new(std::iter::once(*id)),
            Self::Join(left, right, ..) => Box::new(left.iter().chain(right.iter())),
        }
    }

    pub(super) fn get_cardinality(&self) -> usize {
        match self {
            Self::Relation(_, cardinality) => *cardinality,
            Self::Join(_, _, _, cardinality) => *cardinality,
        }
    }

    #[cfg(test)]
    // Check if the join structure is the same, regardless of cardinality or join conditions.
    pub(super) fn order_eq(this: &Self, other: &Self) -> bool {
        match (this, other) {
            (JoinOrderTree::Relation(id1, _), JoinOrderTree::Relation(id2, _)) => id1 == id2,
            (
                JoinOrderTree::Join(left1, right1, _, _),
                JoinOrderTree::Join(left2, right2, _, _),
            ) => Self::order_eq(left1, left2) && Self::order_eq(right1, right2),
            _ => false,
        }
    }

    #[cfg(test)]
    pub(super) fn num_join_conditions(this: &Self) -> usize {
        match this {
            JoinOrderTree::Relation(_, _) => 0,
            JoinOrderTree::Join(left, right, conditions, _) => {
                Self::num_join_conditions(left)
                    + Self::num_join_conditions(right)
                    + conditions.len()
            }
        }
    }
}

pub(super) trait JoinOrderer {
    fn order(&self, graph: &JoinGraph) -> JoinOrderTree;
}

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
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

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub(super) struct JoinCondition {
    pub left_on: String,
    pub right_on: String,
}

pub(super) struct JoinAdjList {
    pub max_id: usize,
    plan_to_id: HashMap<*const LogicalPlan, usize>,
    pub id_to_plan: HashMap<usize, LogicalPlanRef>,
    pub edges: HashMap<usize, HashMap<usize, Vec<JoinCondition>>>,
    // The maximum equivalence set id. Each equivalence set is a set of columns that are joined together.
    // For example, if we have a join between A.x and B.x, and B.x and C.x, then the equivalence set is
    // {A.x, B.x, C.x}. Extending this example, if we have a join between A.y and D.y, then the equivalence sets
    // are {A.x, B.x, C.x} and {A.y, D.y}.
    max_equivalence_set_id: usize,
    // Maps (plan id, column name) -> equivalence set id.
    pub equivalence_set_map: HashMap<(usize, String), usize>,
    // Vec of total domains for equivalence sets. Where total_domains[equivalence set id] -> total domain of the equivalence set.
    // The total domain is the number of distinct values in the columns that are part of the equivalence set. For pk-fk joins,
    // this would be the number of primary keys. In the absence of ndv statistics, we take the smallest table in the equivalence set,
    // assume it's the primary key table, and use its cardinality as the total domain.
    total_domains: Vec<usize>,
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
            max_equivalence_set_id: 0,
            equivalence_set_map: HashMap::new(),
            total_domains: vec![],
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
        let left_on = join_condition.left_on.clone();
        let right_on = join_condition.right_on.clone();
        if let Some(neighbors) = self.edges.get_mut(&left_id) {
            if let Some(join_conditions) = neighbors.get_mut(&right_id) {
                // Check if the condition already exists.
                if join_conditions
                    .iter()
                    .any(|cond| cond.right_on == join_condition.right_on && cond.left_on == left_on)
                {
                    return;
                }
                // Add a new condition to the existing edge.
                join_conditions.push(join_condition);
            } else {
                // Add a new edge.
                neighbors.insert(right_id, vec![join_condition]);
            }
        } else {
            // Add a new node then add a new edge.
            let mut neighbors = HashMap::new();
            neighbors.insert(right_id, vec![join_condition]);
            self.edges.insert(left_id, neighbors);
        }
        // Infer transitive edges.
        // E.g. if we have (A.x join B.x) and (B.x join C.x), then we infer (A.x join C.x).
        let mut new_edges = vec![];
        if let Some(right_neighbours) = self.edges.get(&right_id) {
            for (&right_neighbour_id, right_neighbour_join_conds) in right_neighbours {
                if right_neighbour_id == left_id {
                    continue;
                }
                for right_neighbour_join_cond in right_neighbour_join_conds {
                    if right_neighbour_join_cond.left_on == right_on {
                        let plan1 = self.id_to_plan.get(&left_id).unwrap();
                        let plan2 = self.id_to_plan.get(&right_neighbour_id).unwrap();
                        let node1 = JoinNode::new(left_on.clone(), plan1.clone());
                        let node2 = JoinNode::new(
                            right_neighbour_join_cond.right_on.clone(),
                            plan2.clone(),
                        );
                        new_edges.push((node1, node2));
                    }
                }
            }
        }
        for (node1, node2) in new_edges {
            self.add_bidirectional_edge(node1, node2);
        }
    }

    // Helper function that estimates the total domain for a join between two relations.
    fn get_estimated_total_domain(
        &self,
        left_plan: &LogicalPlanRef,
        right_plan: &LogicalPlanRef,
    ) -> usize {
        let left_stats = left_plan.materialized_stats();
        let right_stats = right_plan.materialized_stats();
        // We multiple the number of rows by the reciprocal of the selectivity to get the original total domain.
        let left_rows = left_stats.approx_stats.num_rows as f64
            / left_stats.approx_stats.acc_selectivity.max(0.01);
        let right_rows = right_stats.approx_stats.num_rows as f64
            / right_stats.approx_stats.acc_selectivity.max(0.01);
        left_rows.min(right_rows).max(1.0) as usize
    }

    pub(super) fn add_bidirectional_edge(&mut self, node1: JoinNode, node2: JoinNode) {
        let node1_id = self.get_or_create_plan_id(&node1.plan);
        let node2_id = self.get_or_create_plan_id(&node2.plan);
        // Find the minimal total domain for the join columns, either from the current nodes or from the existing total domains.
        let mut td = self.get_estimated_total_domain(&node1.plan, &node2.plan);
        if let Some(equivalence_set_id) = self
            .equivalence_set_map
            .get(&(node1_id, node1.relation_name.clone()))
        {
            td = td.min(self.total_domains[*equivalence_set_id]);
        }
        if let Some(equivalence_set_id) = self
            .equivalence_set_map
            .get(&(node2_id, node2.relation_name.clone()))
        {
            td = td.min(self.total_domains[*equivalence_set_id]);
        }

        self.add_bidirectional_edge_with_total_domain(node1, node2, td);
    }

    pub(super) fn add_bidirectional_edge_with_total_domain(
        &mut self,
        node1: JoinNode,
        node2: JoinNode,
        td: usize,
    ) {
        // Update the total domains for the join columns.
        let node1_id = self.get_or_create_plan_id(&node1.plan);
        let node2_id = self.get_or_create_plan_id(&node2.plan);
        let node1_equivalence_set_id = self
            .equivalence_set_map
            .get(&(node1_id, node1.relation_name.clone()));
        let node2_equivalence_set_id = self
            .equivalence_set_map
            .get(&(node2_id, node2.relation_name.clone()));
        match (node1_equivalence_set_id, node2_equivalence_set_id) {
            (Some(&node1_equivalence_set_id), Some(&node2_equivalence_set_id)) => {
                if node1_equivalence_set_id != node2_equivalence_set_id {
                    // If we had previously seenA.x = B.x, and C.x = D.x, then we would have incorrectly placed them in difference equivalence
                    // sets. We need to merge the equivalence sets.
                    let merged_equivalence_set_id =
                        node1_equivalence_set_id.min(node2_equivalence_set_id);
                    for value in self.equivalence_set_map.values_mut() {
                        if *value == node1_equivalence_set_id || *value == node2_equivalence_set_id
                        {
                            *value = merged_equivalence_set_id;
                        }
                    }
                    self.total_domains[merged_equivalence_set_id] = td;
                } else {
                    self.total_domains[node1_equivalence_set_id] = td;
                }
            }
            (Some(&node1_equivalence_set_id), None) => {
                self.total_domains[node1_equivalence_set_id] = td;
                self.equivalence_set_map.insert(
                    (node2_id, node2.relation_name.clone()),
                    node1_equivalence_set_id,
                );
            }
            (None, Some(&node2_equivalence_set_id)) => {
                self.total_domains[node2_equivalence_set_id] = td;
                self.equivalence_set_map.insert(
                    (node1_id, node1.relation_name.clone()),
                    node2_equivalence_set_id,
                );
            }
            _ => {
                self.total_domains.push(td);
                self.equivalence_set_map.insert(
                    (node1_id, node1.relation_name.clone()),
                    self.max_equivalence_set_id,
                );
                self.equivalence_set_map.insert(
                    (node2_id, node2.relation_name.clone()),
                    self.max_equivalence_set_id,
                );
                self.max_equivalence_set_id += 1;
            }
        }
        // Add the unidirectional edges.
        self.add_unidirectional_edge(&node1, &node2);
        self.add_unidirectional_edge(&node2, &node1);
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

    // Returns the join conditions that connect the left and right trees, and the maximum total domain for the join columns.
    pub(super) fn get_connections(
        &self,
        left: &JoinOrderTree,
        right: &JoinOrderTree,
    ) -> (Vec<JoinCondition>, usize) {
        // Grab the minimum spanning tree of join conditions that connect the left and right trees, i.e. we take at most one join condition
        // from each equivalence set of join conditions.
        let mut conds = vec![];
        let mut added_equivalence_set_id_for_td = HashSet::new();
        let mut added_equivalence_set_id_for_conds = HashSet::new();
        let mut double_counted_equivalence_set_ids = HashSet::new();
        let mut td = 1;
        for left_node in left.iter() {
            if let Some(neighbors) = self.edges.get(&left_node) {
                for right_node in right.iter() {
                    if let Some(edges) = neighbors.get(&right_node) {
                        // When there is only one join condition, we multiply the total domain by the domain of the equivalence set.
                        // However, when there's more than one join condition between two nodes, then we know that this is not a pk-fk join
                        // on the join keys. Rather, it's a pk-fk join on the tuple of join keys. So we estimate its total domain as the
                        // cardinality of the smaller table. In this case as well, we should avoid multiplying the total domain by the
                        // domains of the equivalence sets. So we use `double_counted_equivalence_set_ids` to keep track of the
                        // equivalence sets that we should not multiply the total domain by.
                        //
                        // For a more concrete example, consider the following join:
                        //
                        // part.x = partsupp.x
                        //
                        // Assuming |part| < |partsupp|, then the total domain of the join is |part|.
                        //
                        // Now consider the following joins:
                        //
                        // part.x = partsupp.x
                        // supp.y = partsupp.y
                        // lineitem.x = partsupp.x
                        // lineitem.y = partsupp.y
                        //
                        // Note that there are implicit join edges part.x = lineitem.x and supp.y = lineitem.y that we infer.
                        //
                        // Assume |supp| < |part| < |partsupp| < |lineitem|.
                        //
                        // When joining part and partsupp, we know that the join is a pk-fk join on part.x,
                        // so the selectivity of the join is 1/|part|.
                        //
                        // When joining partsupp and lineitem, we know that the join is a pk-fk join on (partsupp.x, partsupp.y).
                        // We cannot use the total domains of |supp| or |part| to determine the total domain of (partsupp.x, partsupp.y)
                        // in partsupp. Instead, we estimate the total domain of |(partsupp.x, partsupp.y)| in partsupp as |partsupp|.
                        // So the selectivity of the join is 1/|partsupp|.
                        //
                        // The same is true when we join (partsupp x part) and lineitem: the total domain of the join is still |partsupp|.
                        if edges.len() == 1 {
                            let edge = edges[0].clone();
                            let equivalence_set_id = self
                                .equivalence_set_map
                                .get(&(left_node, edge.left_on.clone()))
                                .expect("Left join condition should be part of an equivalence set");
                            if added_equivalence_set_id_for_td.insert(*equivalence_set_id) {
                                td *= self.total_domains[*equivalence_set_id];
                            }
                            if added_equivalence_set_id_for_conds.insert(*equivalence_set_id) {
                                conds.push(edge.clone());
                            }
                        }
                        if edges.len() > 1 {
                            let node1_plan = self
                                .id_to_plan
                                .get(&left_node)
                                .expect("left id not found in adj list");
                            let node2_plan = self
                                .id_to_plan
                                .get(&right_node)
                                .expect("right id not found in adj list");
                            td *= self.get_estimated_total_domain(node1_plan, node2_plan);
                            for edge in edges {
                                let equivalence_set_id = self
                                    .equivalence_set_map
                                    .get(&(left_node, edge.left_on.clone()))
                                    .expect(
                                        "Left join condition should be part of an equivalence set",
                                    );
                                if added_equivalence_set_id_for_conds.insert(*equivalence_set_id) {
                                    conds.push(edge.clone());
                                }
                                double_counted_equivalence_set_ids.insert(*equivalence_set_id);
                            }
                        }
                    }
                }
            }
        }
        for equivalence_set_id in double_counted_equivalence_set_ids {
            if added_equivalence_set_id_for_td.contains(&equivalence_set_id) {
                td /= self.total_domains[equivalence_set_id].max(1);
            }
        }
        td = td.max(1);
        (conds, td)
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
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

    fn apply_projections_and_filters_to_plan(
        &mut self,
        mut plan: LogicalPlanRef,
    ) -> DaftResult<LogicalPlanRef> {
        // Apply projections and filters in post-traversal order.
        while let Some(projection_or_filter) = self.final_projections_and_filters.pop() {
            let is_last = self.final_projections_and_filters.is_empty();

            match projection_or_filter {
                ProjectionOrFilter::Projection(projections) => {
                    let projections = if is_last {
                        // The final projection is the output projection, so here we select the final projection.
                        projections
                    } else {
                        // Intermediate projections might only transform a subset of columns, so we keep all original columns.
                        plan.schema()
                            .names()
                            .into_iter()
                            .map(resolved_col)
                            .chain(projections)
                            .collect()
                    };

                    plan = Project::try_new(plan, projections)?.into();
                }
                ProjectionOrFilter::Filter(predicate) => {
                    plan = Filter::try_new(plan, predicate)?.into();
                }
            }
        }
        Ok(plan)
    }

    /// Converts a `JoinOrderTree` into a tree of inner joins.
    /// Returns a tuple of the logical plan builder consisting of joins, and a bitmask indicating the plan IDs
    /// that are contained within the current logical plan builder. The bitmask is used for determining join
    /// conditions to use when logical plan builders are joined together.
    pub(crate) fn build_joins_from_join_order(
        &self,
        join_order: &JoinOrderTree,
    ) -> DaftResult<LogicalPlanRef> {
        match join_order {
            JoinOrderTree::Relation(id, ..) => {
                let relation = self
                    .adj_list
                    .id_to_plan
                    .get(id)
                    .expect("Join order contains non-existent plan id 1");
                Ok(relation.clone())
            }
            JoinOrderTree::Join(left_tree, right_tree, conds, _) => {
                let left_plan = self.build_joins_from_join_order(left_tree)?;
                let right_plan = self.build_joins_from_join_order(right_tree)?;

                let on =
                    combine_conjunction(conds.iter().map(|JoinCondition { left_on, right_on }| {
                        let left_field = left_plan
                            .schema()
                            .get_field(left_on)
                            .expect("left_on to exist in left_plan schema")
                            .clone();
                        let right_field = right_plan
                            .schema()
                            .get_field(right_on)
                            .expect("right_on to exist in right_plan schema")
                            .clone();

                        left_col(left_field).eq(right_col(right_field))
                    }));

                let join_plan = Join::try_new(
                    left_plan,
                    right_plan,
                    JoinPredicate::try_new(on)?,
                    JoinType::Inner,
                    None,
                )?;

                Ok(join_plan.into())
            }
        }
    }

    /// Takes a `JoinOrderTree` and creates a logical plan from the current join graph.
    /// Takes in `&mut self` because it drains the projections and filters to apply from the current join graph.
    pub(super) fn build_logical_plan(
        &mut self,
        join_order: JoinOrderTree,
    ) -> DaftResult<LogicalPlanRef> {
        let mut plan = self.build_joins_from_join_order(&join_order)?;
        plan = self.apply_projections_and_filters_to_plan(plan)?;
        Ok(plan)
    }

    pub(super) fn could_reorder(&self) -> bool {
        // For this join graph to reorder joins, there must be at least 3 relations to join. Otherwise
        // there is only one join to perform and no reordering is needed.
        // TODO: We should raise the limit once we implement a DP-based join ordering algorithm.
        self.adj_list.max_id >= 3 && self.adj_list.max_id <= 7
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
    cfg: Arc<DaftExecutionConfig>,
}

impl JoinGraphBuilder {
    pub(super) fn build(mut self) -> JoinGraph {
        self.process_node(&self.plan.clone());
        JoinGraph::new(self.adj_list, self.final_projections_and_filters)
    }

    pub(super) fn from_logical_plan(plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        let output_schema = plan.schema();
        // During join reordering, we might produce an output schema that differs from the initial output schema. For example,
        // columns might be rearranged, or columns that were not originally selected might now be in the output schema.
        // Hence, we take the original output schema and turn it into a projection that we should apply after all other join
        // ordering projections have been applied.
        let output_projection = output_schema.field_names().map(resolved_col).collect();
        Self {
            plan,
            join_conds_to_resolve: vec![],
            final_name_map: HashMap::new(),
            adj_list: JoinAdjList::empty(),
            final_projections_and_filters: vec![ProjectionOrFilter::Projection(output_projection)],
            cfg,
        }
    }

    /// `process_node` goes down the query tree finding reorderable nodes (e.g. inner joins, filters, certain projects etc)
    /// and extracting join edges. It stops recursing down each branch of the query tree once it hits an unreorderable node
    /// (e.g. an aggregation, an outer join, a source node etc).
    /// It keeps track of the following state:
    /// - join_conds_to_resolve: Join conditions (left_on/right_on) from downstream joins that need to be resolved by
    ///   linking to some upstream relation.
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
                    // TODO(desmond): Currently we only support reordering through Project nodes that only project columns. Ideally we should
                    // perform a projection pushup at the start of join reordering in order to separate out this logic from join graph construction,
                    // and so that we can reorder joins that have more complex projections in between them.
                    let reorderable_project = projection
                        .iter()
                        .all(|e| matches!(e.as_ref(), Expr::Column(_)));
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
                // We only process joins with predicates that are all columns.
                // TODO: Figure out how to handle joins with non-column predicates, such as aliases.
                LogicalPlan::Join(Join {
                    on,
                    join_type: JoinType::Inner,
                    ..
                }) => {
                    let (remaining_on, left_on, right_on, _) = on.split_eq_preds();

                    if left_on.is_empty()
                        || !remaining_on.is_empty()
                        || !left_on
                            .iter()
                            .chain(right_on.iter())
                            .all(|c| matches!(c.as_ref(), Expr::Column(_)))
                    {
                        // Encountered a non-reorderable join. Add the root plan at the top of the current linear chain as a relation to join.
                        self.add_relation(root_plan);
                    } else {
                        self.process_linear_chain(root_plan, plan);
                    }

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
                                .insert(input.clone(), resolved_col(output.clone()));
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
                _ => unreachable!(
                    "process_linear_chain is only called with a linear chain of Project and Filters that end with a Join"
                ),
            }
        }
        match &**cur_node {
            // The cur_node is now at the ending_node which MUST be a join node.
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type: JoinType::Inner,
                ..
            }) => {
                let (remaining_on, left_on, right_on, _) = on.split_eq_preds();
                if left_on.is_empty() || !remaining_on.is_empty() {
                    unreachable!(
                        "JoinGraphBuilder::process_linear_chain should not be called with a join that is not orderable"
                    )
                }

                for l in &left_on {
                    let name = l.name();
                    let final_name = if let Some(final_name) = self.final_name_map.get(name) {
                        final_name.name()
                    } else {
                        self.final_name_map
                            .insert(name.to_string(), resolved_col(name));
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
                for r in &right_on {
                    let name = r.name();
                    let final_name = if let Some(final_name) = self.final_name_map.get(name) {
                        final_name.name()
                    } else {
                        self.final_name_map
                            .insert(name.to_string(), resolved_col(name));
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
                panic!("Expected an inner join node")
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
                    projections.push(resolved_col(input.clone()).alias(final_name));
                    needs_projection = true;
                } else {
                    projections.push(resolved_col(input.clone()));
                }
            }
        }
        // Apply projections and return the new plan as the relation for the appropriate join conditions.
        let projected_plan = if needs_projection {
            // Add the non-join-key columns to the projection.
            for name in &schema.names() {
                if !seen_names.contains(name) {
                    projections.push(resolved_col(name.clone()));
                }
            }
            let projected_plan = LogicalPlanBuilder::from(plan.clone())
                .select(projections)
                .expect("Computed projections could not be applied to relation")
                .build();
            Arc::new(Arc::unwrap_or_clone(projected_plan).with_materialized_stats(&self.cfg))
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

    use common_daft_config::DaftExecutionConfig;
    use common_scan_info::Pushdowns;
    use common_treenode::TransformedResult;
    use daft_core::prelude::*;
    use daft_dsl::{AggExpr, Expr, resolved_col, unresolved_col};
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
        .select(vec![unresolved_col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator_with_size(vec![Field::new("d", DataType::Int64)], Some(100)),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(scan_b, unresolved_col("a").eq(unresolved_col("b")))
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(scan_d, unresolved_col("c").eq(unresolved_col("d")))
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(join_plan_r, unresolved_col("a").eq(unresolved_col("d")))
            .unwrap();
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        // Plus 3 inferred edges:
        // - a <-> c
        // - b <-> c
        // - b <-> d
        assert!(join_graph.num_edges() == 6);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(a) <-> Source(d)",
            "Source(a) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Source(d)",  // Inferred edge.
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
        .select(vec![unresolved_col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(scan_b, unresolved_col("a").eq(unresolved_col("b")))
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(scan_d, unresolved_col("c").eq(unresolved_col("d")))
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(join_plan_r, unresolved_col("b").eq(unresolved_col("d")))
            .unwrap();
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph =
            JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - b <-> d
        // Plus 3 inferred edges:
        // - a <-> c
        // - b <-> c
        // - b <-> d
        assert!(join_graph.num_edges() == 6);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(b) <-> Source(d)",
            "Source(a) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Source(d)",  // Inferred edge.
        ]));
    }

    #[test]
    #[ignore = "Temporarily skipped - Join reordering algorithm needs to be updated to do a projection pushup"]
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
        .select(vec![unresolved_col("a").alias("a_alpha")])
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
            .inner_join(scan_b, unresolved_col("a_alpha").eq(unresolved_col("b")))
            .unwrap()
            .select(vec![
                unresolved_col("a_alpha").alias("a_beta"),
                unresolved_col("b"),
            ])
            .unwrap();
        let join_plan_2 = join_plan_1
            .inner_join(scan_c, unresolved_col("a_beta").eq(unresolved_col("c")))
            .unwrap();
        let original_plan = join_plan_2.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph =
            JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a_beta <-> b
        // - a_beta <-> c
        // Plus 1 inferred edge:
        // - b <-> c
        assert!(join_graph.num_edges() == 3);
        assert!(join_graph.contains_edges(vec![
            "Project(a_beta) <-> Source(b)",
            "Project(a_beta) <-> Source(c)",
            "Source(b) <-> Source(c)", // Inferred edge.
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
        let double_proj = unresolved_col("c_prime")
            .add(unresolved_col("c_prime"))
            .alias("double");
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![
            unresolved_col("c_prime").alias("c"),
            double_proj.clone(),
        ])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(scan_b, unresolved_col("a").eq(unresolved_col("b")))
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(scan_d, unresolved_col("c").eq(unresolved_col("d")))
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(join_plan_r, unresolved_col("a").eq(unresolved_col("d")))
            .unwrap();
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph =
            JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg.clone()).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        // Plus 3 inferred edges:
        // - a <-> c
        // - b <-> c
        // - b <-> d
        assert!(join_graph.num_edges() == 6);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Source(a) <-> Source(d)",
            "Source(a) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Source(d)",  // Inferred edge.
        ]));
    }

    #[test]
    #[ignore = "Temporarily skipped - Join reordering algorithm needs to be updated to do a projection pushup"]
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
        let double_proj = unresolved_col("c_prime")
            .add(unresolved_col("c_prime"))
            .alias("double");
        let filter_c_prime =
            unresolved_col("c_prime").gt(Arc::new(Expr::Literal(Literal::Int64(0))));
        let filter_c = unresolved_col("c").lt(Arc::new(Expr::Literal(Literal::Int64(5))));
        let scan_c = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("c_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .filter(filter_c_prime.clone())
        .unwrap()
        .select(vec![
            unresolved_col("c_prime").alias("c"),
            double_proj.clone(),
        ])
        .unwrap()
        .filter(filter_c.clone())
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(scan_b, unresolved_col("a").eq(unresolved_col("b")))
            .unwrap();
        let quad_proj = unresolved_col("double")
            .add(unresolved_col("double"))
            .alias("quad");
        let join_plan_r = scan_c
            .inner_join(scan_d, unresolved_col("c").eq(unresolved_col("d")))
            .unwrap()
            .select(vec![unresolved_col("d"), quad_proj.clone()])
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(join_plan_r, unresolved_col("a").eq(unresolved_col("d")))
            .unwrap();
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        // Plus 3 inferred edges:
        // - a <-> c
        // - b <-> c
        // - b <-> d
        assert!(join_graph.num_edges() == 6);
        assert!(join_graph.contains_edges(vec![
            "Source(a) <-> Source(b)",
            "Filter(c) <-> Source(d)",
            "Source(a) <-> Source(d)",
            "Source(a) <-> Filter(c)", // Inferred edge.
            "Source(b) <-> Filter(c)", // Inferred edge.
            "Source(b) <-> Source(d)", // Inferred edge.
        ]));
        // Check for non-join projections and filters at the end.
        // The join graph should only keep track of projections and filters that sit between joins.
        assert!(join_graph.contains_projections_and_filters(vec![
                &resolved_col("double")
                    .add(resolved_col("double"))
                    .alias("quad"),
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
        let a_proj = unresolved_col("a_prime").alias("a");
        let scan_a = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a_prime", DataType::Int64)]),
            Pushdowns::default(),
        )
        .select(vec![a_proj.clone()])
        .unwrap()
        .aggregate(
            vec![Arc::new(Expr::Agg(AggExpr::Count(
                unresolved_col("a"),
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
        .select(vec![unresolved_col("c_prime").alias("c")])
        .unwrap();
        let scan_d = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("d", DataType::Int64)]),
            Pushdowns::default(),
        );
        let join_plan_l = scan_a
            .inner_join(scan_b, unresolved_col("a").eq(unresolved_col("b")))
            .unwrap();
        let join_plan_r = scan_c
            .inner_join(scan_d, unresolved_col("c").eq(unresolved_col("d")))
            .unwrap();
        let join_plan = join_plan_l
            .inner_join(join_plan_r, unresolved_col("a").eq(unresolved_col("d")))
            .unwrap();
        let original_plan = join_plan.build();
        let scan_materializer = MaterializeScans::new();
        let original_plan = scan_materializer
            .try_optimize(original_plan)
            .data()
            .unwrap();
        let cfg = Arc::new(DaftExecutionConfig::default());
        let stats_enricher = EnrichWithStats::new(Some(cfg.clone()));
        let original_plan = stats_enricher.try_optimize(original_plan).data().unwrap();
        let join_graph = JoinGraphBuilder::from_logical_plan(original_plan.clone(), cfg).build();
        assert!(join_graph.fully_connected());
        // There should be edges between:
        // - a <-> b
        // - c <-> d
        // - a <-> d
        // Plus 3 inferred edges:
        // - a <-> c
        // - b <-> c
        // - b <-> d
        assert!(join_graph.num_edges() == 6);
        assert!(join_graph.contains_edges(vec![
            "Aggregate(a) <-> Source(b)",
            "Project(c) <-> Source(d)",
            "Aggregate(a) <-> Source(d)",
            "Aggregate(a) <-> Project(c)", // Inferred edge.
            "Source(b) <-> Project(c)",    // Inferred edge.
            "Source(b) <-> Source(d)",     // Inferred edge.
        ]));
        // Projections below the aggregation should not be part of the final projections.
        assert!(
            !join_graph.contains_projections_and_filters(vec![&resolved_col("a_prime").alias("a")])
        );
    }
}
