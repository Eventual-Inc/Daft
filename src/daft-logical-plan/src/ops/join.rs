use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_algebra::boolean::{combine_conjunction, split_conjunction};
use daft_core::{join::JoinSide, prelude::*};
use daft_dsl::{
    Column, Expr, ExprRef, Operator, ResolvedColumn, join::infer_join_schema, resolved_col,
    right_col,
};
use indexmap::IndexSet;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan, LogicalPlanRef,
    logical_plan::{self},
    ops::Project,
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinPredicate(Option<ExprRef>);

impl JoinPredicate {
    pub fn try_new(pred: Option<ExprRef>) -> DaftResult<Self> {
        if let Some(pred) = &pred {
            pred.apply(|e| match e.as_ref() {
                Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(..))) => {
                    Ok(TreeNodeRecursion::Continue)
                }
                Expr::Column(..) => Err(DaftError::ValueError(format!(
                    "Expected join predicates to only contain join side columns, received: {e}"
                ))),
                _ => Ok(TreeNodeRecursion::Continue),
            })?;
        }

        Ok(Self(pred))
    }

    pub fn empty() -> Self {
        Self(None)
    }

    pub fn inner(&self) -> Option<&ExprRef> {
        self.0.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Check if an expression contains columns from one side of the join
    fn uses_join_side(expr: &ExprRef, side: JoinSide) -> bool {
        expr.exists(|e| {
            matches!(
                e.as_ref(),
                Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(_, s))) if *s == side
            )
        })
    }

    /// Replace ResolvedColumn::JoinSide with ResolvedColumn::Basic
    fn replace_join_side_cols(expr: ExprRef) -> ExprRef {
        expr.transform(|e| {
            if let Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(Field { name, .. }, _))) =
                e.as_ref()
            {
                Ok(Transformed::yes(resolved_col(name.clone())))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .unwrap()
        .data
    }

    /// Split out the equality predicates where one side is all left and the other side is all right columns.
    ///
    /// Returns (remaining, left keys, right keys, null equals null)
    pub fn split_eq_preds(&self) -> (Self, Vec<ExprRef>, Vec<ExprRef>, Vec<bool>) {
        let Some(pred) = &self.0 else {
            return (Self::empty(), vec![], vec![], vec![]);
        };

        fn extract_equi_predicate(expr: &Expr) -> Option<(ExprRef, ExprRef, bool)> {
            match expr {
                Expr::BinaryOp { op, left, right }
                    if matches!(op, Operator::Eq | Operator::EqNullSafe) =>
                {
                    let (left_expr, right_expr) = match (
                        JoinPredicate::uses_join_side(left, JoinSide::Left),
                        JoinPredicate::uses_join_side(left, JoinSide::Right),
                        JoinPredicate::uses_join_side(right, JoinSide::Left),
                        JoinPredicate::uses_join_side(right, JoinSide::Right),
                    ) {
                        (true, false, false, true) => (left, right),
                        (false, true, true, false) => (right, left),
                        _ => return None,
                    };
                    let left_cleaned = JoinPredicate::replace_join_side_cols(left_expr.clone());
                    let right_cleaned = JoinPredicate::replace_join_side_cols(right_expr.clone());
                    let is_null_safe = *op == Operator::EqNullSafe;
                    Some((left_cleaned, right_cleaned, is_null_safe))
                }
                _ => None,
            }
        }

        let exprs = split_conjunction(pred);

        let mut remaining_exprs = Vec::new();
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut null_equals_null = Vec::new();

        for e in exprs {
            if let Some((left_expr, right_expr, is_null_safe)) = extract_equi_predicate(&e) {
                left_keys.push(left_expr);
                right_keys.push(right_expr);
                null_equals_null.push(is_null_safe);
            } else {
                remaining_exprs.push(e);
            }
        }

        let remaining = Self(combine_conjunction(remaining_exprs));

        (remaining, left_keys, right_keys, null_equals_null)
    }

    /// Split out the predicates in the conjunction that only use columns from one side of the join.
    ///
    /// Does not include predicates that use neither join side.
    ///
    /// Returns (remaining, side-only predicate)
    pub fn split_side_only_preds(&self, side: JoinSide) -> (Self, Option<ExprRef>) {
        let Some(pred) = &self.0 else {
            return (Self::empty(), None);
        };

        let exprs = split_conjunction(pred);

        let mut remaining_exprs = Vec::new();
        let mut side_only_preds = Vec::new();

        for e in exprs {
            if Self::uses_join_side(&e, side) && !Self::uses_join_side(&e, !side) {
                side_only_preds.push(e);
            } else {
                remaining_exprs.push(e);
            }
        }

        let remaining = Self(combine_conjunction(remaining_exprs));
        let side_only_pred = combine_conjunction(side_only_preds).map(Self::replace_join_side_cols);

        (remaining, side_only_pred)
    }
}

impl TryFrom<ExprRef> for JoinPredicate {
    type Error = DaftError;

    fn try_from(value: ExprRef) -> DaftResult<Self> {
        Self::try_new(Some(value))
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Join {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub on: JoinPredicate,
    pub join_type: JoinType,
    pub join_strategy: Option<JoinStrategy>,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Join {
    /// Create a new join node, checking the validity of the inputs and deriving the output schema.
    ///
    /// Columns that have the same name between left and right are assumed to be merged.
    /// If that is not the desired behavior, call `Join::deduplicate_join_keys` before initializing the join node.
    pub(crate) fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        on: JoinPredicate,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> logical_plan::Result<Self> {
        let output_schema = infer_join_schema(&left.schema(), &right.schema(), join_type)?;

        Ok(Self {
            plan_id: None,
            node_id: None,
            left,
            right,
            on,
            join_type,
            join_strategy,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Add a project under the right side plan when necessary in order to resolve naming conflicts
    /// between left and right side columns.
    ///
    /// Columns in `using` are merged so they are not renamed
    ///
    /// Returns:
    /// - left (unchanged)
    /// - updated right
    /// - updated on
    pub(crate) fn deduplicate_join_columns(
        left: LogicalPlanRef,
        right: LogicalPlanRef,
        on: Option<ExprRef>,
        using: &[String],
        join_type: JoinType,
        options: JoinOptions,
    ) -> DaftResult<(LogicalPlanRef, LogicalPlanRef, Option<ExprRef>)> {
        if matches!(join_type, JoinType::Anti | JoinType::Semi) {
            Ok((left, right, on))
        } else {
            let merged_names: IndexSet<String> = IndexSet::from_iter(using.iter().cloned());
            let left_names: IndexSet<String> = IndexSet::from_iter(left.schema().names());
            let right_names: IndexSet<String> = IndexSet::from_iter(right.schema().names());

            let common_names: IndexSet<String> =
                IndexSet::from_iter(right_names.intersection(&left_names).cloned());
            if !common_names.is_superset(&merged_names) {
                let missing_names = merged_names.difference(&common_names).collect::<Vec<_>>();
                return Err(DaftError::SchemaMismatch(format!(
                    "Expected merged columns in join to exist in both sides of the join, found: {missing_names:?}"
                )));
            }

            let mut names_so_far: HashSet<String> =
                HashSet::from_iter(left_names.union(&right_names).cloned());

            // rename right columns that have the same name as left columns and are not merged
            // old_name -> new_name
            let right_rename_mapping = common_names
                .difference(&merged_names)
                .map(|name| {
                    let mut new_name = name.clone();
                    while names_so_far.contains(&new_name) {
                        new_name = match (&options.prefix, &options.suffix) {
                            (Some(prefix), Some(suffix)) => {
                                format!("{}{}{}", prefix, new_name, suffix)
                            }
                            (Some(prefix), None) => {
                                format!("{}{}", prefix, new_name)
                            }
                            (None, Some(suffix)) => {
                                format!("{}{}", new_name, suffix)
                            }
                            (None, None) => {
                                format!("right.{}", new_name)
                            }
                        };
                    }
                    names_so_far.insert(new_name.clone());

                    (name.clone(), new_name)
                })
                .collect::<HashMap<_, _>>();

            if right_rename_mapping.is_empty() {
                Ok((left, right, on))
            } else {
                // projection to update the right side with the new column names
                let new_right_projection: Vec<_> = right_names
                    .iter()
                    .map(|name| {
                        if let Some(new_name) = right_rename_mapping.get(name) {
                            resolved_col(name.clone()).alias(new_name.clone())
                        } else {
                            resolved_col(name.clone())
                        }
                    })
                    .collect();

                let new_right: LogicalPlan = Project::try_new(right, new_right_projection)?.into();

                // change any column references in the join predicate to the new column names
                let new_on = on.map(|pred| {
                    pred.transform(|e| {
                        if let Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                            Field { name, dtype, .. },
                            JoinSide::Right,
                        ))) = e.as_ref()
                            && let Some(new_name) = right_rename_mapping.get(&name.clone())
                        {
                            Ok(Transformed::yes(right_col(Field::new(
                                new_name,
                                dtype.clone(),
                            ))))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .unwrap()
                    .data
                });

                Ok((left, new_right.into(), new_on))
            }
        }
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // Assume a Primary-key + Foreign-Key join which would yield the max of the two tables.
        // TODO(desmond): We can do better estimations here. For now, use the old logic.
        let left_stats = self.left.materialized_stats();
        let right_stats = self.right.materialized_stats();
        // We assume that if one side of a join had its cardinality reduced by some operations
        // (e.g. filters, limits, aggregations), then assuming a pk-fk join, the total number of
        // rows output from the join will be reduced proportionally. Hence, apply the right side's
        // selectivity to the number of rows/size in bytes on the left and vice versa.
        let left_num_rows =
            left_stats.approx_stats.num_rows as f64 * right_stats.approx_stats.acc_selectivity;
        let right_num_rows =
            right_stats.approx_stats.num_rows as f64 * left_stats.approx_stats.acc_selectivity;
        let left_size =
            left_stats.approx_stats.size_bytes as f64 * right_stats.approx_stats.acc_selectivity;
        let right_size =
            right_stats.approx_stats.size_bytes as f64 * left_stats.approx_stats.acc_selectivity;
        let approx_stats = ApproxStats {
            num_rows: left_num_rows.max(right_num_rows).ceil() as usize,
            size_bytes: left_size.max(right_size).ceil() as usize,
            acc_selectivity: left_stats.approx_stats.acc_selectivity
                * right_stats.approx_stats.acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Join: Type = {}", self.join_type));
        res.push(format!(
            "Strategy = {}",
            self.join_strategy
                .map_or_else(|| "Auto".to_string(), |s| s.to_string())
        ));

        if let Some(on_expr) = self.on.inner() {
            res.push(format!("On = {on_expr}",));
        }

        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}

#[cfg_attr(feature = "python", pyclass)]
#[derive(Clone, Default)]
pub struct JoinOptions {
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

impl JoinOptions {
    pub fn prefix(mut self, val: impl Into<String>) -> Self {
        self.prefix = Some(val.into());
        self
    }

    pub fn suffix(mut self, val: impl Into<String>) -> Self {
        self.suffix = Some(val.into());
        self
    }
}
