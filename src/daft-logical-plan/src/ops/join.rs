use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_algebra::boolean::{combine_conjunction, split_conjunction};
use daft_core::{join::JoinSide, prelude::*};
use daft_dsl::{
    join::infer_join_schema, resolved_col, right_col, Column, Expr, ExprRef, Operator,
    ResolvedColumn,
};
use indexmap::IndexSet;
#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::{
    logical_plan::{self},
    ops::Project,
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan, LogicalPlanRef,
};

fn has_sides(expr: &ExprRef) -> (bool, bool) {
    let mut has_left = false;
    let mut has_right = false;

    expr.apply(|e| {
        match e.as_ref() {
            Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(_, JoinSide::Left))) => {
                has_left = true;
            }
            Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(_, JoinSide::Right))) => {
                has_right = true;
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    (has_left, has_right)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

    /// Pop the equality predicates out of the conjunction where one side is all left and the other side is all right columns.
    ///
    /// Returns (left keys, right keys, null equals null)
    pub fn pop_equi_preds(&mut self) -> (Vec<ExprRef>, Vec<ExprRef>, Vec<bool>) {
        let Some(pred) = &self.0 else {
            return (vec![], vec![], vec![]);
        };

        let exprs = split_conjunction(pred);

        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut null_equals_null = Vec::new();

        let remaining = exprs.into_iter().filter(|e| match e.as_ref() {
            Expr::BinaryOp { op, left, right }
                if matches!(op, Operator::Eq | Operator::EqNullSafe) =>
            {
                let (left_has_left, left_has_right) = has_sides(left);
                let (right_has_left, right_has_right) = has_sides(right);

                let (left, right) = match (
                    left_has_left,
                    left_has_right,
                    right_has_left,
                    right_has_right,
                ) {
                    (true, false, false, true) => (left, right),
                    (false, true, true, false) => (right, left),
                    _ => {
                        return true;
                    }
                };

                fn replace_join_side_cols(expr: ExprRef) -> ExprRef {
                    expr.transform(|e| {
                        if let Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                            Field { name, .. },
                            _,
                        ))) = e.as_ref()
                        {
                            Ok(Transformed::yes(resolved_col(name.clone())))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .unwrap()
                    .data
                }

                let left = replace_join_side_cols(left.clone());
                let right = replace_join_side_cols(right.clone());

                left_keys.push(left);
                right_keys.push(right);
                null_equals_null.push(*op == Operator::EqNullSafe);

                false
            }
            _ => true,
        });

        self.0 = combine_conjunction(remaining);

        (left_keys, right_keys, null_equals_null)
    }

    pub fn equi_preds(&self) -> (Vec<ExprRef>, Vec<ExprRef>, Vec<bool>) {
        self.clone().pop_equi_preds()
    }

    /// Pop the predicates in the conjunction that only use columns from one side of the join.
    ///
    /// Returns (left predicate, right predicate)
    pub fn pop_side_only_preds(&mut self) -> (Option<ExprRef>, Option<ExprRef>) {
        let Some(pred) = &self.0 else {
            return (None, None);
        };

        let exprs = split_conjunction(pred);

        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();

        let remaining = exprs.into_iter().filter(|e| {
            let (has_left, has_right) = has_sides(e);

            if !has_right {
                // put expressions that use neither side in left as well
                left_preds.push(e.clone());

                false
            } else if !has_left {
                right_preds.push(e.clone());

                false
            } else {
                true
            }
        });

        self.0 = combine_conjunction(remaining);

        let left_pred = combine_conjunction(left_preds);
        let right_pred = combine_conjunction(right_preds);

        (left_pred, right_pred)
    }

    pub fn side_only_preds(&self) -> (Option<ExprRef>, Option<ExprRef>) {
        self.clone().pop_side_only_preds()
    }
}

impl TryFrom<ExprRef> for JoinPredicate {
    type Error = DaftError;

    fn try_from(value: ExprRef) -> DaftResult<Self> {
        Self::try_new(Some(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    pub plan_id: Option<usize>,
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
                return Err(DaftError::SchemaMismatch(format!("Expected merged columns in join to exist in both sides of the join, found: {missing_names:?}")));
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
                            && let Some(new_name) = right_rename_mapping.get(&name.to_string())
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
