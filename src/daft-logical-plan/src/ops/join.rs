use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use daft_dsl::{
    col, join::infer_join_schema, optimization::replace_columns_with_expressions, Expr, ExprRef,
};
use indexmap::IndexSet;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    ops::Project,
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan, LogicalPlanRef,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub null_equals_nulls: Option<Vec<bool>>,
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
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> logical_plan::Result<Self> {
        if left_on.len() != right_on.len() {
            return Err(DaftError::ValueError(format!(
                "Expected length of left_on to match length of right_on for Join, received: {} vs {}",
                left_on.len(),
                right_on.len()
            )))
            .context(CreationSnafu);
        }

        for (l, r) in left_on.iter().zip(right_on.iter()) {
            let l_dtype = l.to_field(&left.schema())?.dtype;
            let r_dtype = r.to_field(&right.schema())?.dtype;

            try_get_supertype(&l_dtype, &r_dtype).map_err(|_| {
                DaftError::TypeError(
                    format!("Expected dtypes of left_on and right_on for Join to have a valid supertype, received: {l_dtype} vs {r_dtype}")
                )
            })?;
        }

        if let Some(null_equals_null) = &null_equals_nulls {
            if null_equals_null.len() != left_on.len() {
                return Err(DaftError::ValueError(format!(
                    "Expected null_equals_nulls to have the same length as left_on or right_on, received: {} vs {}",
                    null_equals_null.len(),
                    left_on.len()
                )))
                .context(CreationSnafu);
            }
        }

        let output_schema = infer_join_schema(&left.schema(), &right.schema(), join_type)?;

        Ok(Self {
            left,
            right,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            join_strategy,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    /// Add a project under the right side plan when necessary in order to resolve naming conflicts
    /// between left and right side columns.
    ///
    /// Returns:
    /// - left (unchanged)
    /// - updated right
    /// - left_on (unchanged)
    /// - updated right_on
    pub(crate) fn deduplicate_join_columns(
        left: LogicalPlanRef,
        right: LogicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        options: JoinOptions,
    ) -> DaftResult<(LogicalPlanRef, LogicalPlanRef, Vec<ExprRef>, Vec<ExprRef>)> {
        if matches!(join_type, JoinType::Anti | JoinType::Semi) {
            Ok((left, right, left_on, right_on))
        } else {
            let merged_cols = if options.merge_matching_join_keys {
                left_on
                    .iter()
                    .zip(right_on.iter())
                    .filter_map(|(l, r)| match (l.as_ref(), r.as_ref()) {
                        (Expr::Column(l_name), Expr::Column(r_name)) if l_name == r_name => {
                            Some(l_name.to_string())
                        }
                        _ => None,
                    })
                    .collect()
            } else {
                IndexSet::new()
            };

            let left_names = left.schema().names();
            let right_names = right.schema().names();

            let mut names_so_far: HashSet<String> = HashSet::from_iter(left_names);

            // rename right columns that have the same name as left columns and are not join keys
            // old_name -> new_name
            let right_rename_mapping: HashMap<_, _> = right_names
                .iter()
                .filter_map(|name| {
                    if !names_so_far.contains(name) || merged_cols.contains(name.as_str()) {
                        names_so_far.insert(name.clone());
                        None
                    } else {
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

                        Some((name.clone(), new_name))
                    }
                })
                .collect();

            if right_rename_mapping.is_empty() {
                Ok((left, right, left_on, right_on))
            } else {
                // projection to update the right side with the new column names
                let new_right_projection: Vec<_> = right_names
                    .iter()
                    .map(|name| {
                        if let Some(new_name) = right_rename_mapping.get(name) {
                            Expr::Alias(col(name.clone()), new_name.clone().into()).into()
                        } else {
                            col(name.clone())
                        }
                    })
                    .collect();

                let new_right: LogicalPlan = Project::try_new(right, new_right_projection)?.into();

                let right_on_replace_map = right_rename_mapping
                    .iter()
                    .map(|(old_name, new_name)| (old_name.clone(), col(new_name.clone())))
                    .collect::<HashMap<_, _>>();

                // change any column references in the right_on expressions to the new column names
                let new_right_on = right_on
                    .into_iter()
                    .map(|expr| replace_columns_with_expressions(expr, &right_on_replace_map))
                    .collect::<Vec<_>>();

                Ok((left, new_right.into(), left_on, new_right_on))
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
        if !self.left_on.is_empty() && !self.right_on.is_empty() && self.left_on == self.right_on {
            res.push(format!(
                "On = {}",
                self.left_on.iter().map(|e| e.to_string()).join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
        }
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
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
    /// For join predicates in the form col(a) = col(a),
    /// merge column "a" from both sides into one column.
    pub merge_matching_join_keys: bool,
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

    pub fn merge_matching_join_keys(mut self, val: bool) -> Self {
        self.merge_matching_join_keys = val;
        self
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinOptions {
    #[new]
    #[pyo3(signature = (
        prefix,
        suffix,
        merge_matching_join_keys,
    ))]
    pub fn new(
        prefix: Option<String>,
        suffix: Option<String>,
        merge_matching_join_keys: bool,
    ) -> Self {
        Self {
            prefix,
            suffix,
            merge_matching_join_keys,
        }
    }
}
