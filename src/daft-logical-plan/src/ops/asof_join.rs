use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::join::AsofJoinStrategy;
use daft_dsl::{Column, Expr, ExprRef, UnresolvedColumn, resolved_col, unresolved_col};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan, LogicalPlanRef,
    logical_plan::{self},
    ops::Project,
    stats::{ApproxStats, PlanStats, StatsState},
};

/// Infer the output schema for an asof join.
/// Column order: all left columns in their original schema order, then
/// right columns not in `right_cols_to_drop` in their original schema order.
fn infer_asof_join_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    right_cols_to_drop: &HashSet<String>,
) -> DaftResult<SchemaRef> {
    let fields: Vec<_> = left_schema
        .into_iter()
        .chain(
            right_schema
                .into_iter()
                .filter(|f| !right_cols_to_drop.contains(f.name.as_ref())),
        )
        .cloned()
        .collect();
    Ok(daft_schema::schema::Schema::new(fields).into())
}

/// Compute the right key column names to exclude from the asof join output.
///
/// For by-keys: all right by columns that are direct column references (not complex expressions) are dropped.
/// For on-keys: the right on column is dropped only when it has the same name as the left on column.
///
/// Returns the set of right column names to drop from the output.
pub(crate) fn get_right_cols_to_drop<E, F>(
    right_by: &[E],
    left_on: &E,
    right_on: &E,
    extract_name: F,
) -> HashSet<String>
where
    F: Fn(&E) -> Option<String>,
{
    let mut right_cols_to_drop = HashSet::new();

    for r in right_by {
        if let Some(right_name) = extract_name(r) {
            right_cols_to_drop.insert(right_name);
        }
    }

    if let (Some(left_name), Some(right_name)) = (extract_name(left_on), extract_name(right_on))
        && left_name == right_name
    {
        right_cols_to_drop.insert(right_name);
    }

    right_cols_to_drop
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct AsofJoin {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub left_by: Vec<ExprRef>,
    pub right_by: Vec<ExprRef>,
    pub left_on: ExprRef,
    pub right_on: ExprRef,
    pub strategy: AsofJoinStrategy,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
    pub assume_sorted_and_aligned: bool,
}

impl AsofJoin {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        left: LogicalPlanRef,
        right: LogicalPlanRef,
        left_by: Vec<ExprRef>,
        right_by: Vec<ExprRef>,
        left_on: ExprRef,
        right_on: ExprRef,
        right_cols_to_drop: HashSet<String>,
        strategy: AsofJoinStrategy,
        assume_sorted_and_aligned: bool,
    ) -> logical_plan::Result<Self> {
        let output_schema =
            infer_asof_join_schema(&left.schema(), &right.schema(), &right_cols_to_drop)?;

        Ok(Self {
            plan_id: None,
            node_id: None,
            left,
            right,
            left_by,
            right_by,
            left_on,
            right_on,
            strategy,
            output_schema,
            stats_state: StatsState::NotMaterialized,
            assume_sorted_and_aligned,
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

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // An asof join is a left join: every left row produces exactly one output row
        // (matched or null-filled). The output cardinality is therefore always equal to
        // the left side's row count, regardless of the right table size.
        let left_stats = self.left.materialized_stats();
        let right_stats = self.right.materialized_stats();
        let size_bytes = left_stats.approx_stats.size_bytes
            + (right_stats.approx_stats.size_bytes as f64 * left_stats.approx_stats.acc_selectivity)
                as usize;
        let approx_stats = ApproxStats {
            num_rows: left_stats.approx_stats.num_rows,
            size_bytes,
            acc_selectivity: left_stats.approx_stats.acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    /// Rename right-side columns that clash with left-side columns, excluding right key columns
    /// (which are dropped from the output and don't need renaming). Also rewrites any column
    /// references in `right_by` and `right_on` that point to renamed columns.
    ///
    /// Returns:
    /// - updated right plan (with a Project applied if any renames were needed)
    /// - updated right_by
    /// - updated right_on
    pub(crate) fn deduplicate_asof_join_columns(
        left: LogicalPlanRef,
        right: LogicalPlanRef,
        right_by: Vec<ExprRef>,
        right_on: ExprRef,
        right_cols_to_drop: &HashSet<String>,
        options: &super::join::JoinOptions,
    ) -> DaftResult<(LogicalPlanRef, Vec<ExprRef>, ExprRef)> {
        let left_names: HashSet<String> = HashSet::from_iter(left.schema().names());
        let right_names: Vec<String> = right.schema().names();

        let clashing: Vec<String> = right_names
            .iter()
            .filter(|n| !right_cols_to_drop.contains(n.as_str()) && left_names.contains(n.as_str()))
            .cloned()
            .collect();

        if clashing.is_empty() {
            return Ok((right, right_by, right_on));
        }

        let mut names_so_far: HashSet<String> =
            HashSet::from_iter(left_names.iter().chain(right_names.iter()).cloned());

        let right_rename_mapping: HashMap<String, String> = clashing
            .into_iter()
            .map(|name| {
                let mut new_name = name.clone();
                while names_so_far.contains(&new_name) {
                    new_name = match (&options.prefix, &options.suffix) {
                        (Some(prefix), Some(suffix)) => format!("{prefix}{new_name}{suffix}"),
                        (Some(prefix), None) => format!("{prefix}{new_name}"),
                        (None, Some(suffix)) => format!("{new_name}{suffix}"),
                        (None, None) => format!("right.{new_name}"),
                    };
                }
                names_so_far.insert(new_name.clone());
                (name, new_name)
            })
            .collect();

        let new_right_projection: Vec<ExprRef> = right_names
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

        let rewrite_expr = |expr: ExprRef| -> ExprRef {
            expr.transform(|e| {
                if let Expr::Column(Column::Unresolved(UnresolvedColumn { name, .. })) = e.as_ref()
                    && let Some(new_name) = right_rename_mapping.get(name.as_ref())
                {
                    return Ok(Transformed::yes(unresolved_col(new_name.as_str())));
                }
                Ok(Transformed::no(e))
            })
            .unwrap()
            .data
        };

        let new_right_by: Vec<ExprRef> = right_by.into_iter().map(&rewrite_expr).collect();
        let new_right_on: ExprRef = rewrite_expr(right_on);

        Ok((new_right.into(), new_right_by, new_right_on))
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("AsofJoin: strategy = {}", self.strategy)];
        res.push(format!(
            "left_by = [{}]",
            self.left_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res.push(format!(
            "right_by = [{}]",
            self.right_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res.push(format!("left_on = {}", self.left_on));
        res.push(format!("right_on = {}", self.right_on));
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
