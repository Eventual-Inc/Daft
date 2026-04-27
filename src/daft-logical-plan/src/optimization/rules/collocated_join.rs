/// Collocated-join optimizer rule.
///
/// When both sides of an inner join are already-materialized hive-partitioned
/// scans and at least one equality join key is the shared partition key, this
/// rule splits the join into one sub-join per partition value and wraps the
/// results in a `Concat`.
///
/// # Why this helps
///
/// A regular hash join loads the entire *build* side into memory before
/// probing.  With 5 M polygon rows (each carrying a 93-byte WKB blob) the
/// build side alone exceeds several GB.
///
/// When both tables are partitioned by the join key (e.g. `partition_gh`),
/// rows from partition cell `u1` in the left table can only match rows from
/// cell `u1` in the right table.  So we can run 12 small hash joins (one per
/// cell) instead of one giant one, keeping peak memory at ~1/N of the total.
///
/// # Preconditions
///
/// 1. Fires **after** `MaterializeScans` (requires `ScanState::Tasks`).
/// 2. Both sides must have at least one non-empty `partitioning_keys` field
///    in their `PhysicalScanInfo`.  This is set when the scan was created with
///    `hive_partitioning = true` (e.g. `read_parquet(dir, hive_partitioning=True)`).
/// 3. At least one equality join predicate refers to a column that is a
///    partition key on both sides.
/// 4. Currently only rewrites `Inner` joins (safe to extend to `Left`/`Right`
///    with NULL-fill for unmatched partitions in the future).
use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::join::JoinType;
use daft_dsl::{
    Expr,
    expr::{Column, ResolvedColumn},
};
use daft_scan::{PartitionSpec, ScanState, ScanTaskRef};

use super::OptimizerRule;
use crate::{
    LogicalPlan,
    ops::{Concat, Join},
    source_info::SourceInfo,
};

/// Collocated join: split a join on a shared partition key into per-partition sub-joins.
#[derive(Debug, Default)]
pub struct CollocatedJoin;

impl OptimizerRule for CollocatedJoin {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| self.try_optimize_node(node))
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Extract (tasks, partitioning_key_names) from a Source node that has already
/// been materialized.  Returns `None` for any other node type.
fn source_partition_info(
    plan: &LogicalPlan,
) -> Option<(Arc<Vec<ScanTaskRef>>, Vec<String>)> {
    let LogicalPlan::Source(source) = plan else {
        return None;
    };
    let SourceInfo::Physical(psi) = source.source_info.as_ref() else {
        return None;
    };
    let ScanState::Tasks(tasks) = &psi.scan_state else {
        return None;
    };
    if psi.partitioning_keys.is_empty() {
        return None;
    }
    let pkey_names: Vec<String> = psi
        .partitioning_keys
        .iter()
        .map(|pk| pk.field.name.as_ref().to_owned())
        .collect();
    Some((tasks.clone(), pkey_names))
}

/// Extract a bare column name from a cleaned-up (JoinSide-stripped) expression,
/// if the expression is a simple column reference.
fn expr_col_name(expr: &Expr) -> Option<&str> {
    if let Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) = expr {
        Some(name.as_ref())
    } else {
        None
    }
}

/// Find the first partition key column name that appears as an equality join
/// key on both sides.  Returns `None` if no such column exists.
fn find_shared_partition_key<'a>(
    l_pkeys: &'a [String],
    r_pkeys: &[String],
    l_eq_keys: &[Arc<Expr>],
    r_eq_keys: &[Arc<Expr>],
) -> Option<&'a str> {
    for (l_expr, r_expr) in l_eq_keys.iter().zip(r_eq_keys.iter()) {
        let Some(l_name) = expr_col_name(l_expr) else {
            continue;
        };
        let Some(r_name) = expr_col_name(r_expr) else {
            continue;
        };
        // Return a reference into l_pkeys (lifetime 'a) rather than into l_expr.
        if let Some(lk) = l_pkeys.iter().find(|k| k.as_str() == l_name) {
            if r_pkeys.iter().any(|k| k.as_str() == r_name) {
                return Some(lk.as_str());
            }
        }
    }
    None
}

/// Group scan tasks by the single-column PartitionSpec for `col_name`.
/// Tasks without a partition spec (or missing the column) are grouped under `None`.
fn group_tasks_by_partition_col(
    tasks: &[ScanTaskRef],
    col_name: &str,
) -> HashMap<Option<PartitionSpec>, Vec<ScanTaskRef>> {
    let mut groups: HashMap<Option<PartitionSpec>, Vec<ScanTaskRef>> = HashMap::new();
    for task in tasks {
        let key: Option<PartitionSpec> = task.partition_spec().and_then(|ps| {
            let indices = ps.keys.schema.get_fields_with_name(col_name);
            let (col_idx, _) = indices.first()?;
            let rb = ps.keys.get_columns(&[*col_idx]);
            Some(PartitionSpec { keys: rb })
        });
        groups.entry(key).or_default().push(task.clone());
    }
    groups
}

/// Rebuild the Source node (preserving all config) with a new task list.
fn source_with_tasks(source_plan: &LogicalPlan, tasks: Vec<ScanTaskRef>) -> Arc<LogicalPlan> {
    let LogicalPlan::Source(source) = source_plan else {
        unreachable!("source_with_tasks called on non-Source plan");
    };
    let SourceInfo::Physical(psi) = source.source_info.as_ref() else {
        unreachable!();
    };
    let new_psi = psi.with_scan_state(ScanState::Tasks(Arc::new(tasks)));
    let new_source = source
        .clone()
        .with_source_info(Arc::new(SourceInfo::Physical(new_psi)));
    Arc::new(LogicalPlan::Source(new_source))
}

// ── rule body ─────────────────────────────────────────────────────────────────

impl CollocatedJoin {
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Match an inner Join whose children are both materialized Source nodes.
        let LogicalPlan::Join(join) = plan.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }

        let Some((l_tasks, l_pkeys)) = source_partition_info(&join.left) else {
            return Ok(Transformed::no(plan));
        };
        let Some((r_tasks, r_pkeys)) = source_partition_info(&join.right) else {
            return Ok(Transformed::no(plan));
        };

        // Split out equality predicates.
        let (_remaining, l_eq_keys, r_eq_keys, _null_eq) = join.on.split_eq_preds();
        if l_eq_keys.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let Some(pk_col) =
            find_shared_partition_key(&l_pkeys, &r_pkeys, &l_eq_keys, &r_eq_keys)
        else {
            return Ok(Transformed::no(plan));
        };

        // Group each side's tasks by the shared partition key value.
        let l_groups = group_tasks_by_partition_col(&l_tasks, pk_col);
        let r_groups = group_tasks_by_partition_col(&r_tasks, pk_col);

        // Build one sub-join per partition value present in both sides.
        let mut sub_plans: Vec<Arc<LogicalPlan>> = Vec::new();

        // Handle any tasks whose partition spec was None or missing the column
        // conservatively by joining against all R tasks (rare in practice).
        if let Some(l_unkeyed) = l_groups.get(&None::<PartitionSpec>) {
            let r_all: Vec<ScanTaskRef> = r_tasks.iter().cloned().collect();
            let l_src = source_with_tasks(&join.left, l_unkeyed.to_vec());
            let r_src = source_with_tasks(&join.right, r_all);
            let sub = Join::try_new(
                l_src,
                r_src,
                join.on.clone(),
                join.join_type,
                join.join_strategy,
            )?;
            sub_plans.push(Arc::new(LogicalPlan::Join(sub)));
        }

        for (pspec_opt, l_subtasks) in &l_groups {
            let pspec = match pspec_opt {
                Some(ps) => ps,
                None => continue, // already handled above
            };
            let r_subtasks = match r_groups.get(&Some(pspec.clone())) {
                Some(tasks) => tasks,
                None => continue,
            };
            let l_src = source_with_tasks(&join.left, l_subtasks.to_vec());
            let r_src = source_with_tasks(&join.right, r_subtasks.to_vec());
            let sub = Join::try_new(
                l_src,
                r_src,
                join.on.clone(),
                join.join_type,
                join.join_strategy,
            )?;
            sub_plans.push(Arc::new(LogicalPlan::Join(sub)));
        }

        if sub_plans.is_empty() {
            return Ok(Transformed::no(plan));
        }
        // Only one partition? No gain from wrapping in Concat.
        if sub_plans.len() == 1 && l_groups.get(&None).is_none() {
            return Ok(Transformed::no(plan));
        }

        // Fold sub-plans into a left-leaning Concat tree.
        let result = sub_plans
            .into_iter()
            .reduce(|acc, p| {
                Arc::new(LogicalPlan::Concat(
                    Concat::try_new(acc, p).expect("sub-joins must have same schema"),
                ))
            })
            .unwrap();

        Ok(Transformed::yes(result))
    }
}
