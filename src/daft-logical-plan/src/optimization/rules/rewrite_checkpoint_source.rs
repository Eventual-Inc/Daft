//! Rewrites `Source { checkpoint: Some(cfg) }` into a KeyFiltering anti-join
//! that skips already-checkpointed keys. No I/O at rule time — the scan
//! operator enumerates key files lazily in `to_scan_tasks`.

use std::sync::Arc;

use daft_common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_checkpoint::BlobStoreCheckpointedKeysScanOperator;
use daft_core::join::{JoinStrategy, JoinType};
use daft_dsl::unresolved_col;
use daft_scan::ScanOperatorRef;
use daft_schema::schema::Schema;

use super::OptimizerRule;
use crate::{
    LogicalPlan, LogicalPlanBuilder,
    ops::{KeyFilteringConfig, StageCheckpointKeys, join::JoinOptions},
};

#[derive(Default, Debug)]
pub struct RewriteCheckpointSource {}

impl RewriteCheckpointSource {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

/// Returns `true` if `plan` or any of its descendants is a `Source` with
/// `checkpoint = Some(_)`.
fn subtree_contains_checkpoint_source(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Source(s) = plan
        && s.checkpoint.is_some()
    {
        return true;
    }
    plan.children()
        .iter()
        .any(|c| subtree_contains_checkpoint_source(c))
}

/// Checkpointing only supports map-only pipelines downstream of the source.
/// If the plan contains a shuffle/materialization operator (aggregate, sort,
/// distinct, join, pivot, window, repartition, etc.) with a checkpoint source
/// in its subtree, the key column would be lost or re-ordered before reaching
/// the sink — so we reject at rule time with a clear error.
fn validate_map_only_downstream(plan: &LogicalPlan) -> DaftResult<()> {
    fn op_name(plan: &LogicalPlan) -> &'static str {
        match plan {
            LogicalPlan::Sort(_) => "Sort",
            LogicalPlan::TopN(_) => "TopN",
            LogicalPlan::Repartition(_) => "Repartition",
            LogicalPlan::IntoPartitions(_) => "IntoPartitions",
            LogicalPlan::Shuffle(_) => "Shuffle",
            LogicalPlan::Distinct(_) => "Distinct",
            LogicalPlan::Aggregate(_) => "Aggregate",
            LogicalPlan::Pivot(_) => "Pivot",
            LogicalPlan::Window(_) => "Window",
            LogicalPlan::Join(_) => "Join",
            LogicalPlan::AsofJoin(_) => "AsofJoin",
            LogicalPlan::Concat(_) => "Concat",
            LogicalPlan::Intersect(_) => "Intersect",
            LogicalPlan::Union(_) => "Union",
            // Limit picks rows from the anti-join output, not from the user's
            // original source. Example: source = [a, b, c, d], `.limit(2)`.
            //   Run 1: anti-join = [a, b, c, d] (nothing checkpointed yet);
            //          Limit keeps [a, b]; sink writes [a, b]; all four keys
            //          are checkpointed (a, b by the write path; c, d by SCKO
            //          as "processed").
            //   Run 2: anti-join = []; Limit keeps []; sink writes nothing.
            // Combined output across runs: [a, b] — but if the same pipeline
            // ran freshly (no checkpoint), the user would expect "first 2 of
            // whatever's in the source," which they already got. Fine so far.
            // The failure mode: data appended to the source between runs.
            //   Run 2 with source = [a, b, c, d, e, f]: anti-join = [e, f];
            //   Limit keeps [e, f]. The user asked "first 2 rows of the source"
            //   and got [a, b] the first time and [e, f] the second — semantics
            //   across runs are not "first 2 rows of the source."
            LogicalPlan::Limit(_) => "Limit",
            // Offset has the dual problem: `source = [a, b, c, d].offset(2)`
            // intends to drop [a, b] and keep [c, d]. After Run 1 commits
            // [c, d], Run 2 sees anti-join = [a, b]; `.offset(2)` returns
            // empty; and SCKO still checkpoints a, b. The user's intent — "a and b
            // never make it to the sink" — is preserved for the sink output,
            // but a and b are now marked processed in the checkpoint store,
            // so a future run that expects them to re-appear never sees them.
            LogicalPlan::Offset(_) => "Offset",
            // Sample is non-deterministic: different rows survive each run
            // even with identical source data. Example: source = [a..j],
            // `.sample(fraction=0.3)`.
            //   Run 1: sample keeps {b, e, h}; sink writes 3 rows; SCKO
            //          checkpoints all 10 source keys as "processed."
            //   Run 2 on the same source: anti-join = []; sample has nothing
            //          to draw from; sink writes 0 rows.
            // If the user's intent is "give me a 30% sample of the data,"
            // they got it on Run 1 — but any subsequent run produces nothing,
            // even though the source didn't change. This silently masks real
            // pipeline regressions (a re-run that "succeeds with 0 rows").
            LogicalPlan::Sample(_) => "Sample",
            _ => "",
        }
    }

    fn walk(plan: &LogicalPlan) -> DaftResult<()> {
        let shuffle_op = op_name(plan);
        if !shuffle_op.is_empty()
            && plan
                .children()
                .iter()
                .any(|c| subtree_contains_checkpoint_source(c))
        {
            return Err(DaftError::ValueError(format!(
                "Checkpointing only supports map-only pipelines downstream of the source. \
                 Found `{shuffle_op}` between a checkpoint-enabled source and the sink. \
                 Shuffle/materialization ops (aggregate, sort, distinct, join, pivot, window, \
                 repartition) break the source→sink key flow; row-selectors that interact with \
                 checkpointed-key filtering (limit, offset, sample) are also unsupported."
            )));
        }
        for child in plan.children() {
            walk(child)?;
        }
        Ok(())
    }

    walk(plan)
}

impl OptimizerRule for RewriteCheckpointSource {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        validate_map_only_downstream(plan.as_ref())?;
        plan.transform_down(|node| {
            let LogicalPlan::Source(source) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };
            let Some(cfg) = source.checkpoint.as_ref() else {
                return Ok(Transformed::no(node));
            };
            let cfg = cfg.clone();

            // Left: original source. We strip `checkpoint` from the copy that
            // feeds the anti-join so this node won't re-match as a checkpoint
            // source on subsequent optimizer passes; the staging responsibility
            // now lives on the `StageCheckpointKeys` node we wrap above.
            let left_plan: Arc<LogicalPlan> = {
                let mut source_copy = source.clone();
                source_copy.checkpoint = None;
                Arc::new(LogicalPlan::Source(source_copy))
            };

            // Right: scan over checkpointed key files.
            let key_field = source.output_schema.get_field(&cfg.key_column)?;
            let key_schema = Arc::new(Schema::new(vec![key_field.clone()]));
            let scan_op = ScanOperatorRef(Arc::new(BlobStoreCheckpointedKeysScanOperator::new(
                cfg.store.clone(),
                key_schema,
            )));
            let right_plan = LogicalPlanBuilder::table_scan(scan_op, None)?.build();

            let left_schema = left_plan.schema();
            let right_schema = right_plan.schema();
            // TODO: Decouple key column naming — the right side (checkpointed
            // keys scan) should use a canonical column name (e.g. "key") instead
            // of matching the source's column name.
            let l = unresolved_col(cfg.key_column.as_str()).to_left_cols(left_schema)?;
            let r = unresolved_col(cfg.key_column.as_str()).to_right_cols(right_schema)?;
            let on_expr = l.eq(r);

            // TODO: These KFJ parameters are hardcoded. They should
            // scale with the cluster or be configurable via CheckpointConfig.
            let kfc = KeyFilteringConfig::new(
                Some(2),   // num_workers
                Some(1.0), // cpus_per_worker
                None,      // keys_load_batch_size (unused by bridge)
                Some(1),   // max_concurrency_per_worker
                None,      // filter_batch_size (unused by bridge)
            )
            .map_err(|e| DaftError::ComputeError(format!("{e}")))?
            .with_key_columns(vec![cfg.key_column.clone()], vec![cfg.key_column.clone()])
            .map_err(|e| DaftError::ComputeError(format!("{e}")))?;

            let joined = LogicalPlanBuilder::from(left_plan).join(
                right_plan,
                Some(on_expr),
                vec![],
                JoinType::Anti,
                Some(JoinStrategy::KeyFiltering),
                JoinOptions::default(),
            )?;

            let LogicalPlan::Join(join) = joined.build().as_ref().clone() else {
                unreachable!("LogicalPlanBuilder::join() must return a Join node");
            };
            let join = join.with_key_filtering_config(Some(kfc));
            let join_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Join(join));

            // Stage surviving (not-yet-checkpointed) source keys immediately above the
            // anti-join. Downstream map-only operators (filter/project/UDF) see
            // already-staged rows, so re-runs skip them regardless of whether
            // those rows end up in the sink.
            let stage = StageCheckpointKeys::new(join_plan, cfg);

            Ok(Transformed::new(
                Arc::new(LogicalPlan::StageCheckpointKeys(stage)),
                true,
                TreeNodeRecursion::Jump,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use common_checkpoint_config::{CheckpointConfig, CheckpointStoreConfig};
    use daft_core::prelude::DataType;
    use daft_schema::field::Field;

    use super::*;
    use crate::{
        SourceInfo,
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn scan_with_checkpoint(cfg: Option<CheckpointConfig>) -> Arc<LogicalPlan> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("key", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op).build();
        if let Some(cfg) = cfg {
            let LogicalPlan::Source(source) = plan.as_ref() else {
                unreachable!();
            };
            let mut source = source.clone();
            source.checkpoint = Some(cfg);
            Arc::new(LogicalPlan::Source(source))
        } else {
            plan
        }
    }

    #[test]
    fn untouched_when_no_checkpoint() {
        let plan = scan_with_checkpoint(None);
        let result = RewriteCheckpointSource::new()
            .try_optimize(plan.clone())
            .unwrap();
        assert!(!result.transformed);
        assert!(Arc::ptr_eq(&plan, &result.data));
    }

    #[test]
    fn rewrites_source_with_checkpoint_into_staged_kfj_anti_join() {
        let cfg = CheckpointConfig {
            store: CheckpointStoreConfig::ObjectStore {
                prefix: "s3://does-not-matter-for-rewrite".to_string(),
                io_config: Box::new(common_io_config::IOConfig::default()),
            },
            key_column: "key".to_string(),
        };
        let plan = scan_with_checkpoint(Some(cfg));

        let result = RewriteCheckpointSource::new().try_optimize(plan).unwrap();
        assert!(result.transformed);

        // Root is StageCheckpointKeys wrapping the anti-join.
        let LogicalPlan::StageCheckpointKeys(stage) = result.data.as_ref() else {
            panic!(
                "expected StageCheckpointKeys at root, got {:?}",
                result.data
            );
        };
        assert_eq!(stage.checkpoint_config.key_column, "key");

        let LogicalPlan::Join(join) = stage.input.as_ref() else {
            panic!(
                "expected Join under StageCheckpointKeys, got {:?}",
                stage.input
            );
        };
        assert_eq!(join.join_type, JoinType::Anti);
        assert_eq!(join.join_strategy, Some(JoinStrategy::KeyFiltering));

        let kfc = join
            .key_filtering_config
            .as_ref()
            .expect("KeyFilteringConfig must be attached");
        assert_eq!(kfc.left_key_columns, vec!["key".to_string()]);
        assert_eq!(kfc.right_key_columns, vec!["key".to_string()]);

        // Left side: Source with `checkpoint` stripped — staging responsibility
        // lives on the wrapping StageCheckpointKeys node, not on the Source.
        let LogicalPlan::Source(left) = join.left.as_ref() else {
            panic!("expected Source on left, got {:?}", join.left);
        };
        assert!(left.checkpoint.is_none());

        // Right side: Source wrapping the BlobStoreCheckpointedKeysScanOperator.
        let LogicalPlan::Source(right) = join.right.as_ref() else {
            panic!("expected Source on right, got {:?}", join.right);
        };
        let SourceInfo::Physical(physical) = right.source_info.as_ref() else {
            panic!("expected physical source info on right");
        };
        assert_eq!(
            physical.scan_state.get_scan_op().0.name(),
            "BlobStoreCheckpointedKeysScanOperator"
        );
    }
}
