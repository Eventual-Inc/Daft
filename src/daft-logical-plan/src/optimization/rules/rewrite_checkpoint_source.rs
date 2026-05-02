//! Rewrites `Source { checkpoint: Some(cfg) }` into a KeyFiltering anti-join
//! that skips already-checkpointed keys. No I/O at rule time — the scan
//! operator enumerates key files lazily in `to_scan_tasks`.

use std::sync::Arc;

use common_checkpoint_config::{CheckpointKeyMode, CheckpointSettings, SEALED_KEYS_COLUMN};
use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_checkpoint::BlobStoreCheckpointedKeysScanOperator;
use daft_core::join::{JoinStrategy, JoinType};
use daft_dsl::unresolved_col;
use daft_scan::ScanOperatorRef;
use daft_schema::{field::Field, schema::Schema};

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

            match &cfg.key_mode {
                CheckpointKeyMode::RowLevel { key_column } => {
                    self.rewrite_row_level(source, &cfg, key_column)
                }
                CheckpointKeyMode::FilePath => self.rewrite_file_path(source, &cfg),
            }
        })
    }
}

impl RewriteCheckpointSource {
    fn rewrite_row_level(
        &self,
        source: &crate::ops::Source,
        cfg: &common_checkpoint_config::CheckpointConfig,
        key_column: &str,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Left: original source. We strip `checkpoint` from the copy that
        // feeds the anti-join so this node won't re-match as a checkpoint
        // source on subsequent optimizer passes; the staging responsibility
        // now lives on the `StageCheckpointKeys` node we wrap above.
        let left_plan: Arc<LogicalPlan> = {
            let mut source_copy = source.clone();
            source_copy.checkpoint = None;
            Arc::new(LogicalPlan::Source(source_copy))
        };

        // Right: scan over checkpointed key files. The on-disk schema uses
        // a canonical column name (`SEALED_KEYS_COLUMN`) regardless of
        // what the source calls its key column — see `stage_keys` in the
        // store impl. This means a user can rename the source's key column
        // between runs (updating `cfg.key_column` to match) and existing
        // sealed key files stay valid — only the *physical* on-disk format
        // is decoupled from the source schema, not the config. If
        // `cfg.key_column` ever disagrees with the source's actual schema,
        // `get_field` below errors loudly at rule time.
        let source_key_field = source.output_schema.get_field(key_column)?;
        let canonical_key_field = Field::new(SEALED_KEYS_COLUMN, source_key_field.dtype.clone());
        let key_schema = Arc::new(Schema::new(vec![canonical_key_field]));
        let scan_op = ScanOperatorRef(Arc::new(BlobStoreCheckpointedKeysScanOperator::new(
            cfg.store.clone(),
            key_schema,
        )));
        let right_plan = LogicalPlanBuilder::table_scan(scan_op, None)?.build();

        // Left side resolves to the source's key column; right side resolves
        // to the canonical sealed-keys column. The two are joined for
        // equality, so the source can be renamed without breaking the
        // anti-join against existing sealed key files.
        let left_schema = left_plan.schema();
        let right_schema = right_plan.schema();
        let l = unresolved_col(key_column).to_left_cols(left_schema)?;
        let r = unresolved_col(SEALED_KEYS_COLUMN).to_right_cols(right_schema)?;
        let on_expr = l.eq(r);

        // Pull strategy-specific knobs from `cfg.settings`. Today there's
        // only one variant (`KeyFiltering`); future variants would dispatch
        // to different filter strategies entirely, not just different knobs.
        let CheckpointSettings::KeyFiltering(kf) = &cfg.settings;
        let kfc = KeyFilteringConfig::new(
            kf.num_workers.or(Some(2)),
            kf.cpus_per_worker.as_ref().map(|w| w.0).or(Some(1.0)),
            kf.keys_load_batch_size,
            kf.max_concurrency_per_worker.or(Some(1)),
            kf.filter_batch_size,
        )
        .map_err(|e| DaftError::ComputeError(format!("{e}")))?
        .with_key_columns(
            vec![key_column.to_string()],
            vec![SEALED_KEYS_COLUMN.to_string()],
        )
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

        let stage = StageCheckpointKeys::new(join_plan, cfg.clone());

        Ok(Transformed::new(
            Arc::new(LogicalPlan::StageCheckpointKeys(stage)),
            true,
            TreeNodeRecursion::Jump,
        ))
    }

    fn rewrite_file_path(
        &self,
        source: &crate::ops::Source,
        cfg: &common_checkpoint_config::CheckpointConfig,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let source_plan: Arc<LogicalPlan> = {
            let mut source_copy = source.clone();
            source_copy.checkpoint = None;
            Arc::new(LogicalPlan::Source(source_copy))
        };

        let stage = StageCheckpointKeys::new(source_plan, cfg.clone());

        Ok(Transformed::new(
            Arc::new(LogicalPlan::StageCheckpointKeys(stage)),
            true,
            TreeNodeRecursion::Jump,
        ))
    }
}

#[cfg(test)]
mod tests {
    use common_checkpoint_config::{
        CheckpointConfig, CheckpointKeyMode, CheckpointStoreConfig, KeyFilteringSettings,
    };
    use common_hashable_float_wrapper::FloatWrapper;
    use daft_core::prelude::DataType;
    use daft_schema::field::Field;

    use super::*;
    use crate::{
        SourceInfo,
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn scan_with_checkpoint(cfg: Option<CheckpointConfig>) -> Arc<LogicalPlan> {
        // Use a source column name that is *not* the canonical sealed-keys
        // column name, so assertions that the right side of the anti-join
        // uses the canonical name (rather than the source's name) catch the
        // decoupling. Use `Int64` for the key dtype (instead of the typical
        // `Utf8`) so the dtype-propagation assertion downstream is
        // load-bearing — a regression that hardcoded Utf8 for the canonical
        // field would fail loudly here.
        let scan_op = dummy_scan_operator(vec![
            Field::new("file_id", DataType::Int64),
            Field::new("value", DataType::Utf8),
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
            key_mode: CheckpointKeyMode::RowLevel {
                key_column: "file_id".to_string(),
            },
            settings: common_checkpoint_config::CheckpointSettings::default(),
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
        assert_eq!(stage.checkpoint_config.key_column(), Some("file_id"));

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
        // Left side resolves to the source's column; right side to the
        // canonical sealed-keys column. They must NOT have to match.
        assert_eq!(kfc.left_key_columns, vec!["file_id".to_string()]);
        assert_eq!(kfc.right_key_columns, vec![SEALED_KEYS_COLUMN.to_string()]);

        // Default settings must preserve today's hardcoded values exactly.
        // Pins the rule's `.or(Some(2))` / `.or(Some(1.0))` / `.or(Some(1))`
        // fallbacks against accidental changes.
        assert_eq!(kfc.num_workers, Some(2));
        assert_eq!(kfc.cpus_per_worker.as_ref().map(|w| w.0), Some(1.0));
        assert_eq!(kfc.keys_load_batch_size, None);
        assert_eq!(kfc.max_concurrency_per_worker, Some(1));
        assert_eq!(kfc.filter_batch_size, None);

        // Default settings must preserve today's hardcoded values exactly.
        // Pins the rule's `.or(Some(2))` / `.or(Some(1.0))` / `.or(Some(1))`
        // fallbacks against accidental changes.
        assert_eq!(kfc.num_workers, Some(2));
        assert_eq!(kfc.cpus_per_worker.as_ref().map(|w| w.0), Some(1.0));
        assert_eq!(kfc.keys_load_batch_size, None);
        assert_eq!(kfc.max_concurrency_per_worker, Some(1));
        assert_eq!(kfc.filter_batch_size, None);

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

        // Right plan's *actual schema* must carry the canonical column name
        // with the source key column's dtype — guards against a regression
        // that wires `kfc` correctly but builds the schema with the source's
        // column name (or the wrong dtype). A schema mismatch would only
        // surface at execution time, when the optimizer test wouldn't catch it.
        let right_schema = join.right.schema();
        let canonical_field = right_schema
            .get_field(SEALED_KEYS_COLUMN)
            .expect("right plan schema must carry the canonical sealed-keys column");
        assert_eq!(
            canonical_field.dtype,
            DataType::Int64,
            "canonical column dtype must propagate from the source key column dtype, not be hardcoded"
        );
        assert_eq!(
            right_schema.len(),
            1,
            "right plan schema should have only the canonical key column"
        );
    }

    /// Non-default `KeyFilteringSettings` on `CheckpointConfig` must flow through
    /// to the `KeyFilteringConfig` produced by the rule. Pins the wiring against
    /// regressions where someone reintroduces hardcoded values in the rewrite.
    #[test]
    fn user_supplied_settings_flow_through_to_key_filtering_config() {
        let cfg = CheckpointConfig {
            store: CheckpointStoreConfig::ObjectStore {
                prefix: "s3://does-not-matter-for-rewrite".to_string(),
                io_config: Box::new(common_io_config::IOConfig::default()),
            },
            key_mode: CheckpointKeyMode::RowLevel {
                key_column: "file_id".to_string(),
            },
            settings: common_checkpoint_config::CheckpointSettings::KeyFiltering(
                KeyFilteringSettings {
                    num_workers: Some(7),
                    cpus_per_worker: Some(FloatWrapper(2.5)),
                    keys_load_batch_size: Some(1024),
                    max_concurrency_per_worker: Some(3),
                    filter_batch_size: Some(512),
                },
            ),
        };
        let plan = scan_with_checkpoint(Some(cfg));

        let result = RewriteCheckpointSource::new().try_optimize(plan).unwrap();
        let LogicalPlan::StageCheckpointKeys(stage) = result.data.as_ref() else {
            panic!("expected StageCheckpointKeys at root");
        };
        let LogicalPlan::Join(join) = stage.input.as_ref() else {
            panic!("expected Join under StageCheckpointKeys");
        };
        let kfc = join
            .key_filtering_config
            .as_ref()
            .expect("KeyFilteringConfig must be attached");

        assert_eq!(kfc.num_workers, Some(7));
        assert_eq!(kfc.cpus_per_worker.as_ref().map(|w| w.0), Some(2.5));
        assert_eq!(kfc.keys_load_batch_size, Some(1024));
        assert_eq!(kfc.max_concurrency_per_worker, Some(3));
        assert_eq!(kfc.filter_batch_size, Some(512));
    }

    #[test]
    fn file_path_mode_wraps_source_without_anti_join() {
        let cfg = CheckpointConfig {
            store: CheckpointStoreConfig::ObjectStore {
                prefix: "s3://does-not-matter-for-rewrite".to_string(),
                io_config: Box::new(common_io_config::IOConfig::default()),
            },
            key_mode: CheckpointKeyMode::FilePath,
            settings: common_checkpoint_config::CheckpointSettings::default(),
        };
        let plan = scan_with_checkpoint(Some(cfg.clone()));

        let result = RewriteCheckpointSource::new().try_optimize(plan).unwrap();
        assert!(result.transformed);

        // Root must be StageCheckpointKeys.
        let LogicalPlan::StageCheckpointKeys(stage) = result.data.as_ref() else {
            panic!(
                "expected StageCheckpointKeys at root, got {:?}",
                result.data
            );
        };
        assert!(stage.checkpoint_config.is_file_path_mode());
        assert_eq!(stage.checkpoint_config.key_column(), None);

        // Child must be Source directly — no anti-join in file-path mode.
        let LogicalPlan::Source(source) = stage.input.as_ref() else {
            panic!(
                "expected Source under StageCheckpointKeys (no anti-join), got {:?}",
                stage.input
            );
        };
        assert!(
            source.checkpoint.is_none(),
            "checkpoint must be stripped from the inner Source"
        );
    }

    /// Partial overrides: a user who sets only `num_workers` must still get
    /// the hardcoded fallbacks for `cpus_per_worker` / `max_concurrency_per_worker`,
    /// not `None`. Pins per-field independence in the rule's fallback chain
    /// (`x.or(Some(default))`) — a regression that gates one fallback on
    /// another field being unset would slip past the all-default and
    /// all-override tests above.
    #[test]
    fn partial_settings_override_keeps_per_field_fallbacks() {
        let cfg = CheckpointConfig {
            store: CheckpointStoreConfig::ObjectStore {
                prefix: "s3://does-not-matter-for-rewrite".to_string(),
                io_config: Box::new(common_io_config::IOConfig::default()),
            },
            key_mode: CheckpointKeyMode::RowLevel {
                key_column: "file_id".to_string(),
            },
            settings: common_checkpoint_config::CheckpointSettings::KeyFiltering(
                KeyFilteringSettings {
                    num_workers: Some(7),
                    ..Default::default()
                },
            ),
        };
        let plan = scan_with_checkpoint(Some(cfg));

        let result = RewriteCheckpointSource::new().try_optimize(plan).unwrap();
        let LogicalPlan::StageCheckpointKeys(stage) = result.data.as_ref() else {
            panic!("expected StageCheckpointKeys at root");
        };
        let LogicalPlan::Join(join) = stage.input.as_ref() else {
            panic!("expected Join under StageCheckpointKeys");
        };
        let kfc = join
            .key_filtering_config
            .as_ref()
            .expect("KeyFilteringConfig must be attached");

        // User-supplied — preserved.
        assert_eq!(kfc.num_workers, Some(7));
        // Untouched — must fall back to hardcoded rule defaults.
        assert_eq!(kfc.cpus_per_worker.as_ref().map(|w| w.0), Some(1.0));
        assert_eq!(kfc.max_concurrency_per_worker, Some(1));
        // Untouched fields with no rule fallback — stay `None`.
        assert_eq!(kfc.keys_load_batch_size, None);
        assert_eq!(kfc.filter_batch_size, None);
    }
}
