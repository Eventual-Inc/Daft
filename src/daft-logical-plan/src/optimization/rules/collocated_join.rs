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
fn source_partition_info(plan: &LogicalPlan) -> Option<(Arc<Vec<ScanTaskRef>>, Vec<String>)> {
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

/// Deterministic sort key for a partition-value group, computed here at the
/// materialization site where the concrete `PartitionSpec`/tasks are known.
/// `daft_scan::PartitionSpec` (defined in `daft-stats`) implements neither
/// `Ord` nor `PartialOrd`, and shouldn't gain one just for this rule, so we
/// can't sort inside the generic `plan_sub_joins` — we derive an owned key
/// here instead.
///
/// Primary: a canonical string rendering of the partition value itself, so
/// groups sort in a human-meaningful order (the review's ask to "sort by
/// partition key").
/// Secondary (tie-break): the group's sorted source file paths. For a
/// hive-partitioned layout a file's path encodes its partition value, so
/// distinct groups own disjoint path lists and the order is total. This is a
/// strong practical guarantee rather than a type-level one: two distinct
/// partition values rendering identically AND owning identical path multisets
/// (e.g. one physical file scanned under two differing specs) would tie, and a
/// tie falls back to `sort_by_cached_key`'s stable order — i.e. the incoming
/// HashMap order, which is not deterministic. Not reachable for real hive
/// scans; noted so a future reader doesn't over-trust the "unique" claim.
fn group_sort_key(key: &Option<PartitionSpec>, tasks: &[ScanTaskRef]) -> (String, Vec<String>) {
    let rendered = key
        .as_ref()
        .map(|ps| ps.keys.to_string())
        .unwrap_or_default();
    let mut paths: Vec<String> = tasks.iter().flat_map(|t| t.get_file_paths()).collect();
    paths.sort();
    (rendered, paths)
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

/// One sub-join to materialize during the collocated rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SubJoin<K> {
    /// Left tasks with no resolvable partition value × ALL right tasks.
    LeftUnkeyedVsAllRight,
    /// Right tasks with no resolvable partition value × left KEYED tasks only
    /// (left-unkeyed × right-unkeyed is already covered by the arm above —
    /// pairing against ALL left tasks would emit those pairs twice).
    RightUnkeyedVsKeyedLeft,
    /// One partition value present on both sides.
    KeyedPair(K),
}

/// Decide the sub-joins for the rewrite, given each side's group keys
/// (`None` = tasks with no resolvable partition value for the shared column).
///
/// Returns `None` to abort the rewrite: when both sides have keyed groups but
/// not a single value pairs up, the mismatch is systematic (e.g. the two sides
/// inferred different partition-value dtypes, so no `PartitionSpec` can ever
/// compare equal), not a genuine absence — only the original, un-split join is
/// safe. A *partial* pairing, by contrast, is genuine: this rewrite only fires
/// for Inner joins, where a left value absent on the right correctly
/// contributes zero rows, so unpaired keyed groups are simply skipped.
fn plan_sub_joins<K: Eq + std::hash::Hash + Clone>(
    l_keys: &[Option<K>],
    r_keys: &[Option<K>],
) -> Option<Vec<SubJoin<K>>> {
    let l_keyed: Vec<&K> = l_keys.iter().flatten().collect();
    let r_keyed: std::collections::HashSet<&K> = r_keys.iter().flatten().collect();
    let l_has_unkeyed = l_keys.iter().any(Option::is_none);
    let r_has_unkeyed = r_keys.iter().any(Option::is_none);

    let pairs: Vec<&K> = l_keyed
        .iter()
        .copied()
        .filter(|k| r_keyed.contains(*k))
        .collect();
    if !l_keyed.is_empty() && !r_keyed.is_empty() && pairs.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    if l_has_unkeyed {
        out.push(SubJoin::LeftUnkeyedVsAllRight);
    }
    if r_has_unkeyed && !l_keyed.is_empty() {
        out.push(SubJoin::RightUnkeyedVsKeyedLeft);
    }
    out.extend(pairs.into_iter().cloned().map(SubJoin::KeyedPair));
    Some(out)
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

        let Some(pk_col) = find_shared_partition_key(&l_pkeys, &r_pkeys, &l_eq_keys, &r_eq_keys)
        else {
            return Ok(Transformed::no(plan));
        };

        // Group each side's tasks by the shared partition key value.
        let l_groups = group_tasks_by_partition_col(&l_tasks, pk_col);
        let r_groups = group_tasks_by_partition_col(&r_tasks, pk_col);

        // `l_groups`/`r_groups` are `HashMap`s, so iterating their keys
        // directly is nondeterministic run-to-run. `plan_sub_joins` emits its
        // `SubJoin::KeyedPair`s in `l_keys`' order (see its `l_keyed`), so
        // that nondeterminism would otherwise leak into the sub-join order
        // (hence the `Concat` tree shape). Sort `l_keys` deterministically
        // before planning; see `group_sort_key` for why the sort happens here
        // rather than inside `plan_sub_joins`. `r_keys`' order does not
        // affect `plan_sub_joins`'s output (it is only ever consulted via a
        // `HashSet`/`any`), so it is left as-is.
        let mut l_keys: Vec<Option<PartitionSpec>> = l_groups.keys().cloned().collect();
        l_keys.sort_by_cached_key(|k| {
            group_sort_key(k, l_groups.get(k).expect("key sourced from l_groups"))
        });
        let r_keys: Vec<Option<PartitionSpec>> = r_groups.keys().cloned().collect();
        let Some(planned) = plan_sub_joins(&l_keys, &r_keys) else {
            // Systematic partition-value mismatch: fall back to the original join.
            return Ok(Transformed::no(plan));
        };
        // A single sub-join can't reduce peak memory; keep the original plan.
        if planned.len() <= 1 {
            return Ok(Transformed::no(plan));
        }

        let make_sub_join = |l_tasks_sub: Vec<ScanTaskRef>,
                             r_tasks_sub: Vec<ScanTaskRef>|
         -> DaftResult<Arc<LogicalPlan>> {
            let l_src = source_with_tasks(&join.left, l_tasks_sub);
            let r_src = source_with_tasks(&join.right, r_tasks_sub);
            let sub = Join::try_new(
                l_src,
                r_src,
                join.on.clone(),
                join.join_type,
                join.join_strategy,
            )?;
            Ok(Arc::new(LogicalPlan::Join(sub)))
        };

        // Iterate the already-sorted `l_keys` (rather than `l_groups`
        // directly) so `l_keyed_tasks`' group order is deterministic too —
        // and matches the `KeyedPair` emission order above, since both are
        // driven by the same sort. Task order *within* a group is already
        // deterministic: `group_tasks_by_partition_col` preserves the
        // original `l_tasks` `Vec` order, which is not derived from a
        // `HashMap`.
        let l_keyed_tasks: Vec<ScanTaskRef> = l_keys
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(|ps| {
                l_groups
                    .get(&Some(ps.clone()))
                    .expect("key sourced from l_groups")
                    .iter()
                    .cloned()
            })
            .collect();

        let mut sub_plans: Vec<Arc<LogicalPlan>> = Vec::with_capacity(planned.len());
        for sub in planned {
            match sub {
                SubJoin::LeftUnkeyedVsAllRight => {
                    let l_unkeyed = l_groups.get(&None).expect("planned implies present");
                    sub_plans.push(make_sub_join(
                        l_unkeyed.clone(),
                        r_tasks.iter().cloned().collect(),
                    )?);
                }
                SubJoin::RightUnkeyedVsKeyedLeft => {
                    let r_unkeyed = r_groups.get(&None).expect("planned implies present");
                    sub_plans.push(make_sub_join(l_keyed_tasks.clone(), r_unkeyed.clone())?);
                }
                SubJoin::KeyedPair(ps) => {
                    let l_sub = l_groups
                        .get(&Some(ps.clone()))
                        .expect("planned implies present");
                    let r_sub = r_groups.get(&Some(ps)).expect("planned implies present");
                    sub_plans.push(make_sub_join(l_sub.clone(), r_sub.clone())?);
                }
            }
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

#[cfg(test)]
mod pairing_tests {
    use super::*;

    // K = i32 stands in for PartitionSpec: the planner is generic over the key.

    #[test]
    fn keyed_pairs_and_partial_overlap() {
        // Left has {1,2,3}, right has {2,3,4}: pairs {2,3}; 1 and 4 are genuine
        // inner-join misses and are correctly skipped (no abort).
        let l = vec![Some(1), Some(2), Some(3)];
        let r = vec![Some(2), Some(3), Some(4)];
        let plan = plan_sub_joins(&l, &r).expect("partial overlap must not abort");
        assert!(plan.contains(&SubJoin::KeyedPair(2)));
        assert!(plan.contains(&SubJoin::KeyedPair(3)));
        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn right_unkeyed_tasks_are_joined_against_keyed_left() {
        let l = vec![Some(1), Some(2)];
        let r = vec![Some(1), None];
        let plan = plan_sub_joins(&l, &r).unwrap();
        assert!(plan.contains(&SubJoin::RightUnkeyedVsKeyedLeft));
        assert!(plan.contains(&SubJoin::KeyedPair(1)));
    }

    #[test]
    fn unkeyed_on_both_sides_is_not_double_counted() {
        // left-unkeyed × right-unkeyed must be covered by LeftUnkeyedVsAllRight
        // ONLY (RightUnkeyedVsKeyedLeft pairs with keyed left tasks exclusively).
        let l = vec![Some(1), None];
        let r = vec![Some(1), None];
        let plan = plan_sub_joins(&l, &r).unwrap();
        assert!(plan.contains(&SubJoin::LeftUnkeyedVsAllRight));
        assert!(plan.contains(&SubJoin::RightUnkeyedVsKeyedLeft));
        assert!(plan.contains(&SubJoin::KeyedPair(1)));
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn systematic_mismatch_aborts() {
        // Both sides keyed, zero pairs: e.g. dtype-divergent PartitionSpecs that can
        // never compare equal. Skipping would silently drop every row — abort instead.
        let l = vec![Some(1), Some(2)];
        let r = vec![Some(10), Some(20)];
        assert!(plan_sub_joins(&l, &r).is_none());
    }

    #[test]
    fn systematic_mismatch_with_left_unkeyed_still_aborts() {
        // The dangerous variant: a left unkeyed group used to bypass the guards and
        // fire a rewrite that drops all keyed left tasks.
        let l = vec![Some(1), Some(2), None];
        let r = vec![Some(10)];
        assert!(plan_sub_joins(&l, &r).is_none());
    }

    #[test]
    fn all_right_unkeyed_pairs_with_keyed_left() {
        // e.g. differently-named partition column on the right: every right task
        // groups under None; keyed left must still see them.
        let l = vec![Some(1), Some(2)];
        let r = vec![None];
        let plan = plan_sub_joins(&l, &r).unwrap();
        assert_eq!(plan, vec![SubJoin::RightUnkeyedVsKeyedLeft]);
    }

    #[test]
    fn find_shared_partition_key_requires_both_sides() {
        use daft_dsl::resolved_col;
        let l_pkeys = vec!["region".to_string()];
        let r_pkeys: Vec<String> = vec![];
        let l_eq = vec![resolved_col("region")];
        let r_eq = vec![resolved_col("region")];
        assert_eq!(
            find_shared_partition_key(&l_pkeys, &r_pkeys, &l_eq, &r_eq),
            None
        );
        let r_pkeys = vec!["region".to_string()];
        assert_eq!(
            find_shared_partition_key(&l_pkeys, &r_pkeys, &l_eq, &r_eq),
            Some("region")
        );
    }
}

/// Exercises the MATERIALIZATION step of `try_optimize_node` end-to-end (real
/// `ScanTask`s through `CollocatedJoin::try_optimize`), not just the pure
/// `plan_sub_joins` planner covered by `pairing_tests` above. In particular
/// this pins that `RightUnkeyedVsKeyedLeft` is materialized against
/// `l_keyed_tasks` (keyed left tasks ONLY), not `l_tasks` (all left tasks) —
/// a regression that would pass every `pairing_tests` case since those only
/// exercise the key-level planner, never the task lists it's materialized
/// against.
#[cfg(test)]
mod materialization_tests {
    use std::sync::Arc as StdArc;

    use daft_core::{datatypes::Utf8Array, series::IntoSeries};
    use daft_dsl::{left_col, right_col};
    use daft_recordbatch::RecordBatch;
    use daft_scan::{
        FileFormatConfig, ParquetSourceConfig, PartitionField, PhysicalScanInfo, Pushdowns,
        ScanOperatorRef, ScanSource, ScanSourceKind, ScanTask, SourceConfig,
        storage_config::StorageConfig, test_utils::DummyScanOperator,
    };
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
    };

    use super::*;
    use crate::ops::{Source, join::JoinPredicate};

    const REGION: &str = "region";

    fn region_field() -> Field {
        Field::new(REGION, DataType::Utf8)
    }

    /// Build one scan task at `path`. `region_val` mirrors the shared
    /// partition value: `Some(v)` tags the task's `PartitionSpec` with
    /// `region = v`; `None` means "no resolvable partition value" (unkeyed).
    fn make_task(path: &str, schema: SchemaRef, region_val: Option<&str>) -> ScanTaskRef {
        let partition_spec = region_val.map(|v| {
            let series = Utf8Array::from_slice(REGION, &[v]).into_series();
            let keys = RecordBatch::from_nonempty_columns(vec![series]).unwrap();
            PartitionSpec { keys }
        });
        StdArc::new(ScanTask::new(
            vec![ScanSource {
                size_bytes: None,
                metadata: None,
                statistics: None,
                partition_spec,
                kind: ScanSourceKind::File {
                    path: path.to_string(),
                    chunk_spec: None,
                    iceberg_delete_files: None,
                    parquet_metadata: None,
                },
            }],
            StdArc::new(SourceConfig::File(FileFormatConfig::Parquet(
                ParquetSourceConfig {
                    coerce_int96_timestamp_unit: daft_schema::time_unit::TimeUnit::Seconds,
                    field_id_mapping: None,
                    row_groups: None,
                    chunk_size: None,
                    ignore_corrupt_files: false,
                    geometry: false,
                },
            ))),
            schema,
            StdArc::new(StorageConfig::new_internal(false, None)),
            Pushdowns::default(),
            None,
        ))
    }

    /// A materialized `Source` plan over `tasks`, hive-partitioned on `region`.
    fn make_source(schema: SchemaRef, tasks: Vec<ScanTaskRef>) -> Arc<LogicalPlan> {
        let scan_op = StdArc::new(DummyScanOperator {
            schema: schema.clone(),
            ..Default::default()
        });
        let mut psi = PhysicalScanInfo::new(
            ScanOperatorRef(scan_op),
            schema.clone(),
            vec![PartitionField::new(region_field(), None, None).unwrap()],
            Pushdowns::default(),
            None,
        );
        psi.scan_state = ScanState::Tasks(StdArc::new(tasks));
        Arc::new(LogicalPlan::Source(Source::new(
            schema,
            StdArc::new(SourceInfo::Physical(psi)),
        )))
    }

    /// Sorted source file paths under a `Source` plan node (panics on any other shape).
    fn source_paths(plan: &LogicalPlan) -> Vec<String> {
        let LogicalPlan::Source(s) = plan else {
            panic!("expected Source, got {plan:?}")
        };
        let SourceInfo::Physical(p) = s.source_info.as_ref() else {
            panic!("expected a physical scan")
        };
        let ScanState::Tasks(tasks) = &p.scan_state else {
            panic!("expected materialized ScanState::Tasks")
        };
        let mut paths: Vec<String> = tasks.iter().flat_map(|t| t.get_file_paths()).collect();
        paths.sort();
        paths
    }

    /// Flatten the left-leaning `Concat` tree the rule builds into the
    /// (sorted left paths, sorted right paths) of each leaf `Join`.
    fn collect_sub_joins(plan: &LogicalPlan) -> Vec<(Vec<String>, Vec<String>)> {
        let mut out = Vec::new();
        fn walk(plan: &LogicalPlan, out: &mut Vec<(Vec<String>, Vec<String>)>) {
            match plan {
                LogicalPlan::Concat(c) => {
                    walk(&c.input, out);
                    walk(&c.other, out);
                }
                LogicalPlan::Join(j) => {
                    out.push((source_paths(&j.left), source_paths(&j.right)));
                }
                other => panic!("unexpected node under the sub-join Concat tree: {other:?}"),
            }
        }
        walk(plan, &mut out);
        out
    }

    #[test]
    fn right_unkeyed_pairs_with_keyed_left_only_not_all_left_tasks() {
        let schema = StdArc::new(Schema::new(vec![
            region_field(),
            Field::new("value", DataType::Int64),
        ]));

        // Left: one unkeyed task + one task keyed to "A".
        let l_unkeyed = make_task("left/unkeyed/part-0.parquet", schema.clone(), None);
        let l_a = make_task("left/region=A/part-0.parquet", schema.clone(), Some("A"));
        let left = make_source(schema.clone(), vec![l_unkeyed, l_a]);

        // Right: one task keyed to "A" (present on both sides) + one unkeyed task.
        let r_a = make_task("right/region=A/part-0.parquet", schema.clone(), Some("A"));
        let r_unkeyed = make_task("right/unkeyed/part-0.parquet", schema.clone(), None);
        let right = make_source(schema.clone(), vec![r_a, r_unkeyed]);

        let on =
            JoinPredicate::try_new(Some(left_col(region_field()).eq(right_col(region_field()))))
                .unwrap();
        let join = Join::try_new(left, right, on, JoinType::Inner, None).unwrap();
        let plan = Arc::new(LogicalPlan::Join(join));

        let result = CollocatedJoin.try_optimize(plan).unwrap();
        assert!(
            result.transformed,
            "rule must fire: shared region key on both sides"
        );

        let sub_joins = collect_sub_joins(&result.data);
        assert_eq!(
            sub_joins.len(),
            3,
            "expected LeftUnkeyedVsAllRight + RightUnkeyedVsKeyedLeft + KeyedPair(A), got {sub_joins:?}"
        );

        // The critical property: the sub-join whose right side is exactly the
        // right-unkeyed task must have a LEFT side that is EXACTLY the keyed
        // left tasks (["left/region=A/part-0.parquet"]) — NOT all left tasks.
        // Substituting `l_tasks` for `l_keyed_tasks` in the
        // `RightUnkeyedVsKeyedLeft` arm would leak `l_unkeyed`'s path in here
        // too, double-counting rows already covered by `LeftUnkeyedVsAllRight`.
        let right_unkeyed_sub_join = sub_joins
            .iter()
            .find(|(_, r)| r == &vec!["right/unkeyed/part-0.parquet".to_string()])
            .expect("a RightUnkeyedVsKeyedLeft sub-join must be present");
        assert_eq!(
            right_unkeyed_sub_join.0,
            vec!["left/region=A/part-0.parquet".to_string()],
            "RightUnkeyedVsKeyedLeft must pair right-unkeyed tasks with ONLY the \
             keyed left tasks, not all left tasks"
        );

        // The other two expected sub-joins are present with correct task membership.
        assert!(
            sub_joins.iter().any(|(l, r)| {
                l == &vec!["left/unkeyed/part-0.parquet".to_string()]
                    && r == &vec![
                        "right/region=A/part-0.parquet".to_string(),
                        "right/unkeyed/part-0.parquet".to_string(),
                    ]
            }),
            "LeftUnkeyedVsAllRight must pair the left-unkeyed task against ALL right tasks: {sub_joins:?}"
        );
        assert!(
            sub_joins.iter().any(|(l, r)| {
                l == &vec!["left/region=A/part-0.parquet".to_string()]
                    && r == &vec!["right/region=A/part-0.parquet".to_string()]
            }),
            "KeyedPair(A) must pair the region=A tasks on both sides: {sub_joins:?}"
        );
    }

    #[test]
    fn sub_join_order_is_deterministic_across_repeated_runs() {
        // Three keyed partition values on both sides, built from a `HashMap`
        // grouping internally — repeated optimizer runs over an identical
        // input must yield the identical `Concat`/sub-join tree shape (same
        // `KeyedPair` order), never an order that varies run-to-run with
        // `HashMap` iteration.
        let schema = StdArc::new(Schema::new(vec![region_field()]));
        let build = || {
            let left = make_source(
                schema.clone(),
                vec![
                    make_task("left/region=A/part-0.parquet", schema.clone(), Some("A")),
                    make_task("left/region=B/part-0.parquet", schema.clone(), Some("B")),
                    make_task("left/region=C/part-0.parquet", schema.clone(), Some("C")),
                ],
            );
            let right = make_source(
                schema.clone(),
                vec![
                    make_task("right/region=A/part-0.parquet", schema.clone(), Some("A")),
                    make_task("right/region=B/part-0.parquet", schema.clone(), Some("B")),
                    make_task("right/region=C/part-0.parquet", schema.clone(), Some("C")),
                ],
            );
            let on = JoinPredicate::try_new(Some(
                left_col(region_field()).eq(right_col(region_field())),
            ))
            .unwrap();
            let join = Join::try_new(left, right, on, JoinType::Inner, None).unwrap();
            Arc::new(LogicalPlan::Join(join))
        };

        let first = collect_sub_joins(&CollocatedJoin.try_optimize(build()).unwrap().data);
        for _ in 0..5 {
            let again = collect_sub_joins(&CollocatedJoin.try_optimize(build()).unwrap().data);
            assert_eq!(
                first, again,
                "sub-join order (Concat tree shape) must be stable across repeated runs"
            );
        }
    }
}
