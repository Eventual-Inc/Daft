use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    array::ops::DaftCompare,
    join::{JoinSide, JoinType},
};
use daft_dsl::{expr::bound_expr::BoundExpr, join::infer_join_schema};
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;
use daft_stats::TruthValue;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    fn join<F>(
        &self,
        right: &Self,
        io_stats: Arc<IOStatsContext>,
        left_on: &[BoundExpr],
        right_on: &[BoundExpr],
        how: JoinType,
        table_join: F,
    ) -> DaftResult<Self>
    where
        F: FnOnce(
            &RecordBatch,
            &RecordBatch,
            &[BoundExpr],
            &[BoundExpr],
            JoinType,
        ) -> DaftResult<RecordBatch>,
    {
        let join_schema = infer_join_schema(&self.schema, &right.schema, how)?;
        match (how, self.len(), right.len()) {
            (JoinType::Inner | JoinType::Left | JoinType::Semi, 0, _)
            | (JoinType::Inner | JoinType::Right, _, 0)
            | (JoinType::Outer, 0, 0) => {
                return Ok(Self::empty(Some(join_schema)));
            }
            _ => {}
        }

        // TODO(Kevin): short circuits are also possible for other join types
        if how == JoinType::Inner {
            let tv = match (&self.statistics, &right.statistics) {
                (_, None) => TruthValue::Maybe,
                (None, _) => TruthValue::Maybe,
                (Some(l), Some(r)) => {
                    let l_eval_stats = l.eval_expression_list(left_on)?;
                    let r_eval_stats = r.eval_expression_list(right_on)?;
                    let mut curr_tv = TruthValue::Maybe;
                    for (lc, rc) in l_eval_stats.into_iter().zip(&r_eval_stats) {
                        if lc.equal(rc)?.to_truth_value() == TruthValue::False {
                            curr_tv = TruthValue::False;
                            break;
                        }
                    }
                    curr_tv
                }
            };
            if tv == TruthValue::False {
                return Ok(Self::empty(Some(join_schema)));
            }
        }

        // TODO(Clark): Elide concatenations where possible by doing a chunk-aware local table join.
        let lt = self.concat_or_get_update(io_stats.clone())?;
        let rt = right.concat_or_get_update(io_stats)?;

        match (lt, rt) {
            (None, _) | (_, None) => Ok(Self::empty(Some(join_schema))),
            (Some(lt), Some(rt)) => {
                let joined_table = table_join(&lt, &rt, left_on, right_on, how)?;
                Ok(Self::new_loaded(
                    join_schema,
                    vec![joined_table].into(),
                    None,
                ))
            }
        }
    }

    pub fn hash_join(
        &self,
        right: &Self,
        left_on: &[BoundExpr],
        right_on: &[BoundExpr],
        null_equals_nulls: Option<Vec<bool>>,
        how: JoinType,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::hash_join");
        let null_equals_nulls = null_equals_nulls.unwrap_or_else(|| vec![false; left_on.len()]);

        let table_join = |lt: &RecordBatch,
                          rt: &RecordBatch,
                          lo: &[BoundExpr],
                          ro: &[BoundExpr],
                          _how: JoinType| {
            RecordBatch::hash_join(lt, rt, lo, ro, null_equals_nulls.as_slice(), _how)
        };

        self.join(right, io_stats, left_on, right_on, how, table_join)
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[BoundExpr],
        right_on: &[BoundExpr],
        is_sorted: bool,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::sort_merge_join");
        let table_join = |lt: &RecordBatch,
                          rt: &RecordBatch,
                          lo: &[BoundExpr],
                          ro: &[BoundExpr],
                          _how: JoinType| {
            RecordBatch::sort_merge_join(lt, rt, lo, ro, is_sorted)
        };

        self.join(
            right,
            io_stats,
            left_on,
            right_on,
            JoinType::Inner,
            table_join,
        )
    }

    pub fn cross_join(&self, right: &Self, outer_loop_side: JoinSide) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::cross_join");

        let table_join =
            |lt: &RecordBatch, rt: &RecordBatch, _: &[BoundExpr], _: &[BoundExpr], _: JoinType| {
                RecordBatch::cross_join(lt, rt, outer_loop_side)
            };

        self.join(right, io_stats, &[], &[], JoinType::Inner, table_join)
    }
}
