use std::sync::Arc;

use daft_common::error::DaftResult;
use daft_core::{
    array::ops::DaftCompare,
    join::{JoinSide, JoinType},
};
use daft_dsl::{
    Expr,
    expr::bound_expr::BoundExpr,
    join::{get_right_cols_to_drop, infer_asof_join_schema, infer_join_schema},
};
use daft_recordbatch::{RecordBatch, build_left_to_right_map};
use daft_recordbatch::stats::TruthValue;
use rayon::prelude::*;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    fn join<F>(
        &self,
        right: &Self,
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
        let lt = self.concat_or_get()?;
        let rt = right.concat_or_get()?;

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
        let null_equals_nulls = null_equals_nulls.unwrap_or_else(|| vec![false; left_on.len()]);

        let table_join = |lt: &RecordBatch,
                          rt: &RecordBatch,
                          lo: &[BoundExpr],
                          ro: &[BoundExpr],
                          _how: JoinType| {
            RecordBatch::hash_join(lt, rt, lo, ro, null_equals_nulls.as_slice(), _how)
        };

        self.join(right, left_on, right_on, how, table_join)
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[BoundExpr],
        right_on: &[BoundExpr],
        how: JoinType,
        is_sorted: bool,
    ) -> DaftResult<Self> {
        let table_join = |lt: &RecordBatch,
                          rt: &RecordBatch,
                          lo: &[BoundExpr],
                          ro: &[BoundExpr],
                          _how: JoinType| {
            RecordBatch::sort_merge_join(lt, rt, lo, ro, is_sorted)
        };

        self.join(right, left_on, right_on, how, table_join)
    }

    pub fn asof_join(
        &self,
        right: &Self,
        left_by: &[BoundExpr],
        right_by: &[BoundExpr],
        left_on: &BoundExpr,
        right_on: &BoundExpr,
    ) -> DaftResult<Self> {
        let right_cols_to_drop = get_right_cols_to_drop(right_by, left_on, right_on, |e| {
            match e.inner().unwrap_alias().0.as_ref() {
                Expr::Column(_) => Some(e.inner().unwrap_alias().0.name().to_string()),
                _ => None,
            }
        });
        let join_schema = infer_asof_join_schema(&self.schema, &right.schema, &right_cols_to_drop)?;
        if self.is_empty() {
            return Ok(Self::empty(Some(join_schema)));
        }

        let lt = self.concat_or_get()?;
        let rt = right.concat_or_get()?;

        let Some(lt) = lt else {
            return Ok(Self::empty(Some(join_schema)));
        };
        let rt = match rt {
            Some(rt) => rt,
            None => RecordBatch::empty(Some(right.schema())),
        };

        if left_by.is_empty() {
            let joined = RecordBatch::asof_join(&lt, &rt, &right_cols_to_drop, left_on, right_on)?;
            return Ok(Self::new_loaded(join_schema, Arc::new(vec![joined]), None));
        }

        let (left_groups, left_keys) = lt.partition_by_value(left_by)?;
        let (right_groups, right_keys) = rt.partition_by_value(right_by)?;

        let left_to_right = build_left_to_right_map(&left_keys, &right_keys)?;

        let empty_right = RecordBatch::empty(Some(rt.schema));
        let batches = left_groups
            .par_iter()
            .enumerate()
            .map(|(i, left_group)| {
                let right_group = match left_to_right[i] {
                    Some(r_idx) => &right_groups[r_idx],
                    None => &empty_right,
                };
                RecordBatch::asof_join(
                    left_group,
                    right_group,
                    &right_cols_to_drop,
                    left_on,
                    right_on,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Self::new_loaded(join_schema, Arc::new(batches), None))
    }

    pub fn cross_join(&self, right: &Self, outer_loop_side: JoinSide) -> DaftResult<Self> {
        let table_join =
            |lt: &RecordBatch, rt: &RecordBatch, _: &[BoundExpr], _: &[BoundExpr], _: JoinType| {
                RecordBatch::cross_join(lt, rt, outer_loop_side)
            };

        self.join(right, &[], &[], JoinType::Inner, table_join)
    }
}
