use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{array::ops::DaftCompare, join::JoinType};
use daft_dsl::{join::infer_join_schema, ExprRef};
use daft_io::IOStatsContext;
use daft_stats::TruthValue;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    fn join<F>(
        &self,
        right: &Self,
        io_stats: Arc<IOStatsContext>,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        how: JoinType,
        table_join: F,
    ) -> DaftResult<Self>
    where
        F: FnOnce(&Table, &Table, &[ExprRef], &[ExprRef], JoinType) -> DaftResult<Table>,
    {
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on, how)?;
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
                    let l_eval_stats = l.eval_expression_list(left_on, &self.schema)?;
                    let r_eval_stats = r.eval_expression_list(right_on, &right.schema)?;
                    let mut curr_tv = TruthValue::Maybe;
                    for (lc, rc) in l_eval_stats
                        .columns
                        .values()
                        .zip(r_eval_stats.columns.values())
                    {
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
        let lt = self.concat_or_get(io_stats.clone())?;
        let rt = right.concat_or_get(io_stats)?;

        match (lt.as_slice(), rt.as_slice()) {
            ([], _) | (_, []) => Ok(Self::empty(Some(join_schema))),
            ([lt], [rt]) => {
                let joined_table = table_join(lt, rt, left_on, right_on, how)?;
                Ok(Self::new_loaded(
                    join_schema,
                    vec![joined_table].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn hash_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        null_equals_nulls: Option<Vec<bool>>,
        how: JoinType,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::hash_join");
        let null_equals_nulls = null_equals_nulls.unwrap_or_else(|| vec![false; left_on.len()]);
        let table_join =
            |lt: &Table, rt: &Table, lo: &[ExprRef], ro: &[ExprRef], _how: JoinType| {
                Table::hash_join(lt, rt, lo, ro, null_equals_nulls.as_slice(), _how)
            };

        self.join(right, io_stats, left_on, right_on, how, table_join)
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        is_sorted: bool,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::sort_merge_join");
        let table_join =
            |lt: &Table, rt: &Table, lo: &[ExprRef], ro: &[ExprRef], _how: JoinType| {
                Table::sort_merge_join(lt, rt, lo, ro, is_sorted)
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
}
