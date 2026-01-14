use common_error::DaftResult;
use daft_core::{
    array::ops::DaftCompare,
    join::{JoinSide, JoinType},
};
use daft_dsl::{expr::bound_expr::BoundExpr, join::infer_join_schema};
use daft_recordbatch::RecordBatch;
use daft_stats::TruthValue;

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
            (None, _) => Ok(Self::empty(Some(join_schema))),
            (Some(lt), None) => {
                match how {
                    // Anti-join with empty right side should return all left rows
                    JoinType::Anti => Ok(Self::new_loaded(join_schema, vec![lt].into(), None)),
                    // Semi-join with empty right side yields empty
                    JoinType::Semi => Ok(Self::empty(Some(join_schema))),
                    // Other join types with empty right side produce empty output here
                    _ => Ok(Self::empty(Some(join_schema))),
                }
            }
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
                          how: JoinType| {
            RecordBatch::sort_merge_join(lt, rt, lo, ro, how, is_sorted)
        };

        self.join(right, left_on, right_on, how, table_join)
    }

    pub fn cross_join(&self, right: &Self, outer_loop_side: JoinSide) -> DaftResult<Self> {
        let table_join =
            |lt: &RecordBatch, rt: &RecordBatch, _: &[BoundExpr], _: &[BoundExpr], _: JoinType| {
                RecordBatch::cross_join(lt, rt, outer_loop_side)
            };

        self.join(right, &[], &[], JoinType::Inner, table_join)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{
        datatypes::{DataType, Field, Int32Array},
        join::JoinType,
        prelude::Schema,
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};

    use super::MicroPartition;

    fn make_left_mp() -> (MicroPartition, Arc<Schema>) {
        let key = Int32Array::from_values("key", vec![1, 2, 3].into_iter()).into_series();
        let v = Int32Array::from_values("v", vec![10, 20, 30].into_iter()).into_series();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32),
            Field::new("v", DataType::Int32),
        ]));
        let table = daft_recordbatch::RecordBatch::from_nonempty_columns(vec![key, v]).unwrap();
        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![table.clone()]), None);
        (mp, schema)
    }

    fn make_right_empty(schema: Arc<Schema>) -> MicroPartition {
        MicroPartition::empty(Some(schema))
    }

    fn join_keys(left_schema: &Schema, right_schema: &Schema) -> (Vec<BoundExpr>, Vec<BoundExpr>) {
        let left_on = BoundExpr::bind_all(&[resolved_col("key")], left_schema).unwrap();
        let right_on = BoundExpr::bind_all(&[resolved_col("key")], right_schema).unwrap();
        (left_on, right_on)
    }

    #[test]
    fn hash_join_anti_with_empty_right_returns_all_left() -> DaftResult<()> {
        let (left_mp, left_schema) = make_left_mp();
        let right_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32)]));
        let right_mp = make_right_empty(right_schema.clone());
        let (left_on, right_on) = join_keys(left_schema.as_ref(), right_schema.as_ref());

        let out = left_mp.hash_join(&right_mp, &left_on, &right_on, None, JoinType::Anti)?;
        assert_eq!(out.len(), 3);
        assert_eq!(out.record_batches().len(), 1);
        assert_eq!(left_mp.record_batches().len(), 1);
        assert_eq!(out.record_batches()[0], left_mp.record_batches()[0]);
        Ok(())
    }

    #[test]
    fn hash_join_semi_with_empty_right_returns_empty() -> DaftResult<()> {
        let (left_mp, left_schema) = make_left_mp();
        let right_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32)]));
        let right_mp = make_right_empty(right_schema.clone());
        let (left_on, right_on) = join_keys(left_schema.as_ref(), right_schema.as_ref());

        let out = left_mp.hash_join(&right_mp, &left_on, &right_on, None, JoinType::Semi)?;
        assert_eq!(out.len(), 0);
        assert_eq!(out.record_batches().len(), 0);
        Ok(())
    }

    #[test]
    fn hash_join_left_with_empty_right_returns_empty() -> DaftResult<()> {
        let (left_mp, left_schema) = make_left_mp();
        let right_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32)]));
        let right_mp = make_right_empty(right_schema.clone());
        let (left_on, right_on) = join_keys(left_schema.as_ref(), right_schema.as_ref());

        let out = left_mp.hash_join(&right_mp, &left_on, &right_on, None, JoinType::Left)?;
        assert_eq!(out.len(), 0);
        assert_eq!(out.record_batches().len(), 0);
        Ok(())
    }
}
