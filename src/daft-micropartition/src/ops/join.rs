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

        let lt = lt.unwrap_or_else(|| RecordBatch::empty(Some(self.schema.clone())));
        let rt = rt.unwrap_or_else(|| RecordBatch::empty(Some(right.schema.clone())));

        let joined_table = table_join(&lt, &rt, left_on, right_on, how)?;
        Ok(Self::new_loaded(
            join_schema,
            vec![joined_table].into(),
            None,
        ))
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

    use daft_core::{
        datatypes::{Field, Int64Array},
        join::JoinType,
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};
    use daft_recordbatch::RecordBatch;

    use crate::MicroPartition;

    fn make_mp(col_name: &str, values: Vec<i64>) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        let array = Int64Array::from_vec(col_name, values);
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None)
    }

    fn make_empty_mp(col_name: &str) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        MicroPartition::empty(Some(schema))
    }

    fn collect_values(mp: &MicroPartition) -> Vec<i64> {
        let rb = mp.concat_or_get().unwrap();
        let mut values = Vec::new();
        if let Some(rb) = rb.as_ref() {
            let col = rb.get_column(0);
            for i in 0..col.len() {
                if let daft_core::lit::Literal::Int64(v) = col.get_lit(i) {
                    values.push(v);
                }
            }
        }
        values
    }

    #[test]
    fn test_join_inner_left_empty() {
        let left = make_empty_mp("a");
        let right = make_mp("a", vec![1, 2, 3]);
        let left_on = vec![BoundExpr::try_new(resolved_col("a"), &left.schema).unwrap()];
        let right_on = vec![BoundExpr::try_new(resolved_col("a"), &right.schema).unwrap()];
        let result = left
            .sort_merge_join(&right, &left_on, &right_on, JoinType::Inner, true)
            .unwrap();
        assert_eq!(result.len(), 0, "inner join: left empty -> empty result");
    }

    #[test]
    fn test_join_inner_right_empty() {
        let left = make_mp("a", vec![1, 2, 3]);
        let right = make_empty_mp("a");
        let left_on = vec![BoundExpr::try_new(resolved_col("a"), &left.schema).unwrap()];
        let right_on = vec![BoundExpr::try_new(resolved_col("a"), &right.schema).unwrap()];
        let result = left
            .sort_merge_join(&right, &left_on, &right_on, JoinType::Inner, true)
            .unwrap();
        assert_eq!(result.len(), 0, "inner join: right empty -> empty result");
    }

    #[test]
    fn test_join_left_right_empty_preserves_left() {
        let left = make_mp("a", vec![1, 2, 3]);
        let right = make_empty_mp("a");
        let left_on = vec![BoundExpr::try_new(resolved_col("a"), &left.schema).unwrap()];
        let right_on = vec![BoundExpr::try_new(resolved_col("a"), &right.schema).unwrap()];
        let result = left
            .sort_merge_join(&right, &left_on, &right_on, JoinType::Left, true)
            .unwrap();
        assert_eq!(
            result.len(),
            3,
            "left join: right empty -> all 3 left rows preserved"
        );
    }

    #[test]
    fn test_sort_merge_join_passes_join_type() {
        let left = make_mp("a", vec![1, 2, 3]);
        let right = make_mp("a", vec![2, 3, 4]);
        let left_on = vec![BoundExpr::try_new(resolved_col("a"), &left.schema).unwrap()];
        let right_on = vec![BoundExpr::try_new(resolved_col("a"), &right.schema).unwrap()];
        let result = left
            .sort_merge_join(&right, &left_on, &right_on, JoinType::Semi, true)
            .unwrap();
        // Semi join: only left rows with match -> [2, 3]
        assert_eq!(result.len(), 2, "semi join should return 2 matching rows");
        let values = collect_values(&result);
        assert_eq!(values, vec![2, 3]);
    }
}
