use common_error::DaftResult;
use daft_core::array::ops::DaftCompare;
use daft_dsl::Expr;
use daft_io::IOStatsContext;
use daft_table::infer_join_schema;

use crate::micropartition::MicroPartition;

use daft_stats::TruthValue;

impl MicroPartition {
    pub fn join(&self, right: &Self, left_on: &[Expr], right_on: &[Expr]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::join");
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on)?;

        if self.len() == 0 || right.len() == 0 {
            return Ok(Self::empty(Some(join_schema.into())));
        }

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
                    if let TruthValue::False = lc.equal(rc)?.to_truth_value() {
                        curr_tv = TruthValue::False;
                        break;
                    }
                }
                curr_tv
            }
        };
        if let TruthValue::False = tv {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        let lt = self.concat_or_get(io_stats.clone())?;
        let rt = right.concat_or_get(io_stats)?;

        match (lt.as_slice(), rt.as_slice()) {
            ([], _) | (_, []) => Ok(Self::empty(Some(join_schema.into()))),
            ([lt], [rt]) => {
                let joined_table = lt.join(rt, left_on, right_on)?;
                Ok(MicroPartition::new_loaded(
                    join_schema.into(),
                    vec![joined_table].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
