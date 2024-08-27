use common_error::DaftResult;

use crate::array::ops::as_arrow::AsArrow;
use crate::array::ops::DaftCountApproxDistinctAggable;
use crate::datatypes::UInt64Array;
use crate::utils::hyperloglog::HyperLogLog;

impl DaftCountApproxDistinctAggable for UInt64Array {
    type Output = DaftResult<UInt64Array>;

    fn approx_count_distinct(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for &value in self.as_arrow().values_iter() {
            hll.add_already_hashed(value);
        }
        todo!()
    }

    fn grouped_approx_count_distinct(&self, _: &super::GroupIndices) -> Self::Output {
        todo!()
    }
}
