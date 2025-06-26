use std::sync::Arc;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use super::{DaftCountAggable, DaftSumAggable};
use crate::{
    array::ops::{DaftMeanAggable, GroupIndices},
    datatypes::*,
    prelude::CountMode,
    utils::stats,
};

impl DaftMeanAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn mean(&self) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let data = PrimitiveArray::from([stats.mean]).boxed();
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        Self::new(field, data)
    }

    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_means = stats::grouped_stats(self, groups)?.map(|(stats, _)| stats.mean);
        let data = Box::new(PrimitiveArray::from_iter(grouped_means));
        Ok(Self::from((self.field.name.as_ref(), data)))
    }
}

impl DataArray<Decimal128Type> {
    pub fn merge_mean(&self, counts: &DataArray<UInt64Type>) -> DaftResult<Self> {
        assert_eq!(self.len(), counts.len());
        let means = self
            .into_iter()
            .zip(counts)
            .map(|(sum, count)| sum.zip(count).map(|(s, c)| s / (*c as i128)));
        Ok(Self::from_iter(self.field.clone(), means))
    }
}

impl DaftMeanAggable for DataArray<Decimal128Type> {
    type Output = DaftResult<Self>;

    fn mean(&self) -> Self::Output {
        let count = self.count(CountMode::Valid)?.get(0);
        let sum = self.sum()?.get(0);

        let val = sum.zip(count).map(|(s, c)| s / (c as i128));

        Ok(Self::from_iter(self.field.clone(), std::iter::once(val)))
    }

    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_sum = self.grouped_sum(groups)?;
        let grouped_count = self.grouped_count(groups, CountMode::Valid)?;
        grouped_sum.merge_mean(&grouped_count)
    }
}
