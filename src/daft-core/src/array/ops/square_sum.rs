use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::array::{
    ops::{DaftSquareSumAggable, GroupIndices},
    prelude::Float64Array,
};

impl DaftSquareSumAggable for Float64Array {
    type Output = DaftResult<Self>;

    fn square_sum(&self) -> Self::Output {
        let sum_square = self
            .into_iter()
            .flatten()
            .copied()
            .fold(0., |acc, value| acc + value.powi(2));
        let data = PrimitiveArray::from([Some(sum_square)]).boxed();
        let field = self.field.clone();
        Self::new(field, data)
    }

    fn grouped_square_sum(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_square_sum_iter = groups
            .iter()
            .map(|group| {
                group.iter().copied().fold(0., |acc, index| {
                    self.get(index as _)
                        .map_or(acc, |value| acc + value.powi(2))
                })
            })
            .map(Some);
        let data = PrimitiveArray::from_trusted_len_iter(grouped_square_sum_iter).boxed();
        let field = self.field.clone();
        Self::new(field, data)
    }
}
