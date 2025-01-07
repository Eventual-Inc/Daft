use common_error::DaftResult;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, UInt64Array},
};

impl BinaryArray {
    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let offsets = self_arrow.offsets();
        let arrow_result = arrow2::array::UInt64Array::from_iter(
            offsets.windows(2).map(|w| Some((w[1] - w[0]) as u64))
        ).with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();

        let arrow_result = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            // Handle broadcasting case
            let (longer_arr, shorter_arr) = if self_arrow.len() > other_arrow.len() {
                (self_arrow, other_arrow)
            } else {
                (other_arrow, self_arrow)
            };
            let shorter_val = shorter_arr.value(0);
            longer_arr
                .iter()
                .map(|val| match val {
                    Some(val) => Some([val, shorter_val].concat()),
                    None => None,
                })
                .collect::<arrow2::array::BinaryArray<i64>>()
        } else {
            // Regular case - element-wise concatenation
            self_arrow
                .iter()
                .zip(other_arrow.iter())
                .map(|(left_val, right_val)| {
                    match (left_val, right_val) {
                        (Some(left), Some(right)) => Some([left, right].concat()),
                        _ => None,
                    }
                })
                .collect::<arrow2::array::BinaryArray<i64>>()
        };

        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }
}
