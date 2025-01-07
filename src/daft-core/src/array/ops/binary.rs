use common_error::DaftResult;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, UInt64Array},
};

impl BinaryArray {
    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.len() as u64)
            })
            .collect::<arrow2::array::UInt64Array>()
            .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();

        let arrow_result = self_arrow
            .iter()
            .zip(other_arrow.iter())
            .map(|(left_val, right_val)| {
                match (left_val, right_val) {
                    (Some(left), Some(right)) => Some([left, right].concat()),
                    _ => None,
                }
            })
            .collect::<arrow2::array::BinaryArray<i64>>()
            .with_validity(self_arrow.validity().zip(other_arrow.validity()).map(|(a, b)| a & b));

        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }
}
