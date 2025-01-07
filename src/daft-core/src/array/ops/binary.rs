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
}
