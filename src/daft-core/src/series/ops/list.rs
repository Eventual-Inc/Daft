use crate::datatypes::{DataType, UInt64Array, Utf8Array};
use crate::series::Series;
use common_error::DaftError;

use common_error::DaftResult;

impl Series {
    pub fn explode(&self) -> DaftResult<Series> {
        use DataType::*;
        match self.data_type() {
            List(_) => self.list()?.explode(),
            FixedSizeList(..) => self.fixed_size_list()?.explode(),
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_lengths(&self) -> DaftResult<UInt64Array> {
        use DataType::*;

        match self.data_type() {
            List(_) => self.list()?.lengths(),
            FixedSizeList(..) => self.fixed_size_list()?.lengths(),
            Embedding(..) | FixedShapeImage(..) => self.as_physical()?.list_lengths(),
            Image(..) => {
                let struct_array = self.as_physical()?;
                let data_array = struct_array.struct_()?.children[0].list().unwrap();
                let offsets = data_array.offsets();
                let array = Box::new(
                    arrow2::array::PrimitiveArray::from_vec(
                        offsets.lengths().map(|l| l as u64).collect(),
                    )
                    .with_validity(data_array.validity().cloned()),
                );
                Ok(UInt64Array::from((self.name(), array)))
            }
            dt => Err(DaftError::TypeError(format!(
                "lengths not implemented for {}",
                dt
            ))),
        }
    }

    pub fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        match self.data_type() {
            DataType::List(_) => self.list()?.join(delimiter),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.join(delimiter),
            dt => Err(DaftError::TypeError(format!(
                "Join not implemented for {}",
                dt
            ))),
        }
    }
}
