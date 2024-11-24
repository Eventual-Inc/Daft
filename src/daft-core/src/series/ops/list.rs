use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::{DataType, UInt64Array, Utf8Array},
    prelude::CountMode,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn list_value_counts(&self) -> DaftResult<Self> {
        let series = match self.data_type() {
            DataType::List(_) => self.list()?.value_counts(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.value_counts(),
            dt => {
                return Err(DaftError::TypeError(format!(
                    "List contains not implemented for {}",
                    dt
                )))
            }
        }?
        .into_series();

        Ok(series)
    }

    pub fn explode(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.explode(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.explode(),
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        match self.data_type() {
            DataType::List(_) => self.list()?.count(mode),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.count(mode),
            DataType::Embedding(..) | DataType::FixedShapeImage(..) => {
                self.as_physical()?.list_count(mode)
            }
            DataType::Image(..) => {
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
                "Count not implemented for {}",
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

    pub fn list_get(&self, idx: &Self, default: &Self) -> DaftResult<Self> {
        let idx = idx.cast(&DataType::Int64)?;
        let idx_arr = idx.i64().unwrap();

        match self.data_type() {
            DataType::List(_) => self.list()?.get_children(idx_arr, default),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.get_children(idx_arr, default),
            dt => Err(DaftError::TypeError(format!(
                "Get not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_slice(&self, start: &Self, end: &Self) -> DaftResult<Self> {
        let start = start.cast(&DataType::Int64)?;
        let start_arr = start.i64().unwrap();
        let end_arr = if end.data_type().is_integer() {
            let end = end.cast(&DataType::Int64)?;
            Some(end.i64().unwrap().clone())
        } else {
            None
        };
        match self.data_type() {
            DataType::List(_) => self.list()?.get_slices(start_arr, end_arr.as_ref()),
            DataType::FixedSizeList(..) => self
                .fixed_size_list()?
                .get_slices(start_arr, end_arr.as_ref()),
            dt => Err(DaftError::TypeError(format!(
                "list slice not implemented for {dt}"
            ))),
        }
    }

    pub fn list_chunk(&self, size: usize) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.get_chunks(size),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.get_chunks(size),
            dt => Err(DaftError::TypeError(format!(
                "list chunk not implemented for {dt}"
            ))),
        }
    }

    pub fn list_sum(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.sum(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.sum(),
            dt => Err(DaftError::TypeError(format!(
                "Sum not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_mean(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.mean(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.mean(),
            dt => Err(DaftError::TypeError(format!(
                "Mean not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_min(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.min(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.min(),
            dt => Err(DaftError::TypeError(format!(
                "Min not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_max(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.max(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.max(),
            dt => Err(DaftError::TypeError(format!(
                "Max not implemented for {}",
                dt
            ))),
        }
    }

    pub fn list_sort(&self, desc: &Self, nulls_first: &Self) -> DaftResult<Self> {
        let desc_arr = desc.bool()?;
        let nulls_first = nulls_first.bool()?;

        match self.data_type() {
            DataType::List(_) => Ok(self.list()?.list_sort(desc_arr, nulls_first)?.into_series()),
            DataType::FixedSizeList(..) => Ok(self
                .fixed_size_list()?
                .list_sort(desc_arr, nulls_first)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "List sort not implemented for {}",
                dt
            ))),
        }
    }
}
