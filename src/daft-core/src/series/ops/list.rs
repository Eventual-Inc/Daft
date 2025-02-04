use std::sync::Arc;

use arrow2::offset::OffsetsBuffer;
use common_error::{DaftError, DaftResult};
use daft_schema::field::Field;

use crate::{
    array::{growable::make_growable, ListArray},
    datatypes::{DataType, UInt64Array, Utf8Array},
    prelude::{CountMode, Int64Array},
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

    /// Given a series of `List` or `FixedSizeList`, return the count of distinct elements in the list.
    ///
    /// # Note
    /// `NULL` values are not counted.
    ///
    /// # Example
    /// ```txt
    /// [[1, 2, 3], [1, 1, 1], [NULL, NULL, 5]] -> [3, 1, 1]
    /// ```
    pub fn list_count_distinct(&self) -> DaftResult<Self> {
        let field = Field::new(self.name(), DataType::UInt64);
        match self.data_type() {
            DataType::List(..) => {
                let iter = self.list()?.into_iter().map(|sub_series| {
                    let sub_series = sub_series?;
                    let length = sub_series
                        .build_probe_table_without_nulls()
                        .expect("Building the probe table should always work")
                        .len() as u64;
                    Some(length)
                });
                Ok(UInt64Array::from_regular_iter(field, iter)?.into_series())
            }
            DataType::FixedSizeList(..) => {
                let iter = self.fixed_size_list()?.into_iter().map(|sub_series| {
                    let sub_series = sub_series?;
                    let length = sub_series
                        .build_probe_table_without_nulls()
                        .expect("Building the probe table should always work")
                        .len() as u64;
                    Some(length)
                });
                Ok(UInt64Array::from_regular_iter(field, iter)?.into_series())
            }
            _ => Err(DaftError::TypeError(format!(
                "List count distinct not implemented for {}",
                self.data_type()
            ))),
        }
    }

    /// Given a series of data T, repeat each data T with num times to create a list, returns
    /// a series of repeated list.
    /// # Example
    /// ```txt
    /// repeat([1, 2, 3], [2, 0, 1]) --> [[1, 1], [], [3]]
    /// ```
    pub fn list_fill(&self, num: &Int64Array) -> DaftResult<Self> {
        ListArray::list_fill(self, num).map(|arr| arr.into_series())
    }

    /// Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.
    ///
    /// # Example
    /// ```txt
    /// [[1, 2, 3], [1, 1, 1], [NULL, NULL, 5]] -> [[1, 2, 3], [1], [5]]
    /// ```
    pub fn list_distinct(&self) -> DaftResult<Self> {
        let input = if let DataType::FixedSizeList(inner_type, _) = self.data_type() {
            self.cast(&DataType::List(inner_type.clone()))?
        } else {
            self.clone()
        };

        let list = input.list()?;
        let mut offsets = Vec::new();
        offsets.push(0i64);
        let mut current_offset = 0i64;
        let mut result = Vec::new();

        for sub_series in list {
            if let Some(sub_series) = sub_series {
                let probe_table = sub_series.build_probe_table_without_nulls()?;

                let mut indices: Vec<_> = probe_table.keys().map(|k| k.idx).collect();
                indices.sort_unstable();

                let mut unique_values = Vec::new();
                for idx in indices {
                    unique_values.push(sub_series.slice(idx as usize, (idx + 1) as usize)?);
                }

                current_offset += unique_values.len() as i64;
                offsets.push(current_offset);
                result.extend(unique_values);
            } else {
                offsets.push(current_offset);
            }
        }

        let field = Arc::new(input.field().to_exploded_field()?);
        let child_data_type = if let DataType::List(inner_type) = input.data_type() {
            inner_type.as_ref().clone()
        } else {
            return Err(DaftError::TypeError("Expected list type".into()));
        };

        if current_offset == 0 {
            let empty_array = arrow2::array::new_empty_array(child_data_type.to_arrow()?);
            let list_array = ListArray::new(
                Arc::new(Field::new(input.name(), input.data_type().clone())),
                Self::from_arrow(field, empty_array)?,
                OffsetsBuffer::try_from(offsets)?,
                input.validity().cloned(),
            );
            return Ok(list_array.into_series());
        }

        let result_refs: Vec<&Self> = result.iter().collect();
        let mut growable = make_growable(
            &field.name,
            &child_data_type,
            result_refs,
            false,
            current_offset as usize,
        );

        for (i, series) in result.iter().enumerate() {
            growable.extend(i, 0, series.len());
        }

        let list_array = ListArray::new(
            Arc::new(Field::new(input.name(), input.data_type().clone())),
            growable.build()?,
            OffsetsBuffer::try_from(offsets)?,
            input.validity().cloned(),
        );

        Ok(list_array.into_series())
    }
}
