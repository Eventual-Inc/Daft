use arrow2::compute::{
    self,
    cast::{can_cast_types, cast, CastOptions},
};

use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftDataType, DaftNumericType, DataType, DateArray,
        FixedSizeListArray, ListArray, NullArray, PythonArray, StructArray, Utf8Array,
    },
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::series::IntoSeries;

use super::downcast::Downcastable;

fn arrow_cast<T>(to_cast: &DataArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftDataType + 'static,
{
    if to_cast.data_type().eq(dtype) {
        return Series::try_from((to_cast.name(), to_cast.data().to_boxed()));
    }

    let _arrow_type = dtype.to_arrow();

    if !dtype.is_arrow() || !to_cast.data_type().is_arrow() {
        return Err(DaftError::TypeError(format!(
            "Can not cast {:?} to type: {:?}: not convertible to Arrow",
            to_cast.data_type(),
            dtype
        )));
    }

    let self_arrow_type = to_cast.data_type().to_arrow()?;
    let target_arrow_type = dtype.to_arrow()?;
    if !can_cast_types(&self_arrow_type, &target_arrow_type) {
        return Err(DaftError::TypeError(format!(
            "can not cast {:?} to type: {:?}: Arrow types not castable",
            to_cast.data_type(),
            dtype
        )));
    }

    let result_array = cast(
        to_cast.data(),
        &target_arrow_type,
        CastOptions {
            wrapped: true,
            partial: false,
        },
    )?;
    Series::try_from((to_cast.name(), result_array))
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl Utf8Array {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl BooleanArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl NullArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl BinaryArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl DateArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        // We need to handle casts that Arrow doesn't allow, but our type-system does
        match dtype {
            DataType::Utf8 => {
                // TODO: we should move this into our own strftime kernel
                let date_array = self.downcast();
                let year_array = compute::temporal::year(date_array)?;
                let month_array = compute::temporal::month(date_array)?;
                let day_array = compute::temporal::day(date_array)?;
                let date_str: arrow2::array::Utf8Array<i64> = year_array
                    .iter()
                    .zip(month_array.iter())
                    .zip(day_array.iter())
                    .map(|((y, m), d)| match (y, m, d) {
                        (None, _, _) | (_, None, _) | (_, _, None) => None,
                        (Some(y), Some(m), Some(d)) => Some(format!("{y}-{m}-{d}")),
                    })
                    .collect();
                Ok(Utf8Array::from((self.name(), Box::new(date_str))).into_series())
            }
            DataType::Float32 => self.cast(&DataType::Int32)?.cast(&DataType::Float32),
            DataType::Float64 => self.cast(&DataType::Int32)?.cast(&DataType::Float64),
            _ => arrow_cast(self, dtype),
        }
    }
}

impl ListArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl FixedSizeListArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl StructArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        arrow_cast(self, dtype)
    }
}

impl PythonArray {
    pub fn cast(&self, _dtype: &DataType) -> DaftResult<Series> {
        todo!("Move python casting logic to here")
    }
}
