use arrow2::compute::{
    self,
    cast::{can_cast_types, cast, CastOptions},
};

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftDataType, DaftNumericType, DataType, DateArray, NullArray,
        Utf8Array,
    },
    error::{DaftError, DaftResult},
    series::Series,
};

#[macro_export]
macro_rules! with_match_arrow_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    use $crate::datatypes::*;

    match $key_type {
        Null => __with_ty__! { NullType },
        Boolean => __with_ty__! { BooleanType },
        Binary => __with_ty__! { BinaryType },
        Int8 => __with_ty__! { Int8Type },
        Int16 => __with_ty__! { Int16Type },
        Int32 => __with_ty__! { Int32Type },
        Int64 => __with_ty__! { Int64Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Date => __with_ty__! { DateType },
        Timestamp(_, _) => __with_ty__! { TimestampType },
        List(_) => __with_ty__! { ListType },
        FixedSizeList(..) => __with_ty__! { FixedSizeListType },
        Struct(_) => __with_ty__! { StructType },
        Utf8 => __with_ty__! { Utf8Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

fn arrow_cast<T>(to_cast: &DataArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftDataType + 'static,
{
    if to_cast.data_type().eq(dtype) {
        return Ok(
            DataArray::<T>::try_from((to_cast.name(), to_cast.data().to_boxed()))?.into_series(),
        );
    }

    let _arrow_type = dtype.to_arrow();

    if !dtype.is_arrow() || !to_cast.data_type().is_arrow() {
        return Err(DaftError::TypeError(format!(
            "Can not cast {:?} to type: {:?}: not convertible to Arrow",
            T::get_dtype(),
            dtype
        )));
    }

    let self_arrow_type = to_cast.data_type().to_arrow()?;
    let target_arrow_type = dtype.to_arrow()?;
    if !can_cast_types(&self_arrow_type, &target_arrow_type) {
        return Err(DaftError::TypeError(format!(
            "can not cast {:?} to type: {:?}: Arrow types not castable",
            T::get_dtype(),
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

    Ok(
        with_match_arrow_daft_types!(dtype, |$T| DataArray::<$T>::try_from((to_cast.name(), result_array))?.into_series()),
    )
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
