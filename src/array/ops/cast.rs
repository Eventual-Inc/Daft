use arrow2::compute::cast::{can_cast_types, cast, CastOptions};

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{DaftDataType, DataType},
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

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        if self.data_type().eq(dtype) {
            return Ok(DataArray::<T>::from(self.data().to_boxed()).into_series());
        }

        let _arrow_type = dtype.to_arrow();

        if !dtype.is_arrow() || !self.data_type().is_arrow() {
            return Err(DaftError::TypeError(format!(
                "can not cast {:?} to type: {:?}",
                T::get_dtype(),
                dtype
            )));
        }

        let self_arrow_type = self.data_type().to_arrow()?;
        let target_arrow_type = dtype.to_arrow()?;
        if !can_cast_types(&self_arrow_type, &target_arrow_type) {
            return Err(DaftError::TypeError(format!(
                "can not cast {:?} to type: {:?}",
                T::get_dtype(),
                dtype
            )));
        }

        let result_array = cast(
            self.data(),
            &target_arrow_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?;

        Ok(
            with_match_arrow_daft_types!(dtype, |$T| DataArray::<$T>::from(result_array).into_series()),
        )
    }
}
