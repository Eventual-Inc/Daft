use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;
use daft_arrow::types::months_days_ns;

use super::Growable;
use crate::{
    array::prelude::*,
    datatypes::prelude::*,
    series::{IntoSeries, Series},
};

pub struct ArrowBackedDataArrayGrowable<
    'a,
    T: DaftArrowBackedType,
    G: daft_arrow::array::builder::Growable<'a>,
> {
    name: String,
    dtype: DataType,
    arrow_builder: G,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftArrowBackedType, G: daft_arrow::array::builder::Growable<'a>> Growable
    for ArrowBackedDataArrayGrowable<'a, T, G>
where
    DataArray<T>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.arrow_builder.extend(index, start, len);
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.arrow_builder.extend_validity(additional);
    }

    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let arrow_array = self.arrow_builder.as_box();
        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        Ok(DataArray::<T>::from_arrow(field, arrow_array)?.into_series())
    }
}

pub type ArrowNullGrowable<'a> =
    ArrowBackedDataArrayGrowable<'a, NullType, daft_arrow::array::builder::NullBuilder>;

impl ArrowNullGrowable<'_> {
    pub fn new(name: &str) -> Self {
        let arrow_builder =
            daft_arrow::array::builder::NullBuilder::new();
        Self {
            name: name.to_string(),
            dtype: DataType::Null,
            arrow_builder,
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_arrow_backed_data_array_growable {
    ($growable_name:ident, $daft_type:ty, $arrow_builder_type:ty) => {
        pub type $growable_name<'a> =
            ArrowBackedDataArrayGrowable<'a, $daft_type, $arrow_builder_type>;

        impl<'a> $growable_name<'a> {
            pub fn new(
                name: &str,
                dtype: &DataType,
                arrays: Vec<&'a <$daft_type as DaftDataType>::ArrayType>,
                use_validity: bool,
                capacity: usize,
            ) -> Self {
                let ref_arrays = arrays.to_vec();
                let ref_arrow_arrays = ref_arrays.iter().map(|&a| a.as_arrow()).collect::<Vec<_>>();
                let arrow_builder =
                    <$arrow_builder_type>::with_capacity(capacity);
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    arrow_builder,
                    _phantom: PhantomData,
                }
            }
        }
    };
}

impl_arrow_backed_data_array_growable!(
    ArrowBooleanGrowable,
    BooleanType,
    daft_arrow::array::builder::BooleanBuilder
);
impl_arrow_backed_data_array_growable!(
    ArrowInt8Growable,
    Int8Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, i8>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt16Growable,
    Int16Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, i16>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt32Growable,
    Int32Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, i32>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt64Growable,
    Int64Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, i64>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt8Growable,
    UInt8Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, u8>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt16Growable,
    UInt16Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, u16>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt32Growable,
    UInt32Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, u32>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt64Growable,
    UInt64Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, u64>
);
impl_arrow_backed_data_array_growable!(
    ArrowFloat32Growable,
    Float32Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, f32>
);
impl_arrow_backed_data_array_growable!(
    ArrowFloat64Growable,
    Float64Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, f64>
);
impl_arrow_backed_data_array_growable!(
    ArrowBinaryGrowable,
    BinaryType,
    daft_arrow::array::builder::GrowableBinary<'a, i64>
);
impl_arrow_backed_data_array_growable!(
    ArrowFixedSizeBinaryGrowable,
    FixedSizeBinaryType,
    daft_arrow::array::builder::GrowableFixedSizeBinary<'a>
);
impl_arrow_backed_data_array_growable!(
    ArrowUtf8Growable,
    Utf8Type,
    daft_arrow::array::builder::GrowableUtf8<'a, i64>
);
impl_arrow_backed_data_array_growable!(
    ArrowMonthDayNanoIntervalGrowable,
    IntervalType,
    daft_arrow::array::builder::GrowablePrimitive<'a, months_days_ns>
);

impl_arrow_backed_data_array_growable!(
    ArrowDecimal128Growable,
    Decimal128Type,
    daft_arrow::array::builder::GrowablePrimitive<'a, i128>
);

/// ExtensionTypes are slightly different, because they have a dynamic inner type
pub struct ArrowExtensionGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn daft_arrow::array::builder::Growable<'a> + 'a>,
}

impl<'a> ArrowExtensionGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a ExtensionArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        assert!(matches!(dtype, DataType::Extension(..)));
        let child_ref_arrays = arrays.iter().map(|&a| a.data()).collect::<Vec<_>>();
        let child_growable = daft_arrow::array::builder::make_growable(
            child_ref_arrays.as_slice(),
            use_validity,
            capacity,
        );
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            child_growable,
        }
    }
}

impl Growable for ArrowExtensionGrowable<'_> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(index, start, len);
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.child_growable.extend_validity(additional);
    }
    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let arr = self.child_growable.as_box();
        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        Ok(ExtensionArray::from_arrow(field, arr)?.into_series())
    }
}
