use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    array::{
        ops::{as_arrow::AsArrow, from_arrow::FromArrow},
        DataArray,
    },
    datatypes::{
        BinaryType, BooleanType, DaftArrowBackedType, DaftDataType, ExtensionArray, Field,
        Float32Type, Float64Type, Int128Type, Int16Type, Int32Type, Int64Type, Int8Type, NullType,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type, Utf8Type,
    },
    DataType, IntoSeries, Series,
};

use super::Growable;

pub struct ArrowBackedDataArrayGrowable<
    'a,
    T: DaftArrowBackedType,
    G: arrow2::array::growable::Growable<'a>,
> {
    name: String,
    dtype: DataType,
    arrow2_growable: G,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftArrowBackedType, G: arrow2::array::growable::Growable<'a>> Growable
    for ArrowBackedDataArrayGrowable<'a, T, G>
where
    DataArray<T>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.arrow2_growable.extend(index, start, len);
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.arrow2_growable.extend_validity(additional)
    }

    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let arrow_array = self.arrow2_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        Ok(DataArray::<T>::from_arrow(&field, arrow_array)?.into_series())
    }
}

pub type ArrowNullGrowable<'a> =
    ArrowBackedDataArrayGrowable<'a, NullType, arrow2::array::growable::GrowableNull>;

impl<'a> ArrowNullGrowable<'a> {
    pub fn new(name: &str, dtype: &DataType) -> Self {
        let arrow2_growable = arrow2::array::growable::GrowableNull::new(dtype.to_arrow().unwrap());
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            arrow2_growable,
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_arrow_backed_data_array_growable {
    ($growable_name:ident, $daft_type:ty, $arrow2_growable_type:ty) => {
        pub type $growable_name<'a> =
            ArrowBackedDataArrayGrowable<'a, $daft_type, $arrow2_growable_type>;

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
                let arrow2_growable =
                    <$arrow2_growable_type>::new(ref_arrow_arrays, use_validity, capacity);
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    arrow2_growable,
                    _phantom: PhantomData,
                }
            }
        }
    };
}

impl_arrow_backed_data_array_growable!(
    ArrowBooleanGrowable,
    BooleanType,
    arrow2::array::growable::GrowableBoolean<'a>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt8Growable,
    Int8Type,
    arrow2::array::growable::GrowablePrimitive<'a, i8>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt16Growable,
    Int16Type,
    arrow2::array::growable::GrowablePrimitive<'a, i16>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt32Growable,
    Int32Type,
    arrow2::array::growable::GrowablePrimitive<'a, i32>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt64Growable,
    Int64Type,
    arrow2::array::growable::GrowablePrimitive<'a, i64>
);
impl_arrow_backed_data_array_growable!(
    ArrowInt128Growable,
    Int128Type,
    arrow2::array::growable::GrowablePrimitive<'a, i128>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt8Growable,
    UInt8Type,
    arrow2::array::growable::GrowablePrimitive<'a, u8>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt16Growable,
    UInt16Type,
    arrow2::array::growable::GrowablePrimitive<'a, u16>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt32Growable,
    UInt32Type,
    arrow2::array::growable::GrowablePrimitive<'a, u32>
);
impl_arrow_backed_data_array_growable!(
    ArrowUInt64Growable,
    UInt64Type,
    arrow2::array::growable::GrowablePrimitive<'a, u64>
);
impl_arrow_backed_data_array_growable!(
    ArrowFloat32Growable,
    Float32Type,
    arrow2::array::growable::GrowablePrimitive<'a, f32>
);
impl_arrow_backed_data_array_growable!(
    ArrowFloat64Growable,
    Float64Type,
    arrow2::array::growable::GrowablePrimitive<'a, f64>
);
impl_arrow_backed_data_array_growable!(
    ArrowBinaryGrowable,
    BinaryType,
    arrow2::array::growable::GrowableBinary<'a, i64>
);
impl_arrow_backed_data_array_growable!(
    ArrowUtf8Growable,
    Utf8Type,
    arrow2::array::growable::GrowableUtf8<'a, i64>
);

/// ExtensionTypes are slightly different, because they have a dynamic inner type
pub struct ArrowExtensionGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn arrow2::array::growable::Growable<'a> + 'a>,
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
        let child_growable = arrow2::array::growable::make_growable(
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

impl<'a> Growable for ArrowExtensionGrowable<'a> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(index, start, len)
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.child_growable.extend_validity(additional)
    }
    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let arr = self.child_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        Ok(ExtensionArray::from_arrow(&field, arr)?.into_series())
    }
}
