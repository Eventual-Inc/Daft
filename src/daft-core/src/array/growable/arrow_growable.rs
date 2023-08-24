use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    array::{ops::from_arrow::FromArrow, DataArray},
    datatypes::{
        BinaryType, BooleanType, DaftArrowBackedType, DaftDataType, ExtensionArray, ExtensionType,
        Field, FixedSizeListType, Float32Type, Float64Type, Int128Type, Int16Type, Int32Type,
        Int64Type, Int8Type, ListType, NullType, StructType, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type, Utf8Type,
    },
    DataType, IntoSeries,
};

use super::Growable;

pub struct ArrowGrowable<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    name: String,
    dtype: DataType,
    arrow2_growable: G,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>> ArrowGrowable<'a, T, G>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    pub fn new(name: String, dtype: &DataType, arrow2_growable: G) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            arrow2_growable,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>> Growable<DataArray<T>>
    for ArrowGrowable<'a, T, G>
where
    T: DaftArrowBackedType,
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
    fn build(&mut self) -> DaftResult<DataArray<T>> {
        let arrow_array = self.arrow2_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        DataArray::<T>::from_arrow(&field, arrow_array)
    }
}

pub struct ArrowExtensionGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn arrow2::array::growable::Growable<'a> + 'a>,
}

impl<'a> ArrowExtensionGrowable<'a> {
    pub fn new(
        name: String,
        dtype: &DataType,
        child_growable: Box<dyn arrow2::array::growable::Growable<'a> + 'a>,
    ) -> Self {
        assert!(matches!(dtype, DataType::Extension(..)));
        Self {
            name,
            dtype: dtype.clone(),
            child_growable,
        }
    }
}

impl<'a> Growable<DataArray<ExtensionType>> for ArrowExtensionGrowable<'a> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(index, start, len)
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.child_growable.extend_validity(additional)
    }
    #[inline]
    fn build(&mut self) -> DaftResult<DataArray<ExtensionType>> {
        let arr = self.child_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        ExtensionArray::from_arrow(&field, arr)
    }
}

pub type ArrowNullGrowable<'a> = ArrowGrowable<'a, NullType, arrow2::array::growable::GrowableNull>;
pub type ArrowBooleanGrowable<'a> =
    ArrowGrowable<'a, BooleanType, arrow2::array::growable::GrowableBoolean<'a>>;
pub type ArrowInt8Growable<'a> =
    ArrowGrowable<'a, Int8Type, arrow2::array::growable::GrowablePrimitive<'a, i8>>;
pub type ArrowInt16Growable<'a> =
    ArrowGrowable<'a, Int16Type, arrow2::array::growable::GrowablePrimitive<'a, i16>>;
pub type ArrowInt32Growable<'a> =
    ArrowGrowable<'a, Int32Type, arrow2::array::growable::GrowablePrimitive<'a, i32>>;
pub type ArrowInt64Growable<'a> =
    ArrowGrowable<'a, Int64Type, arrow2::array::growable::GrowablePrimitive<'a, i64>>;
pub type ArrowInt128Growable<'a> =
    ArrowGrowable<'a, Int128Type, arrow2::array::growable::GrowablePrimitive<'a, i128>>;
pub type ArrowUInt8Growable<'a> =
    ArrowGrowable<'a, UInt8Type, arrow2::array::growable::GrowablePrimitive<'a, u8>>;
pub type ArrowUInt16Growable<'a> =
    ArrowGrowable<'a, UInt16Type, arrow2::array::growable::GrowablePrimitive<'a, u16>>;
pub type ArrowUInt32Growable<'a> =
    ArrowGrowable<'a, UInt32Type, arrow2::array::growable::GrowablePrimitive<'a, u32>>;
pub type ArrowUInt64Growable<'a> =
    ArrowGrowable<'a, UInt64Type, arrow2::array::growable::GrowablePrimitive<'a, u64>>;
pub type ArrowFloat32Growable<'a> =
    ArrowGrowable<'a, Float32Type, arrow2::array::growable::GrowablePrimitive<'a, f32>>;
pub type ArrowFloat64Growable<'a> =
    ArrowGrowable<'a, Float64Type, arrow2::array::growable::GrowablePrimitive<'a, f64>>;
pub type ArrowBinaryGrowable<'a> =
    ArrowGrowable<'a, BinaryType, arrow2::array::growable::GrowableBinary<'a, i64>>;
pub type ArrowUtf8Growable<'a> =
    ArrowGrowable<'a, Utf8Type, arrow2::array::growable::GrowableUtf8<'a, i64>>;
pub type ArrowListGrowable<'a> =
    ArrowGrowable<'a, ListType, arrow2::array::growable::GrowableList<'a, i64>>;
pub type ArrowFixedSizeListGrowable<'a> =
    ArrowGrowable<'a, FixedSizeListType, arrow2::array::growable::GrowableFixedSizeList<'a>>;
pub type ArrowStructGrowable<'a> =
    ArrowGrowable<'a, StructType, arrow2::array::growable::GrowableStruct<'a>>;
