use common_error::DaftResult;

use crate::{
    datatypes::{self, DaftDataType, DaftLogicalType},
    DataType, Series,
};

use super::ops::as_arrow::AsArrow;

mod arrow_growable;
mod logical_growable;

#[cfg(feature = "python")]
mod python_growable;

/// Describes a struct that can be extended from slices of other pre-existing Series.
/// This is very useful for abstracting many "physical" operations such as takes, broadcasts,
/// filters and more.
pub trait Growable {
    /// Extends this [`Growable`] with elements from the bounded [`Array`] at index `index` from
    /// a slice starting at `start` and length `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`Growable`] with null elements
    fn add_nulls(&mut self, additional: usize);

    /// Builds an array from the [`Growable`]
    fn build(&mut self) -> DaftResult<Series>;
}

type ArrowNullGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, datatypes::NullType, arrow2::array::growable::GrowableNull>;
type ArrowBooleanGrowable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::BooleanType,
    arrow2::array::growable::GrowableBoolean<'a>,
>;
type ArrowInt8Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Int8Type,
    arrow2::array::growable::GrowablePrimitive<'a, i8>,
>;
type ArrowInt16Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Int16Type,
    arrow2::array::growable::GrowablePrimitive<'a, i16>,
>;
type ArrowInt32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Int32Type,
    arrow2::array::growable::GrowablePrimitive<'a, i32>,
>;
type ArrowInt64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Int64Type,
    arrow2::array::growable::GrowablePrimitive<'a, i64>,
>;
type ArrowInt128Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Int128Type,
    arrow2::array::growable::GrowablePrimitive<'a, i128>,
>;
type ArrowUInt8Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::UInt8Type,
    arrow2::array::growable::GrowablePrimitive<'a, u8>,
>;
type ArrowUInt16Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::UInt16Type,
    arrow2::array::growable::GrowablePrimitive<'a, u16>,
>;
type ArrowUInt32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::UInt32Type,
    arrow2::array::growable::GrowablePrimitive<'a, u32>,
>;
type ArrowUInt64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::UInt64Type,
    arrow2::array::growable::GrowablePrimitive<'a, u64>,
>;
// type ArrowFloat16Growable<'a> = ArrowGrowable<'a, datatypes::Float16Type, arrow2::array::growable::GrowablePrimitive::<'a, f16>>;
type ArrowFloat32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Float32Type,
    arrow2::array::growable::GrowablePrimitive<'a, f32>,
>;
type ArrowFloat64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Float64Type,
    arrow2::array::growable::GrowablePrimitive<'a, f64>,
>;
type ArrowBinaryGrowable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::BinaryType,
    arrow2::array::growable::GrowableBinary<'a, i64>,
>;
type ArrowUtf8Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::Utf8Type,
    arrow2::array::growable::GrowableUtf8<'a, i64>,
>;
type ArrowListGrowable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::ListType,
    arrow2::array::growable::GrowableList<'a, i64>,
>;
type ArrowStructGrowable<'a> = arrow_growable::ArrowGrowable<
    'a,
    datatypes::StructType,
    arrow2::array::growable::GrowableStruct<'a>,
>;

/// Helper macro to downcast a series to its underlying concrete arrow array type
macro_rules! series_to_arrow_array {
    ($series:ident, $daft_type:path) => {
        $series
            .iter()
            .map(|s| s.downcast::<<$daft_type as DaftDataType>::ArrayType>().unwrap().as_arrow())
            .collect::<Vec<_>>()
    };
}

pub fn make_growable<'a>(
    name: String,
    dtype: &DataType,
    series: &'a [&'a Series],
    capacity: usize,
) -> Box<dyn Growable + 'a> {
    match dtype {
        // Arrow-backed types
        DataType::Null => Box::new(ArrowNullGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableNull::new(dtype.to_arrow().unwrap()),
        )),
        DataType::Boolean => Box::new(ArrowBooleanGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableBoolean::new(
                series_to_arrow_array!(series, datatypes::BooleanType),
                false,
                capacity,
            ),
        )),
        DataType::Int8 => Box::new(ArrowInt8Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<i8>::new(
                series_to_arrow_array!(series, datatypes::Int8Type),
                false,
                capacity,
            ),
        )),
        DataType::Int16 => Box::new(ArrowInt16Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<i16>::new(
                series_to_arrow_array!(series, datatypes::Int16Type),
                false,
                capacity,
            ),
        )),
        DataType::Int32 => Box::new(ArrowInt32Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<i32>::new(
                series_to_arrow_array!(series, datatypes::Int32Type),
                false,
                capacity,
            ),
        )),
        DataType::Int64 => Box::new(ArrowInt64Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<i64>::new(
                series_to_arrow_array!(series, datatypes::Int64Type),
                false,
                capacity,
            ),
        )),
        DataType::Int128 => Box::new(ArrowInt128Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<i128>::new(
                series_to_arrow_array!(series, datatypes::Int128Type),
                false,
                capacity,
            ),
        )),
        DataType::UInt8 => Box::new(ArrowUInt8Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<u8>::new(
                series_to_arrow_array!(series, datatypes::UInt8Type),
                false,
                capacity,
            ),
        )),
        DataType::UInt16 => Box::new(ArrowUInt16Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<u16>::new(
                series_to_arrow_array!(series, datatypes::UInt16Type),
                false,
                capacity,
            ),
        )),
        DataType::UInt32 => Box::new(ArrowUInt32Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<u32>::new(
                series_to_arrow_array!(series, datatypes::UInt32Type),
                false,
                capacity,
            ),
        )),
        DataType::UInt64 => Box::new(ArrowUInt64Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<u64>::new(
                series_to_arrow_array!(series, datatypes::UInt64Type),
                false,
                capacity,
            ),
        )),
        DataType::Float32 => Box::new(ArrowFloat32Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<f32>::new(
                series_to_arrow_array!(series, datatypes::Float32Type),
                false,
                capacity,
            ),
        )),
        DataType::Float64 => Box::new(ArrowFloat64Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowablePrimitive::<f64>::new(
                series_to_arrow_array!(series, datatypes::Float64Type),
                false,
                capacity,
            ),
        )),
        DataType::Binary => Box::new(ArrowBinaryGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableBinary::<i64>::new(
                series_to_arrow_array!(series, datatypes::BinaryType),
                false,
                capacity,
            ),
        )),
        DataType::Utf8 => Box::new(ArrowUtf8Growable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableUtf8::<i64>::new(
                series_to_arrow_array!(series, datatypes::Utf8Type),
                false,
                capacity,
            ),
        )),
        DataType::List(..) => Box::new(ArrowListGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableList::<i64>::new(
                series_to_arrow_array!(series, datatypes::ListType),
                false,
                capacity,
            ),
        )),
        DataType::Struct(..) => Box::new(ArrowStructGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableStruct::new(
                series_to_arrow_array!(series, datatypes::StructType),
                false,
                capacity,
            ),
        )),

        // Logical types
        DataType::Extension(_, child_dtype, _) => {
            Box::new(arrow_growable::ArrowExtensionGrowable::new(
                name.clone(),
                dtype,
                make_growable(name, child_dtype.as_ref(), series, capacity),
            ))
        }
        DataType::Timestamp(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::TimestampType,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::TimestampType as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),
        DataType::Duration(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::DurationType,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::DurationType as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),
        DataType::Date => Box::new(
            logical_growable::LogicalGrowable::<datatypes::DateType>::new(
                name.clone(),
                dtype,
                make_growable(
                    name,
                    &<datatypes::DateType as DaftLogicalType>::PhysicalType::get_dtype(),
                    series,
                    capacity,
                ),
            ),
        ),
        DataType::Embedding(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::EmbeddingType,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::DurationType as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),
        DataType::FixedShapeImage(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::FixedShapeImageType,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::FixedShapeImageType as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),
        DataType::FixedShapeTensor(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::FixedShapeTensorType,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::FixedShapeTensorType as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),
        DataType::Image(..) => Box::new(
            logical_growable::LogicalGrowable::<datatypes::ImageType>::new(
                name.clone(),
                dtype,
                make_growable(
                    name,
                    &<datatypes::ImageType as DaftLogicalType>::PhysicalType::get_dtype(),
                    series,
                    capacity,
                ),
            ),
        ),
        DataType::Tensor(..) => Box::new(
            logical_growable::LogicalGrowable::<datatypes::TensorType>::new(
                name.clone(),
                dtype,
                make_growable(
                    name,
                    &<datatypes::TensorType as DaftLogicalType>::PhysicalType::get_dtype(),
                    series,
                    capacity,
                ),
            ),
        ),
        DataType::Decimal128(..) => Box::new(logical_growable::LogicalGrowable::<
            datatypes::Decimal128Type,
        >::new(
            name.clone(),
            dtype,
            make_growable(
                name,
                &<datatypes::Decimal128Type as DaftLogicalType>::PhysicalType::get_dtype(),
                series,
                capacity,
            ),
        )),

        #[cfg(feature = "python")]
        DataType::Python => Box::new(python_growable::PythonGrowable::new(
            name, dtype, series, capacity,
        )),

        // Custom arrays
        DataType::FixedSizeList(..) => todo!("Implement growable for FixedSizeList"),
        DataType::Time(..) => unimplemented!("Cannot create growable for Time type"),
        DataType::Unknown => panic!("Cannot create growable for unknown type"),
    }
}
