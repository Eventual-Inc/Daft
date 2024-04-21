use std::{marker::PhantomData, sync::Arc};

use crate::{
    array::{ListArray, StructArray},
    datatypes::{DaftLogicalType, DateType, Field},
    with_match_daft_logical_primitive_types,
};
use common_error::DaftResult;

use super::{
    DaftArrayType, DaftDataType, DataArray, DataType, Decimal128Type, DurationType, EmbeddingType,
    FixedShapeImageType, FixedShapeTensorType, FixedSizeListArray, ImageType, MapType, TensorType,
    TimeType, TimestampType,
};

/// A LogicalArray is a wrapper on top of some underlying array, applying the semantic meaning of its
/// field.datatype() to the underlying array.
#[derive(Clone, Debug)]
pub struct LogicalArrayImpl<L: DaftLogicalType, PhysicalArray: DaftArrayType> {
    pub field: Arc<Field>,
    pub physical: PhysicalArray,
    marker_: PhantomData<L>,
}

impl<L: DaftLogicalType, W: DaftArrayType> DaftArrayType for LogicalArrayImpl<L, W> {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}

impl<L: DaftLogicalType, P: DaftArrayType> LogicalArrayImpl<L, P> {
    pub fn new<F: Into<Arc<Field>>>(field: F, physical: P) -> Self {
        let field = field.into();
        assert!(
            field.dtype.is_logical(),
            "Can only construct Logical Arrays on Logical Types, got {}",
            field.dtype
        );
        assert_eq!(
            physical.data_type(),
            &field.dtype.to_physical(),
            "Logical field {} expected {} for Physical Array, got {}",
            &field,
            &field.dtype.to_physical(),
            physical.data_type()
        );
        LogicalArrayImpl {
            physical,
            field,
            marker_: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.field.name.as_ref()
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

macro_rules! impl_logical_type {
    ($physical_array_type:ident) => {
        // Clippy triggers false positives here for the MapArray implementation
        // This is added to suppress the warning
        #[allow(clippy::len_without_is_empty)]
        pub fn len(&self) -> usize {
            self.physical.len()
        }

        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }

        pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
            if arrays.is_empty() {
                return Err(common_error::DaftError::ValueError(
                    "Need at least 1 logical array to concat".to_string(),
                ));
            }
            let physicals: Vec<_> = arrays.iter().map(|a| &a.physical).collect();
            let concatd = $physical_array_type::concat(physicals.as_slice())?;
            Ok(Self::new(arrays.first().unwrap().field.clone(), concatd))
        }
    };
}

/// Implementation for a LogicalArray that wraps a DataArray
impl<L: DaftLogicalType> LogicalArrayImpl<L, DataArray<L::PhysicalType>> {
    impl_logical_type!(DataArray);

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let daft_type = self.data_type();
        let arrow_logical_type = daft_type.to_arrow().unwrap();
        let physical_arrow_array = self.physical.data();
        use crate::datatypes::DataType::*;
        match daft_type {
            // For wrapped primitive types, switch the datatype label on the arrow2 Array.
            Decimal128(..) | Date | Timestamp(..) | Duration(..) | Time(..) => {
                with_match_daft_logical_primitive_types!(daft_type, |$P| {
                    use arrow2::array::Array;
                    physical_arrow_array
                        .as_any()
                        .downcast_ref::<arrow2::array::PrimitiveArray<$P>>()
                        .unwrap()
                        .clone()
                        .to(arrow_logical_type)
                        .to_boxed()
                })
            }
            // Otherwise, use arrow cast to make sure the result arrow2 array is of the correct type.
            _ => arrow2::compute::cast::cast(
                physical_arrow_array,
                &arrow_logical_type,
                arrow2::compute::cast::CastOptions {
                    wrapped: true,
                    partial: false,
                },
            )
            .unwrap(),
        }
    }
}

/// Implementation for a LogicalArray that wraps a FixedSizeListArray
impl<L: DaftLogicalType> LogicalArrayImpl<L, FixedSizeListArray> {
    impl_logical_type!(FixedSizeListArray);

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let mut fixed_size_list_arrow_array = self.physical.to_arrow();
        let arrow_logical_type = self.data_type().to_arrow().unwrap();
        fixed_size_list_arrow_array.change_type(arrow_logical_type);
        fixed_size_list_arrow_array
    }
}

/// Implementation for a LogicalArray that wraps a StructArray
impl<L: DaftLogicalType> LogicalArrayImpl<L, StructArray> {
    impl_logical_type!(StructArray);

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let mut struct_arrow_array = self.physical.to_arrow();
        let arrow_logical_type = self.data_type().to_arrow().unwrap();
        struct_arrow_array.change_type(arrow_logical_type);
        struct_arrow_array
    }
}

impl MapArray {
    impl_logical_type!(ListArray);

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let arrow_dtype = self.data_type().to_arrow().unwrap();
        Box::new(arrow2::array::MapArray::new(
            arrow_dtype,
            self.physical.offsets().try_into().unwrap(),
            self.physical.flat_child.to_arrow(),
            self.physical.validity().cloned(),
        ))
    }
}

pub type LogicalArray<L> =
    LogicalArrayImpl<L, <<L as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType>;
pub type Decimal128Array = LogicalArray<Decimal128Type>;
pub type DateArray = LogicalArray<DateType>;
pub type TimeArray = LogicalArray<TimeType>;
pub type DurationArray = LogicalArray<DurationType>;
pub type ImageArray = LogicalArray<ImageType>;
pub type TimestampArray = LogicalArray<TimestampType>;
pub type TensorArray = LogicalArray<TensorType>;
pub type EmbeddingArray = LogicalArray<EmbeddingType>;
pub type FixedShapeTensorArray = LogicalArray<FixedShapeTensorType>;
pub type FixedShapeImageArray = LogicalArray<FixedShapeImageType>;
pub type MapArray = LogicalArray<MapType>;
pub trait DaftImageryType: DaftLogicalType {}

impl DaftImageryType for ImageType {}
impl DaftImageryType for FixedShapeImageType {}
