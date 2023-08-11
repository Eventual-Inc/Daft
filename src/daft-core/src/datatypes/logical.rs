use std::{marker::PhantomData, sync::Arc};

use crate::datatypes::{DaftLogicalType, DateType, Field};
use crate::series::{ArrayWrapper, SeriesLike};
use common_error::DaftResult;

use super::{
    DataType, Decimal128Type, DurationType, EmbeddingType, FixedShapeImageType,
    FixedShapeTensorType, ImageType, TensorType, TimestampType,
};

pub struct LogicalArray<L: DaftLogicalType>
where
    ArrayWrapper<L::ChildArrayType>: SeriesLike,
{
    pub field: Arc<Field>,
    pub physical: ArrayWrapper<L::ChildArrayType>,
    marker_: PhantomData<L>,
}

impl<L: DaftLogicalType> Clone for LogicalArray<L>
where
    ArrayWrapper<L::ChildArrayType>: SeriesLike,
{
    fn clone(&self) -> Self {
        LogicalArray::new(self.field.clone(), dyn_clone::clone(&self.physical).0)
    }
}

impl<L: DaftLogicalType + 'static> LogicalArray<L>
where
    ArrayWrapper<L::ChildArrayType>: SeriesLike,
{
    pub fn new<F: Into<Arc<Field>>>(field: F, physical: L::ChildArrayType) -> Self {
        let physical = ArrayWrapper(physical);
        let field = field.into();
        assert!(
            field.dtype.is_logical(),
            "Can only construct Logical Arrays on Logical Types, got {}",
            field.dtype
        );
        assert_eq!(
            physical.data_type(),
            &field.dtype.to_physical(),
            "Expected {} for Physical Array, got {}",
            &field.dtype.to_physical(),
            physical.data_type()
        );

        LogicalArray {
            field,
            physical,
            marker_: PhantomData,
        }
    }

    pub fn empty(name: &str, dtype: &DataType) -> Self 
    {
        // TODO: Implement ::empty() -- how do we know what concrete type to use for self.physical though?
        todo!()
    }

    pub fn name(&self) -> &str {
        self.field.name.as_ref()
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn logical_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn physical_type(&self) -> &DataType {
        self.physical.data_type()
    }

    pub fn len(&self) -> usize {
        self.physical.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.physical.size_bytes()
    }
}

pub type Decimal128Array = LogicalArray<Decimal128Type>;
pub type DateArray = LogicalArray<DateType>;
pub type DurationArray = LogicalArray<DurationType>;
pub type EmbeddingArray = LogicalArray<EmbeddingType>;
pub type ImageArray = LogicalArray<ImageType>;
pub type FixedShapeImageArray = LogicalArray<FixedShapeImageType>;
pub type TimestampArray = LogicalArray<TimestampType>;
pub type TensorArray = LogicalArray<TensorType>;
pub type FixedShapeTensorArray = LogicalArray<FixedShapeTensorType>;

pub trait DaftImageryType: DaftLogicalType {}

impl DaftImageryType for ImageType {}
impl DaftImageryType for FixedShapeImageType {}
