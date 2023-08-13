use std::{marker::PhantomData, sync::Arc};

use crate::datatypes::{BooleanArray, DaftLogicalType, DateType, Field};
use common_error::DaftResult;

use super::{
    DaftArrowBackedType, DataArray, DataType, Decimal128Type, DurationType, EmbeddingType,
    FixedShapeImageType, FixedShapeTensorType, ImageType, TensorType, TimestampType,
};

/// A LogicalArray is a wrapper on top of some underlying array, applying the semantic meaning of its
/// field.datatype() to the underlying array.
pub struct LogicalArray<L: DaftLogicalType, PhysicalArray> {
    pub field: Arc<Field>,
    pub physical: PhysicalArray,
    marker_: PhantomData<L>,
}

/// LogicalArrays implementations that wrap different underlying types
#[allow(warnings)] // the "where" bounds in a type alias are not enforced on the lhs, but are enforced on the rhs
pub type LogicalDataArray<L: DaftLogicalType>
where
    L::PhysicalType: DaftArrowBackedType,
= LogicalArray<L, DataArray<<L as DaftLogicalType>::PhysicalType>>;

impl<L: DaftLogicalType> Clone for LogicalDataArray<L>
where
    L::PhysicalType: DaftArrowBackedType,
{
    fn clone(&self) -> Self {
        LogicalArray::new(self.field.clone(), self.physical.clone())
    }
}

impl<L: DaftLogicalType> LogicalDataArray<L>
where
    L::PhysicalType: DaftArrowBackedType,
{
    pub fn new<F: Into<Arc<Field>>>(field: F, physical: DataArray<L::PhysicalType>) -> Self {
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
            physical,
            field,
            marker_: PhantomData,
        }
    }

    pub fn empty(name: &str, dtype: &DataType) -> Self {
        let field = Field::new(name, dtype.clone());
        Self::new(field, DataArray::empty(name, &dtype.to_physical()))
    }

    pub fn name(&self) -> &str {
        self.field.name.as_ref()
    }

    pub fn rename(&self, name: &str) -> Self {
        let new_field = self.field.rename(name);
        let new_array = self.physical.rename(name);
        Self::new(new_field, new_array)
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

    // Linter bug: doesn't pick up `is_empty` which is defined below
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.physical.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.physical.size_bytes()
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let new_array = self.physical.slice(start, end)?;
        Ok(Self::new(self.field.clone(), new_array))
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(common_error::DaftError::ValueError(
                "Need at least 1 logical array to concat".to_string(),
            ));
        }
        let physicals: Vec<_> = arrays.iter().map(|a| &a.physical).collect();
        let concatd = DataArray::<L::PhysicalType>::concat(physicals.as_slice())?;
        Ok(Self::new(arrays.first().unwrap().field.clone(), concatd))
    }

    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let new_array = self.physical.filter(mask)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

pub type Decimal128Array = LogicalDataArray<Decimal128Type>;
pub type DateArray = LogicalDataArray<DateType>;
pub type DurationArray = LogicalDataArray<DurationType>;
pub type EmbeddingArray = LogicalDataArray<EmbeddingType>;
pub type ImageArray = LogicalDataArray<ImageType>;
pub type FixedShapeImageArray = LogicalDataArray<FixedShapeImageType>;
pub type TimestampArray = LogicalDataArray<TimestampType>;
pub type TensorArray = LogicalDataArray<TensorType>;
pub type FixedShapeTensorArray = LogicalDataArray<FixedShapeTensorType>;

pub trait DaftImageryType: DaftLogicalType {}

impl DaftImageryType for ImageType {}
impl DaftImageryType for FixedShapeImageType {}
