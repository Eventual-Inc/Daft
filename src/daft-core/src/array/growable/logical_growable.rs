use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    datatypes::{
        logical::LogicalArray, DaftDataType, DaftLogicalType, DateType, Decimal128Type,
        DurationType, EmbeddingType, Field, FixedShapeImageType, FixedShapeTensorType, ImageType,
        TensorType, TimestampType,
    },
    DataType, IntoSeries, Series,
};

use super::{Growable, GrowableArray};

pub struct LogicalGrowable<L: DaftLogicalType, G: Growable>
where
    LogicalArray<L>: IntoSeries,
{
    name: String,
    dtype: DataType,
    physical_growable: G,
    _phantom: PhantomData<L>,
}

impl<L: DaftLogicalType, G: Growable> Growable for LogicalGrowable<L, G>
where
    LogicalArray<L>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.physical_growable.extend(index, start, len);
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.physical_growable.add_nulls(additional)
    }
    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let physical_arr = self.physical_growable.build()?;
        let arr = LogicalArray::<L>::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            physical_arr
                .downcast::<<<L as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType>()
                .unwrap()
                .clone(),
        );
        Ok(arr.into_series())
    }
}

macro_rules! impl_logical_growable {
    ($growable_name:ident, $daft_type:ty) => {
        pub type $growable_name<'a> = LogicalGrowable<$daft_type, <<<$daft_type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType as GrowableArray>::GrowableType<'a>>;

        impl<'a> $growable_name<'a>
        {
            pub fn new(name: &str, dtype: &DataType, arrays: Vec<&'a <$daft_type as DaftDataType>::ArrayType>, use_validity: bool, capacity: usize) -> Self {
                let physical_growable = <<$daft_type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::make_growable(
                    name,
                    &dtype.to_physical(),
                    arrays.iter().map(|a| &a.physical).collect(),
                    use_validity,
                    capacity,
                );
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    physical_growable,
                    _phantom: PhantomData,
                }
            }
        }
    };
}

impl_logical_growable!(LogicalTimestampGrowable, TimestampType);
impl_logical_growable!(LogicalDurationGrowable, DurationType);
impl_logical_growable!(LogicalDateGrowable, DateType);
impl_logical_growable!(LogicalEmbeddingGrowable, EmbeddingType);
impl_logical_growable!(LogicalFixedShapeImageGrowable, FixedShapeImageType);
impl_logical_growable!(LogicalFixedShapeTensorGrowable, FixedShapeTensorType);
impl_logical_growable!(LogicalImageGrowable, ImageType);
impl_logical_growable!(LogicalDecimal128Growable, Decimal128Type);
impl_logical_growable!(LogicalTensorGrowable, TensorType);
