use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;

use crate::{
    array::{
        ops::{
            as_arrow::AsArrow,
            from_arrow::{self, FromArrow},
        },
        DataArray,
    },
    datatypes::{DaftArrowBackedType, DaftDataType, Field},
    with_match_arrow_backed_physical_types, DataType, IntoSeries, Series,
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

impl<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>> Growable
    for ArrowGrowable<'a, T, G>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.arrow2_growable.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.arrow2_growable.extend_validity(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let arrow_array = self.arrow2_growable.as_box();
        let field = Field::new(self.name, self.dtype);
        Ok(DataArray::<T>::from_arrow(&field, arrow_array)?.into_series())
    }
}

pub struct ArrowExtensionGrowable {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn Growable>,
}

impl ArrowExtensionGrowable {
    pub fn new(name: String, dtype: &DataType, child_growable: Box<dyn Growable>) -> Self {
        assert!(matches!(dtype, DataType::Extension(..)));
        Self {
            name,
            dtype: dtype.clone(),
            child_growable,
        }
    }
}

impl Growable for ArrowExtensionGrowable {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(index, start, len)
    }

    fn add_nulls(&mut self, additional: usize) {
        self.child_growable.add_nulls(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let child_series = self.child_growable.build()?;

        // Wrap the child series with the appropriate extension type
        let DataType::Extension(s1, child_dtype, s2) = self.dtype;
        let arrow_extension_type = arrow2::datatypes::DataType::Extension(
            s1,
            Box::new(child_series.data_type().to_arrow()?),
            s2,
        );
        with_match_arrow_backed_physical_types!(child_dtype.as_ref(), |$T| {
            let child_arrow_array = child_series.downcast::<DataArray::<$T>>()?.as_arrow();
            child_arrow_array.to(arrow_extension_type);
            Ok(DataArray::<ExtensionType>::new(
                Arc::new(Field::new(self.name, self.dtype)),
                Box::new(child_arrow_array.clone()),
            )?.into_series())
        })
    }
}
